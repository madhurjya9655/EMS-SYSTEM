# apps/tasks/signals.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time
from threading import Thread
import time as _time

import pytz
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from . import utils as _utils  # email helpers & console-safe logging

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# ---------------------------------------------------------------------
# Import recurrence helpers – prefer recurrence_utils (final rules).
# Fallback to old recurrence (legacy), then to local minimal versions.
# ---------------------------------------------------------------------
_normalize_mode = None
_RECURRING_MODES = None
_compute_next_same_time = None
_compute_next_fixed_7pm = None

try:
    # New module with final rules (no working-day shifts, 7pm helper)
    from .recurrence_utils import (
        normalize_mode,
        RECURRING_MODES,
        get_next_same_time as _compute_next_same_time,
        get_next_fixed_7pm as _compute_next_fixed_7pm,
    )  # type: ignore
    _normalize_mode = normalize_mode
    _RECURRING_MODES = RECURRING_MODES
except Exception:
    pass

if _normalize_mode is None or _RECURRING_MODES is None:
    try:
        # Legacy module (kept only as a fallback)
        from .recurrence import normalize_mode, RECURRING_MODES, get_next_planned_date as _legacy_next  # type: ignore
        _normalize_mode = normalize_mode
        _RECURRING_MODES = RECURRING_MODES

        def _compute_next_fixed_7pm(prev_dt: datetime, mode: str, frequency: int, *, end_date=None):
            # legacy get_next_planned_date already pins to 19:00 IST in that module
            return _legacy_next(prev_dt, mode, frequency)

        # Same-time helper fallback (keeps time)
        def _compute_next_same_time(prev_dt: datetime, mode: str, frequency: int, *, end_date=None):
            m = _normalize_mode(mode)
            if not m:
                return None
            step = max(int(frequency or 1), 1)
            tz = timezone.get_current_timezone()
            if timezone.is_naive(prev_dt):
                prev_dt = timezone.make_aware(prev_dt, tz)
            prev_ist = prev_dt.astimezone(IST)
            if m == "Daily":
                nxt = prev_ist + relativedelta(days=step)
            elif m == "Weekly":
                nxt = prev_ist + relativedelta(weeks=step)
            elif m == "Monthly":
                nxt = prev_ist + relativedelta(months=step)
            else:
                nxt = prev_ist + relativedelta(years=step)
            return nxt.astimezone(tz)
    except Exception:
        pass

# Absolute last-resort defaults
if _RECURRING_MODES is None:
    _RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

def _normalize_mode_local(mode):
    if not mode:
        return None
    s = str(mode).strip().title()
    return s if s in _RECURRING_MODES else None

if _normalize_mode is None:
    _normalize_mode = _normalize_mode_local

# Expose names we reference below
normalize_mode = _normalize_mode
RECURRING_MODES = _RECURRING_MODES

# ---------------------------------------------------------------------
# Email policy toggles
# ---------------------------------------------------------------------
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


def _on_commit(fn):
    """Safe on_commit: if no transaction context, call immediately."""
    try:
        transaction.on_commit(fn)
    except Exception:
        try:
            fn()
        except Exception:
            logger.exception("on_commit fallback failed")


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    """True if now (IST) is within [10:00 - leeway, 10:00 + leeway]."""
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _spawn_delayed(send_at_utc: datetime, fn, *, name: str = "recur-email"):
    """Sleep until `send_at_utc` (UTC) and then call `fn` in a daemon thread."""
    def _runner():
        try:
            while True:
                now = timezone.now().astimezone(pytz.UTC)
                secs = (send_at_utc - now).total_seconds()
                if secs <= 0:
                    break
                _time.sleep(min(60.0, max(0.5, secs)))
            fn()
        except Exception as e:
            logger.error(_utils._safe_console_text(f"Delayed checklist email failed: {e}"))
    Thread(target=_runner, name=name, daemon=True).start()


# ---------------------------------------------------------------------
# 10:00 IST email schedulers
# ---------------------------------------------------------------------
def _schedule_10am_email_for_checklist(obj: Checklist) -> None:
    """
    Schedule/send the assignee-only email for ANY created checklist (first or recurring)
    at ~10:00 IST on its planned date.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    # Never email the assigner (including self-assign)
    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(_utils._safe_console_text(
                f"Checklist email suppressed for CL-{obj.id}: assigner == assignee."
            ))
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(_utils._safe_console_text(
            f"Checklist email skipped for CL-{obj.id}: assignee has no email."
        ))
        return

    def _send_now():
        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        # Subject: recurring vs one-time checklist (based on mode presence)
        prefix = "Recurring Checklist Generated" if normalize_mode(getattr(obj, "mode", None)) in RECURRING_MODES else "Checklist Assigned"
        _utils.send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=prefix,
        )

    if not SEND_RECUR_EMAILS_ONLY_AT_10AM:
        _on_commit(_send_now)
        return

    # Determine anchor 10:00 IST on the PLANNED date
    try:
        planned = obj.planned_date
        if not planned:
            _on_commit(_send_now)
            return

        planned_ist = timezone.localtime(planned, IST)
        anchor_ist = IST.localize(datetime.combine(planned_ist.date(), dt_time(10, 0)))
        now_ist = timezone.now().astimezone(IST)

        if (anchor_ist - timedelta(minutes=5)) <= now_ist <= (anchor_ist + timedelta(minutes=5)):
            _on_commit(_send_now)
        elif now_ist < anchor_ist:
            anchor_utc = anchor_ist.astimezone(pytz.UTC)
            _on_commit(lambda: _spawn_delayed(anchor_utc, _send_now, name=f"cl-10am-email-{obj.id}"))
        else:
            _on_commit(_send_now)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Checklist email scheduling failed for {obj.id}: {e}"))
        _on_commit(_send_now)


def _schedule_10am_email_for_delegation(obj: Delegation) -> None:
    """
    Schedule/send the assignee-only email for ANY created delegation
    at ~10:00 IST on its planned date (delegations are one-time).
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    # Never email the assigner (including self-assign)
    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(_utils._safe_console_text(
                f"Delegation email suppressed for DL-{obj.id}: assigner == assignee."
            ))
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(_utils._safe_console_text(
            f"Delegation email skipped for DL-{obj.id}: assignee has no email."
        ))
        return

    def _send_now():
        complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
        _utils.send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix="Delegation Assigned",
        )

    if not SEND_RECUR_EMAILS_ONLY_AT_10AM:
        _on_commit(_send_now)
        return

    try:
        planned = obj.planned_date
        if not planned:
            _on_commit(_send_now)
            return

        planned_ist = timezone.localtime(planned, IST)
        anchor_ist = IST.localize(datetime.combine(planned_ist.date(), dt_time(10, 0)))
        now_ist = timezone.now().astimezone(IST)

        if (anchor_ist - timedelta(minutes=5)) <= now_ist <= (anchor_ist + timedelta(minutes=5)):
            _on_commit(_send_now)
        elif now_ist < anchor_ist:
            anchor_utc = anchor_ist.astimezone(pytz.UTC)
            _on_commit(lambda: _spawn_delayed(anchor_utc, _send_now, name=f"dl-10am-email-{obj.id}"))
        else:
            _on_commit(_send_now)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Delegation email scheduling failed for {obj.id}: {e}"))
        _on_commit(_send_now)


# ---------------------------------------------------------------------
# CHECKLIST: force planned datetime to 19:00 IST (NO working-day shift)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Checklist)
def force_checklist_planned_time(sender, instance: Checklist, **kwargs):
    """
    Checklist (one-time or recurring):
      • planned datetime MUST be 19:00 IST on the SAME date user chose
      • NO shift off Sundays/holidays
    """
    try:
        if not instance.planned_date:
            return
        dt = instance.planned_date
        tz = timezone.get_current_timezone()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, tz)
        dt_ist = dt.astimezone(IST)
        d = dt_ist.date()
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_checklist_planned_time failed: {e}"))


# ---------------------------------------------------------------------
# DELEGATION: force planned datetime to 19:00 IST (NO working-day shift)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Delegation)
def force_delegation_planned_time(sender, instance: Delegation, **kwargs):
    """
    Delegations are one-time and MUST respect:
      • planned datetime at 19:00 IST on the SAME date (no shift)
    Applied on every save to keep integrity.
    """
    try:
        if not instance.planned_date:
            return
        dt = instance.planned_date
        tz = timezone.get_current_timezone()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, tz)
        dt_ist = dt.astimezone(IST)
        d = dt_ist.date()
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_delegation_planned_time failed: {e}"))


# ---------------------------------------------------------------------
# CREATE NEXT RECURRING CHECKLIST
# (exact stepped date at fixed 19:00 IST; NO working-day shift)
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance: Checklist, created: bool, **kwargs):
    """
    When a recurring checklist is marked 'Completed', create the next occurrence:
      • Valid only for modes in RECURRING_MODES
      • Trigger on update (not initial create)
      • Next occurrence is scheduled at **19:00 IST** on the exact stepped date
        (Daily/Weekly/Monthly/Yearly by frequency). **No Sunday/holiday shift**.
      • Prevent duplicates within a 1-minute window
      • Do NOT send email here; a generic 'created' handler will schedule the 10:00 email.
    """
    # Only recurring series
    if normalize_mode(getattr(instance, "mode", None)) not in RECURRING_MODES:
        return
    # Only when status is Completed
    if instance.status != "Completed":
        return
    # Ignore the initial create
    if created:
        return

    now = timezone.now()

    # If a future pending of this same series already exists, don't create another.
    series_filter = dict(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        group_name=getattr(instance, "group_name", None),
    )
    if Checklist.objects.filter(status="Pending", planned_date__gt=now, **series_filter).exists():
        return

    # Compute next planned date (fixed 19:00 IST, NO working-day shift)
    next_dt = None
    try:
        if _compute_next_fixed_7pm:
            next_dt = _compute_next_fixed_7pm(instance.planned_date, instance.mode, instance.frequency)
        else:
            # As a minimal fallback: compute stepped date in IST and pin 19:00, no shift
            tz = timezone.get_current_timezone()
            prev = instance.planned_date
            if timezone.is_naive(prev):
                prev = timezone.make_aware(prev, tz)
            prev_ist = prev.astimezone(IST)
            m = normalize_mode(instance.mode)
            step = max(int(instance.frequency or 1), 1)
            if m == "Daily":
                nxt_ist = prev_ist + relativedelta(days=step)
            elif m == "Weekly":
                nxt_ist = prev_ist + relativedelta(weeks=step)
            elif m == "Monthly":
                nxt_ist = prev_ist + relativedelta(months=step)
            else:
                nxt_ist = prev_ist + relativedelta(years=step)
            d = nxt_ist.date()
            next_dt = IST.localize(datetime.combine(d, dt_time(19, 0))).astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Error calculating next recurrence for CL-{instance.id}: {e}"))
        next_dt = None

    if not next_dt:
        logger.warning(_utils._safe_console_text(f"No next date for recurring checklist {instance.id}"))
        return

    # Catch-up loop: ensure next occurrence lands in the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:  # ~2 years safety
        try:
            if _compute_next_fixed_7pm:
                next_dt = _compute_next_fixed_7pm(next_dt, instance.mode, instance.frequency)
            else:
                # replicate fallback stepping
                tz = timezone.get_current_timezone()
                prev = next_dt
                if timezone.is_naive(prev):
                    prev = timezone.make_aware(prev, tz)
                prev_ist = prev.astimezone(IST)
                m = normalize_mode(instance.mode)
                step = max(int(instance.frequency or 1), 1)
                if m == "Daily":
                    nxt_ist = prev_ist + relativedelta(days=step)
                elif m == "Weekly":
                    nxt_ist = prev_ist + relativedelta(weeks=step)
                elif m == "Monthly":
                    nxt_ist = prev_ist + relativedelta(months=step)
                else:
                    nxt_ist = prev_ist + relativedelta(years=step)
                d = nxt_ist.date()
                next_dt = IST.localize(datetime.combine(d, dt_time(19, 0))).astimezone(tz)
        except Exception:
            break
        safety += 1
    if not next_dt:
        logger.warning(_utils._safe_console_text(f"Could not find a future date for series '{instance.task_name}'"))
        return

    # Duplicate guard (±1 minute window)
    dupe_exists = Checklist.objects.filter(
        status="Pending",
        planned_date__gte=next_dt - timedelta(minutes=1),
        planned_date__lt=next_dt + timedelta(minutes=1),
        **series_filter,
    ).exists()
    if dupe_exists:
        logger.info(_utils._safe_console_text(f"Duplicate prevented for '{instance.task_name}' at {next_dt}"))
        return

    try:
        with transaction.atomic():
            new_obj = Checklist.objects.create(
                assign_by=instance.assign_by,
                task_name=instance.task_name,
                message=instance.message,
                assign_to=instance.assign_to,
                planned_date=next_dt,  # FIXED 19:00 IST on stepped date (no shift)
                priority=instance.priority,
                attachment_mandatory=instance.attachment_mandatory,
                mode=instance.mode,
                frequency=instance.frequency,
                time_per_task_minutes=instance.time_per_task_minutes,
                remind_before_days=instance.remind_before_days,
                assign_pc=instance.assign_pc,
                notify_to=instance.notify_to,
                auditor=getattr(instance, "auditor", None),
                set_reminder=instance.set_reminder,
                reminder_mode=instance.reminder_mode,
                reminder_frequency=instance.reminder_frequency,
                reminder_starting_time=instance.reminder_starting_time,
                checklist_auto_close=instance.checklist_auto_close,
                checklist_auto_close_days=instance.checklist_auto_close_days,
                group_name=getattr(instance, "group_name", None),
                actual_duration_minutes=0,
                status="Pending",
            )

            logger.info(
                _utils._safe_console_text(
                    f"Created next recurring checklist {new_obj.id} "
                    f"'{new_obj.task_name}' at {new_obj.planned_date}"
                )
            )
        # Do NOT send email directly here — the generic created handler below will schedule @ 10:00 IST.
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Failed to create recurring checklist for {instance.id}: {e}"))


# ---------------------------------------------------------------------
# GENERIC: On ANY checklist creation, schedule the 10:00 IST email
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def schedule_checklist_email_on_create(sender, instance: Checklist, created: bool, **kwargs):
    """
    For BOTH first occurrences and auto-created recurrences:
    schedule (or send) the assignee email at ~10:00 IST on the planned date.
    """
    if not created:
        return
    try:
        _on_commit(lambda: _schedule_10am_email_for_checklist(instance))
    except Exception:
        _schedule_10am_email_for_checklist(instance)


# ---------------------------------------------------------------------
# DELEGATION: On creation, schedule the 10:00 IST email (day-of)
# ---------------------------------------------------------------------
@receiver(post_save, sender=Delegation)
def schedule_delegation_email_on_create(sender, instance: Delegation, created: bool, **kwargs):
    """
    Delegations are one-time; send the assignee email at ~10:00 IST on the planned date.
    """
    if not created:
        return
    try:
        _on_commit(lambda: _schedule_10am_email_for_delegation(instance))
    except Exception:
        _schedule_10am_email_for_delegation(instance)


# ---------------------------------------------------------------------
# HELPTICKET: Immediate email on assignment (created)
# ---------------------------------------------------------------------
@receiver(post_save, sender=HelpTicket)
def send_help_ticket_email_on_create(sender, instance: HelpTicket, created: bool, **kwargs):
    """
    HelpTicket emails go out immediately at time of assignment (no 10:00 gating).
    """
    if not created:
        return
    try:
        def _send_now():
            try:
                complete_url = f"{SITE_URL}{reverse('tasks:help_ticket_detail', args=[instance.id])}"
            except Exception:
                complete_url = SITE_URL
            _utils.send_help_ticket_assignment_to_user(
                ticket=instance,
                complete_url=complete_url,
                subject_prefix="Help Ticket Assigned",
            )
        _on_commit(_send_now)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"HelpTicket email send failed for HT-{getattr(instance, 'id', '?')}: {e}"))


# ---------------------------------------------------------------------
# Logging helpers (unchanged)
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def log_checklist_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Delegation)
def log_delegation_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=HelpTicket)
def log_helpticket_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Closed":
        logger.info(_utils._safe_console_text(
            f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Checklist)
def log_checklist_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created checklist {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))


@receiver(post_save, sender=Delegation)
def log_delegation_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created delegation {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))
