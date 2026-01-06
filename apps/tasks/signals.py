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
from django.db.models import Q  # ✅ NEW: used for tolerant series queries

from .models import Checklist, Delegation, HelpTicket
from . import utils as _utils  # email helpers & console-safe logging

# NEW: for working-day (holiday/Sunday) shifts on recurrence
from apps.settings.models import Holiday

# ✅ Leave-blocking helpers
from apps.tasks.services.blocking import guard_assign
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# If Celery fanout is enabled, DO NOT send/schedule emails from signals (avoid duplicates)
ENABLE_CELERY_EMAIL = getattr(settings, "ENABLE_CELERY_EMAIL", False)

# ---------------------------------------------------------------------
# Import recurrence helpers – prefer recurrence_utils (final rules).
# ---------------------------------------------------------------------
_normalize_mode = None
_RECURRING_MODES = None
_compute_next_same_time = None
_compute_next_fixed_7pm = None

try:
    # New module with final rules (helpers that pin 7 PM)
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
        # Legacy module fallback
        from .recurrence import (
            normalize_mode,
            RECURRING_MODES,
            get_next_planned_date as _legacy_next,
        )  # type: ignore

        _normalize_mode = normalize_mode
        _RECURRING_MODES = RECURRING_MODES

        def _compute_next_fixed_7pm(
            prev_dt: datetime,
            mode: str,
            frequency: int,
            * ,
            end_date=None,
        ):
            # legacy get_next_planned_date already pins to 19:00 IST in that module
            return _legacy_next(prev_dt, mode, frequency)

        def _compute_next_same_time(
            prev_dt: datetime,
            mode: str,
            frequency: int,
            * ,
            end_date=None,
        ):
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

# Expose names used below
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
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (
        anchor + timedelta(minutes=leeway_minutes)
    )


# ---------------------------------------------------------------------
# Working-day helpers (skip Sunday & holidays) for recurrence
# ---------------------------------------------------------------------
def _is_working_day(d):
    # Sunday == 6
    if d.weekday() == 6:
        return False
    return not Holiday.objects.filter(date=d).exists()


def _shift_to_next_working_day_7pm(ist_dt: datetime) -> datetime:
    """
    Given an IST datetime, shift the DATE forward while it's not a working day.
    Return 19:00 IST of that shifted date, converted back to project TZ.
    """
    tz = timezone.get_current_timezone()
    base_ist = ist_dt.astimezone(IST)
    d = base_ist.date()
    for _ in range(120):  # safety
        if _is_working_day(d):
            new_ist = IST.localize(datetime.combine(d, dt_time(19, 0)))
            return new_ist.astimezone(tz)
        d = d + timedelta(days=1)
    # Fallback: just pin 19:00 of original date
    return IST.localize(datetime.combine(base_ist.date(), dt_time(19, 0))).astimezone(tz)


# ---------------------------------------------------------------------
# 10:00 IST email schedulers (adjusted: rely on CRON; no sleeper threads)
# ---------------------------------------------------------------------
def _schedule_10am_email_for_checklist(obj: Checklist) -> None:
    """
    For any created checklist:
      - If it's the planned day and current IST time is AFTER 10:00, send immediately (so cron won't miss it).
      - Otherwise, do nothing here. Daily 10:00 IST cron will dispatch.
      - 🔒 Leave guard: if user is blocked for today's 10:00 IST, DO NOT send.
    """
    if ENABLE_CELERY_EMAIL:
        return
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    # Never email the assigner (including self-assign)
    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(
                _utils._safe_console_text(
                    f"Checklist email suppressed for CL-{obj.id}: assigner == assignee."
                )
            )
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(
            _utils._safe_console_text(
                f"Checklist email skipped for CL-{obj.id}: assignee has no email."
            )
        )
        return

    def _send_now():
        # Final guard at send moment (10:00 anchor logic)
        try:
            now_ist = timezone.now().astimezone(IST)
            anchor_ist = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
            if not guard_assign(obj.assign_to, anchor_ist):
                logger.info(_utils._safe_console_text(f"Checklist CL-{obj.id} suppressed (assignee on leave @ 10:00 IST)."))
                return
        except Exception:
            pass

        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        subject_prefix = f"Today’s Checklist – {obj.task_name}"
        _utils.send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=subject_prefix,
        )

    # If not restricting to 10 AM, send immediately
    if not SEND_RECUR_EMAILS_ONLY_AT_10AM:
        _on_commit(_send_now)
        return

    try:
        planned = obj.planned_date
        if not planned:
            # No planned date: send now (still guarded)
            _on_commit(_send_now)
            return

        now_ist = timezone.now().astimezone(IST)
        planned_ist = timezone.localtime(planned, IST)
        if planned_ist.date() != now_ist.date():
            # Future/past day: let cron handle it (no sleeper threads)
            return

        # It's today's task. If we're past 10:00 IST now, send immediately (with guard).
        anchor_ist = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
        if now_ist >= anchor_ist:
            _on_commit(_send_now)
        # Else before 10:00 IST: do nothing; cron will send.
    except Exception as e:
        logger.error(
            _utils._safe_console_text(
                f"Checklist email scheduling failed for {obj.id}: {e}"
            )
        )
        # As a fallback: do nothing here; cron remains the source of truth.


def _schedule_10am_email_for_delegation(obj: Delegation) -> None:
    """
    For delegations:
      - We already send an immediate "New Delegation Assigned" on create.
      - Schedule a day-of 10:00 IST reminder ONLY if creation is before 10:00 IST of the planned day.
        (Avoid duplicate if creation happens after 10:00 IST.)
    """
    if ENABLE_CELERY_EMAIL:
        return
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    # Never email the assigner (including self-assign)
    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(
                _utils._safe_console_text(
                    f"Delegation email suppressed for DL-{obj.id}: assigner == assignee."
                )
            )
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(
            _utils._safe_console_text(
                f"Delegation email skipped for DL-{obj.id}: assignee has no email."
            )
        )
        return

    def _send_now():
        complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
        subject_prefix = f"Today’s Delegation – {obj.task_name} (due 7 PM)"
        # Day-level guard at 10:00 IST of the day
        try:
            now_ist = timezone.now().astimezone(IST)
            anchor_ist = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
            if not guard_assign(obj.assign_to, anchor_ist):
                logger.info(_utils._safe_console_text(f"Delegation DL-{obj.id} 10AM reminder suppressed (assignee on leave)."))
                return
        except Exception:
            pass
        _utils.send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix=subject_prefix,
        )

    if not SEND_RECUR_EMAILS_ONLY_AT_10AM:
        _on_commit(_send_now)
        return

    try:
        planned = obj.planned_date
        if not planned:
            return

        now_ist = timezone.now().astimezone(IST)
        planned_ist = timezone.localtime(planned, IST)
        if planned_ist.date() != now_ist.date():
            # Not today's reminder; cron will handle on the day
            return

        anchor_ist = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
        if now_ist < anchor_ist:
            # Before 10:00 → allow the cron at 10:00 to send (no threads here)
            return
        else:
            # After 10:00 → BUT we already sent the immediate “New Delegation Assigned”.
            # To avoid a same-moment duplicate, skip sending the 10am-style reminder now.
            return
    except Exception as e:
        logger.error(
            _utils._safe_console_text(
                f"Delegation email scheduling failed for {obj.id}: {e}"
            )
        )
        # Do nothing; cron covers the reminder on time.


# Immediate assignment email for delegations (so user gets mail as soon as task is added)
def _send_delegation_assignment_immediate(obj: Delegation) -> None:
    """
    One-time "New Delegation Assigned" email, sent immediately after creation.
    Daily / 10:00 AM reminders are handled separately.

    🔒 LEAVE GUARD: if the assignee is on leave *right now*, suppress the immediate mail.
    (The task may still exist; visibility & reminders are governed elsewhere.)
    """
    if ENABLE_CELERY_EMAIL:
        return
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(
                _utils._safe_console_text(
                    f"Immediate delegation email suppressed for DL-{obj.id}: assigner == assignee."
                )
            )
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(
            _utils._safe_console_text(
                f"Immediate delegation email skipped for DL-{obj.id}: assignee has no email."
            )
        )
        return

    # Time-level guard (current instant in IST)
    try:
        if is_user_blocked_at(obj.assign_to, timezone.now().astimezone(IST)):
            logger.info(_utils._safe_console_text(f"Immediate delegation email suppressed for DL-{obj.id}: assignee on leave now."))
            return
    except Exception:
        pass

    def _send_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
        except Exception:
            complete_url = SITE_URL

        _utils.send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix=f"New Delegation Assigned – {obj.task_name}",
        )

    _on_commit(_send_now)


# ---------------------------------------------------------------------
# CHECKLIST: force planned datetime to 19:00 IST (NO shift on save)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Checklist)
def force_checklist_planned_time(sender, instance: Checklist, **kwargs):
    """
    Checklist (one-time or recurring):
      • planned datetime MUST be 19:00 IST on the SAME date user chose
      • NO shift off Sundays/holidays at entry time
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
# DELEGATION: force planned datetime to 19:00 IST (NO shift on save)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Delegation)
def force_delegation_planned_time(sender, instance: Delegation, **kwargs):
    """
    Delegations are one-time and MUST respect:
      • planned datetime at 19:00 IST on the SAME date (no shift on save)
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
# (stepped date → then shift to next working day @ 19:00 IST)
# ---------------------------------------------------------------------

def _series_q_for_frequency(assign_to_id: int, task_name: str, mode: str, freq_norm: int, group_name: str | None) -> Q:
    """
    ✅ Tolerant series matcher: treat NULL frequency as 1 (legacy rows).
    Mirrors apps/tasks/tasks.py::_series_q_for_frequency to avoid duplicate spawns.
    """
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq_norm, None])
    return q


@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance: Checklist, created: bool, **kwargs):
    """
    When a recurring checklist is marked 'Completed', create the next occurrence:
      • Valid only for modes in RECURRING_MODES
      • Trigger on update (not initial create)
      • Next occurrence is scheduled at 19:00 IST on the stepped date,
        then SHIFTED to the next working day (skip Sunday/holidays)
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

    # ✅ Tolerant series Q (frequency: accept [exact, NULL])
    freq_norm = max(int(getattr(instance, "frequency", 1) or 1), 1)
    q_series = _series_q_for_frequency(
        assign_to_id=instance.assign_to_id,
        task_name=instance.task_name,
        mode=instance.mode,
        freq_norm=freq_norm,
        group_name=getattr(instance, "group_name", None),
    )

    # If a future Pending in this tolerant series already exists, don't create another.
    if Checklist.objects.filter(status="Pending").filter(q_series).filter(planned_date__gt=now).exists():
        return

    # Compute next planned date at fixed 19:00 IST (no shift yet)
    next_dt = None
    try:
        if _compute_next_fixed_7pm:
            next_dt = _compute_next_fixed_7pm(
                instance.planned_date,
                instance.mode,
                instance.frequency,
            )
        else:
            # Minimal fallback: compute stepped date in IST and pin 19:00
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

        # NOW: shift to next working day (Sunday/holiday) and pin 19:00
        next_dt = _shift_to_next_working_day_7pm(next_dt)
    except Exception as e:
        logger.error(
            _utils._safe_console_text(
                f"Error calculating next recurrence for CL-{instance.id}: {e}"
            )
        )
        next_dt = None

    if not next_dt:
        logger.warning(
            _utils._safe_console_text(
                f"No next date for recurring checklist {instance.id}"
            )
        )
        return

    # Catch-up loop: ensure next occurrence lands in the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:  # ~2 years safety
        try:
            if _compute_next_fixed_7pm:
                tmp = _compute_next_fixed_7pm(
                    next_dt,
                    instance.mode,
                    instance.frequency,
                )
            else:
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
                tmp = IST.localize(datetime.combine(d, dt_time(19, 0))).astimezone(tz)

            next_dt = _shift_to_next_working_day_7pm(tmp)
        except Exception:
            break
        safety += 1

    if not next_dt:
        logger.warning(
            _utils._safe_console_text(
                f"Could not find a future date for series '{instance.task_name}'"
            )
        )
        return

    # ✅ Duplicate guard (±1 minute window) using tolerant series Q
    dupe_exists = Checklist.objects.filter(status="Pending").filter(q_series).filter(
        planned_date__gte=next_dt - timedelta(minutes=1),
        planned_date__lt=next_dt + timedelta(minutes=1),
    ).exists()
    if dupe_exists:
        logger.info(
            _utils._safe_console_text(
                f"Duplicate prevented for '{instance.task_name}' at {next_dt}"
            )
        )
        return

    try:
        with transaction.atomic():
            new_obj = Checklist.objects.create(
                assign_by=instance.assign_by,
                task_name=instance.task_name,
                message=instance.message,
                assign_to=instance.assign_to,
                planned_date=next_dt,  # shifted to next working day @ 19:00 IST
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
        # Do NOT send email directly here — handled elsewhere.
    except Exception as e:
        logger.error(
            _utils._safe_console_text(
                f"Failed to create recurring checklist for {instance.id}: {e}"
            )
        )


# ---------------------------------------------------------------------
# GENERIC: On ANY checklist creation, conditionally send/allow cron
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def schedule_checklist_email_on_create(sender, instance: Checklist, created: bool, **kwargs):
    if not created:
        return
    if ENABLE_CELERY_EMAIL:
        return
    try:
        _on_commit(lambda: _schedule_10am_email_for_checklist(instance))
    except Exception:
        _schedule_10am_email_for_checklist(instance)


# ---------------------------------------------------------------------
# DELEGATION: On creation, immediate assignment + (maybe) 10:00 reminder
# ---------------------------------------------------------------------
@receiver(post_save, sender=Delegation)
def schedule_delegation_email_on_create(sender, instance: Delegation, created: bool, **kwargs):
    if not created:
        return
    if ENABLE_CELERY_EMAIL:
        return
    # 1) Immediate "New Delegation Assigned" email (with leave guard)
    try:
        _on_commit(lambda: _send_delegation_assignment_immediate(instance))
    except Exception:
        _send_delegation_assignment_immediate(instance)
    # 2) Day-of 10:00 IST reminder only if created before the 10:00 gate
    try:
        _on_commit(lambda: _schedule_10am_email_for_delegation(instance))
    except Exception:
        _schedule_10am_email_for_delegation(instance)


# ---------------------------------------------------------------------
# HELPTICKET: Immediate email on assignment (created) — with leave guard
# ---------------------------------------------------------------------
@receiver(post_save, sender=HelpTicket)
def send_help_ticket_email_on_create(sender, instance: HelpTicket, created: bool, **kwargs):
    if not created:
        return

    # If assignee is on leave NOW, or the planned timestamp lies within a leave window,
    # suppress the assignment email.
    try:
        assignee = getattr(instance, "assign_to", None)
        if assignee and is_user_blocked_at(assignee, timezone.now().astimezone(IST)):
            logger.info(_utils._safe_console_text(f"HelpTicket HT-{getattr(instance, 'id', '?')} email suppressed: assignee on leave now."))
            return
        if assignee and getattr(instance, "planned_date", None):
            planned_ist = timezone.localtime(instance.planned_date, IST)
            if is_user_blocked_at(assignee, planned_ist):
                logger.info(_utils._safe_console_text(f"HelpTicket HT-{getattr(instance, 'id', '?')} email suppressed: planned time within leave."))
                return
    except Exception:
        pass

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

    try:
        _on_commit(_send_now)
    except Exception as e:
        logger.error(
            _utils._safe_console_text(
                f"HelpTicket email send failed for HT-{getattr(instance, 'id', '?')}: {e}"
            )
        )


# ---------------------------------------------------------------------
# Logging helpers (unchanged)
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def log_checklist_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(
            _utils._safe_console_text(
                f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
            )
        )


@receiver(post_save, sender=Delegation)
def log_delegation_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(
            _utils._safe_console_text(
                f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
            )
        )


@receiver(post_save, sender=HelpTicket)
def log_helpticket_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Closed":
        logger.info(
            _utils._safe_console_text(
                f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}"
            )
        )


@receiver(post_save, sender=Checklist)
def log_checklist_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(
            _utils._safe_console_text(
                f"Created checklist {instance.id} '{instance.task_name}' "
                f"for {instance.assign_to} at {instance.planned_date}"
            )
        )


@receiver(post_save, sender=Delegation)
def log_delegation_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(
            _utils._safe_console_text(
                f"Created delegation {instance.id} '{instance.task_name}' "
                f"for {instance.assign_to} at {instance.planned_date}"
            )
        )
