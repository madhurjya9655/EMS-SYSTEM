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
# Import recurrence helpers – prefer recurrence_utils, fallback to recurrence.
# Also provide a local get_next_planned_date if not available.
# ---------------------------------------------------------------------
_normalize_mode = None
_is_working_day = None
_next_working_day = None
_RECURRING_MODES = None
_get_next_planned_date = None

# 1) Try recurrence_utils (what your views.py uses)
try:
    from .recurrence_utils import normalize_mode, is_working_day, next_working_day, RECURRING_MODES  # type: ignore
    _normalize_mode = normalize_mode
    _is_working_day = is_working_day
    _next_working_day = next_working_day
    _RECURRING_MODES = RECURRING_MODES
    try:
        from .recurrence_utils import get_next_planned_date as _get_next_planned_date  # type: ignore
    except Exception:
        _get_next_planned_date = None
except Exception:
    pass

# 2) Fallback to old recurrence module (what your original signals used)
if _normalize_mode is None or _is_working_day is None or _next_working_day is None or _RECURRING_MODES is None:
    try:
        from .recurrence import normalize_mode, is_working_day, next_working_day, RECURRING_MODES  # type: ignore
        _normalize_mode = normalize_mode
        _is_working_day = is_working_day
        _next_working_day = next_working_day
        _RECURRING_MODES = RECURRING_MODES
        try:
            from .recurrence import get_next_planned_date as _get_next_planned_date  # type: ignore
        except Exception:
            _get_next_planned_date = None
    except Exception:
        pass

# 3) Absolute last resort: provide local implementations
if _RECURRING_MODES is None:
    _RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

def _normalize_mode_local(mode):
    if not mode:
        return None
    s = str(mode).strip().title()
    return s if s in _RECURRING_MODES else None

if _normalize_mode is None:
    _normalize_mode = _normalize_mode_local

# If we cannot import these from either module, we still need a basic working-day notion.
if _is_working_day is None:
    from apps.settings.models import Holiday
    def _is_working_day(d):
        # Sunday = 6
        if d.weekday() == 6:
            return False
        try:
            return not Holiday.objects.filter(date=d).exists()
        except Exception:
            return d.weekday() != 6

if _next_working_day is None:
    def _next_working_day(d):
        cur = d
        # small safety loop
        for _ in range(0, 90):
            if _is_working_day(cur):
                return cur
            cur = cur + timedelta(days=1)
        return cur

# Canonical local get_next_planned_date:
# - move by freq (Daily/Weekly/Monthly/Yearly)
# - shift to next working day if needed
# - set time to 19:00 IST
def _get_next_planned_date_local(prev_dt: datetime, mode: str, frequency: int | None) -> datetime | None:
    m = _normalize_mode(mode)
    if not m:
        return None
    try:
        step = int(frequency or 1)
    except Exception:
        step = 1
    step = max(1, min(step, 10))

    # Make prev_dt aware in project TZ
    tz = timezone.get_current_timezone()
    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, tz)

    # Work in IST for date arithmetic alignment
    cur_ist = prev_dt.astimezone(IST)
    if m == "Daily":
        cur_ist = cur_ist + relativedelta(days=step)
    elif m == "Weekly":
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif m == "Monthly":
        cur_ist = cur_ist + relativedelta(months=step)
    elif m == "Yearly":
        cur_ist = cur_ist + relativedelta(years=step)

    # Shift to next working day (date-level), then pin to 19:00 IST
    next_date = _next_working_day(cur_ist.date())
    planned_ist = IST.localize(datetime.combine(next_date, dt_time(19, 0, 0)))
    return planned_ist.astimezone(tz)

# Choose the final function to use in this module
if _get_next_planned_date is None:
    get_next_planned_date = _get_next_planned_date_local
else:
    get_next_planned_date = _get_next_planned_date

# Expose names we reference below
normalize_mode = _normalize_mode
is_working_day = _is_working_day
next_working_day = _next_working_day
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
        _utils.send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix="Checklist Assigned" if normalize_mode(getattr(obj, "mode", None)) not in RECURRING_MODES
            else "Recurring Checklist Generated",
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
        # If your URL name differs, adjust here:
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
# CHECKLIST: force planned datetime to 19:00 IST (shift Sun/holidays)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Checklist)
def force_checklist_planned_time(sender, instance: Checklist, **kwargs):
    """
    Checklist (one-time or recurring):
      • planned datetime MUST be 19:00 IST
      • if Sunday/holiday → shift to next working day (still 19:00 IST)
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
        if not is_working_day(d):
            d = next_working_day(d)
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_checklist_planned_time failed: {e}"))


# ---------------------------------------------------------------------
# DELEGATION: force planned datetime to 19:00 IST (shift Sun/holidays)
# ---------------------------------------------------------------------
@receiver(pre_save, sender=Delegation)
def force_delegation_planned_time(sender, instance: Delegation, **kwargs):
    """
    Delegations are one-time tasks but MUST respect:
      • planned datetime at 19:00 IST (regardless of user input time / date-only input)
      • if planned day is Sunday/holiday → shift forward to next working day (time remains 19:00 IST)
    Applied on every save to keep integrity.
    """
    try:
        if not instance.planned_date:
            return
        dt = instance.planned_date
        # normalize to aware in project tz
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
        dt_ist = dt.astimezone(IST)
        d = dt_ist.date()
        if not is_working_day(d):
            d = next_working_day(d)
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(timezone.get_current_timezone())
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_delegation_planned_time failed: {e}"))


# ---------------------------------------------------------------------
# CREATE NEXT RECURRING CHECKLIST (at fixed 19:00 IST on a working day)
# ---------------------------------------------------------------------
@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance: Checklist, created: bool, **kwargs):
    """
    When a recurring checklist is marked 'Completed', create the next occurrence:
      • Valid only for modes in RECURRING_MODES
      • Trigger on update (not initial create)
      • Next occurrence is scheduled via get_next_planned_date()
        → ALWAYS 19:00 IST on a working day (Sun/holidays skipped)
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

    # Compute next planned date (fixed 19:00 IST, skips Sun/holidays)
    next_dt = get_next_planned_date(instance.planned_date, instance.mode, instance.frequency)
    if not next_dt:
        logger.warning(_utils._safe_console_text(f"No next date for recurring checklist {instance.id}"))
        return

    # Catch-up loop: ensure next occurrence lands in the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:  # ~2 years safety
        next_dt = get_next_planned_date(next_dt, instance.mode, instance.frequency)
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
                planned_date=next_dt,  # FIXED 19:00 IST on a working day
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
