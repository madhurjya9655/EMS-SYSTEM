# apps/leave/services/tasks_integration.py
from __future__ import annotations

"""
Helpers for schedulers/cron jobs and post-approval hooks so they can respect
leave blocking rules *before* assigning work and also tidy up already-planned
tasks inside a leave window.

What this module provides
-------------------------
• apply_leave_to_tasks(leave):
    - For APPROVED leaves only
    - Daily-mode tasks in the leave window → mark as "skipped"
    - Non-daily tasks in the window → reschedule to next working day @ 10:00 IST
    - Safe no-op if Checklist model isn't present

• is_user_on_leave_for_date(user, date):
    - True if an APPROVED leave includes that IST date (or a PENDING leave
      applied correctly before 09:30 IST for that same day)

• next_working_day(dt):
    - Return an aware datetime at midnight for the next working day after dt
      (uses apps.settings.Holiday if present; weekend = Sat/Sun)

• should_skip_assignment(user, planned_dt):
    - Convenience wrapper used by assignment pipelines:
      returns True if the assignee must be skipped for planned_dt

All functions are defensive and never raise to the caller; errors are logged to
the 'apps.tasks' logger channel.
"""

from datetime import datetime, date, time, timedelta
from typing import Iterable, Optional

import logging
from django.apps import apps
from django.conf import settings
from django.db import transaction
from django.utils import timezone

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# ---- Timezone (IST) ---------------------------------------------------------

IST = ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata")) if ZoneInfo else None

def _aware(dt: datetime) -> datetime:
    """Ensure tz-aware (IST-preferred)."""
    if timezone.is_aware(dt):
        return dt
    tz = IST or timezone.get_current_timezone()
    return timezone.make_aware(dt, tz)

def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST or timezone.get_current_timezone())
    except Exception:
        return dt

def _ist_date(dt: Optional[datetime]) -> Optional[date]:
    if not dt:
        return None
    return _to_ist(dt).date()

def _date_only(dt: datetime) -> date:
    return _aware(dt).astimezone(IST or timezone.get_current_timezone()).date()

def _datespan_inclusive_ist(start_dt: datetime, end_dt: datetime) -> list[date]:
    s = _ist_date(start_dt)
    e = _ist_date(end_dt)
    if not s or not e:
        return []
    if e < s:
        s, e = e, s
    out: list[date] = []
    cur = s
    while cur <= e:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out

# ---- Logging ----------------------------------------------------------------

logger = logging.getLogger("apps.tasks")

# ---- Optional models ---------------------------------------------------------

def _get_holiday_model():
    try:
        return apps.get_model("settings", "Holiday")
    except Exception:
        return None

def _get_checklist_model():
    """
    Retrieve the checklist-like model used for daily/weekly/etc. tasks.
    Try several common names to be resilient to existing schema; must have an
    assignee field and a planned date field.
    """
    candidates = ["Checklist", "ChecklistTask", "TaskChecklist", "Task"]
    for name in candidates:
        try:
            M = apps.get_model("tasks", name)
            field = _planned_date_field(M)
            have_assignee = any(
                a in {f.name for f in M._meta.get_fields()}
                for a in ("assign_to", "assignee", "user", "owner")
            )
            if field and have_assignee:
                return M
        except Exception:
            continue
    return None

# ---- Field/attr helpers ------------------------------------------------------

def _planned_date_field(model) -> Optional[str]:
    """
    Find the field name used as 'planned date/time' for a model.
    Prefers 'planned_date', then 'due_at'/'due_date'/'schedule_at'/'schedule_date'.
    """
    candidates = ["planned_date", "due_at", "due_date", "schedule_at", "schedule_date"]
    model_fields = {f.name: f for f in model._meta.get_fields() if hasattr(f, "name")}
    for name in candidates:
        if name in model_fields:
            return name
    return None

def _is_datetime_field(model, field_name: str) -> bool:
    try:
        f = model._meta.get_field(field_name)
        return f.get_internal_type() in {"DateTimeField", "DateTimeFieldProxy"}
    except Exception:
        return False

def _mode_value(obj) -> str:
    """Return mode string if present (Daily/Weekly/Monthly/Yearly), else ''."""
    for attr in ("mode", "recurring_mode", "frequency_mode"):
        v = getattr(obj, attr, None)
        if v:
            return str(v)
    return ""

def _assignee_filter_kwargs(model, user_id: int) -> dict:
    """Return a filter dict to match 'assigned to' user across naming variants."""
    names = ["assign_to_id", "assignee_id", "user_id", "owner_id"]
    for n in names:
        if n in {f.attname for f in model._meta.fields}:
            return {n: user_id}
    names_fk = ["assign_to", "assignee", "user", "owner"]
    for n in names_fk:
        if n in {f.name for f in model._meta.fields}:
            return {n: user_id}  # Django allows FK equality by pk
    return {}

def _set_status_skipped(obj) -> bool:
    """
    Best-effort: mark an object as 'skipped'.
    Try status='Skipped' if available, else boolean flags like is_skipped/skipped.
    Returns True if something was updated.
    """
    try:
        updated_fields: list[str] = []
        if hasattr(obj, "status"):
            try:
                choices = getattr(getattr(obj, "_meta", None).get_field("status"), "choices", None)
                allowed = {str(c[0]).lower(): c[0] for c in (choices or [])}
                if not choices or "skipped" in allowed:
                    obj.status = allowed.get("skipped", "Skipped")
                    updated_fields.append("status")
            except Exception:
                obj.status = "Skipped"
                updated_fields.append("status")

        for flag in ("is_skipped", "skipped"):
            if hasattr(obj, flag):
                setattr(obj, flag, True)
                updated_fields.append(flag)

        if updated_fields:
            obj.save(
                update_fields=list(set(updated_fields))
                + (["updated_at"] if hasattr(obj, "updated_at") else [])
            )
            return True
    except Exception:
        logger.exception("Failed to mark object as skipped: %r", obj)
    return False

# ---- Working-day helper ------------------------------------------------------

def _holiday_dates_in_range(start_date: date, end_date: date) -> set[date]:
    """
    Return a set of holiday dates between start_date and end_date inclusive,
    if Holiday model exists; else empty set.
    Expected model fields: `date` (DateField) and optional `is_active` boolean.
    """
    Holiday = _get_holiday_model()
    if not Holiday:
        return set()
    try:
        qs = Holiday.objects.all()
        if "is_active" in {f.name for f in Holiday._meta.fields}:
            qs = qs.filter(is_active=True)
        qs = qs.filter(date__gte=start_date, date__lte=end_date).values_list("date", flat=True)
        return set(qs)
    except Exception:
        logger.exception("Failed to fetch Holiday list; falling back to Mon–Fri.")
        return set()

def next_working_day(dt: datetime) -> datetime:
    """
    Compute the next working day AFTER `dt` (strictly later) using the holiday list if available,
    else Mon–Fri. Returned datetime is tz-aware (IST) at midnight; caller may set time.
    """
    local = _aware(dt).astimezone(IST or timezone.get_current_timezone())
    d = local.date() + timedelta(days=1)  # strictly after the given date
    holidays = _holiday_dates_in_range(local.date(), local.date() + timedelta(days=30))  # small window
    while True:
        is_weekend = d.weekday() >= 5  # 5=Sat,6=Sun
        if not is_weekend and d not in holidays:
            break
        d += timedelta(days=1)
    return _aware(datetime.combine(d, time(0, 0)))

# ---- Leave checks used by schedulers/assignment pipelines -------------------

from apps.leave.models import LeaveRequest, LeaveStatus  # noqa: E402

def _pending_applied_before_930_for_day(leave: LeaveRequest, target_day: date) -> bool:
    """
    Pending leaves block only if:
      • they cover target_day (inclusive, IST), AND
      • they were applied on/before that date BEFORE 09:30 IST.
    """
    if leave.status != LeaveStatus.PENDING:
        return False

    if target_day not in _datespan_inclusive_ist(leave.start_at, leave.end_at):
        return False

    applied = _to_ist(getattr(leave, "applied_at", None))
    if not applied:
        return False

    if applied.date() > target_day:
        return False

    anchor_930 = applied.replace(
        year=target_day.year, month=target_day.month, day=target_day.day,
        hour=9, minute=30, second=0, microsecond=0
    )
    return applied <= anchor_930

def is_user_on_leave_for_date(user, target_day: date) -> bool:
    """
    Returns True if the user is considered 'on leave' for target_day (IST):
      • APPROVED leave covers target_day, OR
      • PENDING leave covers target_day AND was applied before 09:30 IST that day.
    """
    if not getattr(user, "id", None):
        return False

    qs = LeaveRequest.objects.filter(employee=user)
    for lr in qs:
        span = _datespan_inclusive_ist(lr.start_at, lr.end_at)
        if target_day not in span:
            continue
        if lr.status == LeaveStatus.APPROVED:
            return True
        if _pending_applied_before_930_for_day(lr, target_day):
            return True
    return False

def should_skip_assignment(user, planned_dt) -> bool:
    """
    Convenience wrapper for pipelines that have a datetime planned_dt.
    Returns True iff the user should NOT receive assignment at planned_dt.
    """
    try:
        d = _ist_date(planned_dt)
        if not d:
            return False
        return is_user_on_leave_for_date(user, d)
    except Exception:
        return False

# ---- Bulk adjustment for already-planned tasks (post-approval hook) ---------

def apply_leave_to_tasks(leave) -> None:
    """
    For APPROVED leaves:
      - Daily-mode tasks on leave days → mark as *skipped* (not completed).
      - Non-daily tasks within the leave window → reschedule to next working day at 10:00 AM IST.
      - Delegations & help tickets: no change.
    All actions are logged and errors are suppressed (never raised to user).
    """
    try:
        if getattr(leave, "status", None) != LeaveStatus.APPROVED:
            return

        start_at = _aware(leave.start_at)
        end_at = _aware(leave.end_at)
        if end_at <= start_at:
            return

        start_date = _date_only(start_at)
        end_date = _date_only(end_at)

        Checklist = _get_checklist_model()
        if not Checklist:
            logger.info("Task integration: no checklist model found; skipping.")
            return

        pd_field = _planned_date_field(Checklist)
        if not pd_field:
            logger.info("Task integration: checklist model has no planned date field; skipping.")
            return

        is_dt_pd = _is_datetime_field(Checklist, pd_field)

        # inclusive window filters
        if is_dt_pd:
            filters_range = {f"{pd_field}__gte": start_at, f"{pd_field}__lte": end_at}
        else:
            filters_range = {f"{pd_field}__gte": start_date, f"{pd_field}__lte": end_date}

        # Filter by assignee
        assign_filter = _assignee_filter_kwargs(Checklist, leave.employee_id)
        if not assign_filter:
            logger.info("Task integration: couldn't detect assignee field on checklist; skipping.")
            return

        qs = Checklist.objects.filter(**assign_filter).filter(**filters_range)
        total = qs.count()
        if total == 0:
            logger.info("Task integration: no tasks in window %s..%s for user id=%s", start_date, end_date, leave.employee_id)
            return

        logger.info("Task integration: processing %s task(s) for leave id=%s", total, getattr(leave, "id", None))
        ten_am = time(10, 0)

        with transaction.atomic():
            for obj in qs.select_for_update():
                mode = (_mode_value(obj) or "").strip().lower()

                # Daily-mode → Mark skipped
                if mode == "daily":
                    if _set_status_skipped(obj):
                        logger.debug("Marked Daily task as skipped (id=%s)", getattr(obj, "id", None))
                    else:
                        logger.debug("Could not mark skipped; leaving as-is (id=%s)", getattr(obj, "id", None))
                    continue

                # Non-daily → reschedule to next working day @ 10:00 IST
                try:
                    value = getattr(obj, pd_field)
                    if value is None:
                        current_pd = start_at
                    else:
                        current_pd = _aware(value) if is_dt_pd else _aware(datetime.combine(value, time(0, 0)))

                    new_dt = next_working_day(current_pd)
                    new_dt = new_dt.astimezone(IST or timezone.get_current_timezone()).replace(
                        hour=ten_am.hour, minute=ten_am.minute, second=0, microsecond=0
                    )

                    if is_dt_pd:
                        setattr(obj, pd_field, new_dt)
                        obj.save(update_fields=[pd_field] + (["updated_at"] if hasattr(obj, "updated_at") else []))
                    else:
                        setattr(obj, pd_field, new_dt.date())
                        obj.save(update_fields=[pd_field] + (["updated_at"] if hasattr(obj, "updated_at") else []))

                    logger.debug(
                        "Rescheduled %s task id=%s to %s",
                        (mode or "non-daily").title(),
                        getattr(obj, "id", None),
                        new_dt.isoformat(),
                    )
                except Exception:
                    logger.exception("Failed to reschedule task id=%s", getattr(obj, "id", None))

        logger.info("Task integration: done for leave id=%s", getattr(leave, "id", None))

    except Exception:
        logger.exception("Task integration encountered an error (leave id=%s)", getattr(leave, "id", None))
        # Never raise to caller


__all__ = [
    "apply_leave_to_tasks",
    "next_working_day",
    "is_user_on_leave_for_date",
    "should_skip_assignment",
]
