from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Iterable, Optional

from django.apps import apps
from django.conf import settings
from django.db import transaction
from django.utils import timezone

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

IST = ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata")) if ZoneInfo else None

# Use the tasks logger channel as requested
logger = logging.getLogger("apps.tasks")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _aware(dt: datetime) -> datetime:
    """Ensure tz-aware (IST-preferred)."""
    if timezone.is_aware(dt):
        return dt
    tz = IST or timezone.get_current_timezone()
    return timezone.make_aware(dt, tz)


def _date_only(dt: datetime) -> datetime.date:
    return _aware(dt).astimezone(IST or timezone.get_current_timezone()).date()


def _get_holiday_model():
    """Return Holiday model if it exists; else None."""
    try:
        return apps.get_model("settings", "Holiday")
    except Exception:
        return None


def _holiday_dates_in_range(start_date, end_date) -> set:
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
        # Try to filter by active if present
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
    else Mon–Fri. Returned datetime keeps date only; time should be set by caller.
    """
    local = _aware(dt).astimezone(IST or timezone.get_current_timezone())
    d = local.date() + timedelta(days=1)  # strictly after the given date
    holidays = _holiday_dates_in_range(local.date(), local.date() + timedelta(days=30))  # small window
    while True:
        is_weekend = d.weekday() >= 5  # 5=Sat,6=Sun
        if not is_weekend and d not in holidays:
            break
        d += timedelta(days=1)
    # Return aware dt at midnight for that date (caller sets time)
    return _aware(datetime.combine(d, time(0, 0)))


def _set_status_skipped(obj) -> bool:
    """
    Best-effort: mark an object as 'skipped'.
    Try status='Skipped' if available, else boolean flags like is_skipped/skipped.
    Returns True if something was updated.
    """
    try:
        updated_fields = []
        # Common pattern: status field has choices; try to set 'Skipped' if allowed
        if hasattr(obj, "status"):
            try:
                # If choices exist, respect them; else just assign string
                choices = getattr(getattr(obj, "_meta", None).get_field("status"), "choices", None)
                allowed = {str(c[0]).lower(): c[0] for c in (choices or [])}
                if not choices or "skipped" in allowed:
                    obj.status = allowed.get("skipped", "Skipped")
                    updated_fields.append("status")
            except Exception:
                obj.status = "Skipped"
                updated_fields.append("status")

        # Fallback booleans commonly used
        for flag in ("is_skipped", "skipped"):
            if hasattr(obj, flag):
                setattr(obj, flag, True)
                updated_fields.append(flag)

        if updated_fields:
            obj.save(update_fields=list(set(updated_fields)) + ["updated_at"] if hasattr(obj, "updated_at") else list(set(updated_fields)))
            return True
    except Exception:
        logger.exception("Failed to mark object as skipped: %r", obj)
    return False


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


def _mode_value(obj) -> str:
    """Return mode string if present (Daily/Weekly/Monthly/Yearly), else ''."""
    for attr in ("mode", "recurring_mode", "frequency_mode"):
        v = getattr(obj, attr, None)
        if v:
            return str(v)
    return ""


def _is_datetime_field(model, field_name: str) -> bool:
    try:
        f = model._meta.get_field(field_name)
        return f.get_internal_type() in {"DateTimeField", "DateTimeFieldProxy"}
    except Exception:
        return False


def _get_checklist_model():
    """
    Retrieve the checklist-like model used for daily/weekly/etc. tasks.
    Try several common names to be resilient to existing schema.
    """
    candidates = ["Checklist", "ChecklistTask", "TaskChecklist", "Task"]
    for name in candidates:
        try:
            M = apps.get_model("tasks", name)
            # must have assignee and a planned date
            field = _planned_date_field(M)
            if field and any(a in {f.name for f in M._meta.get_fields()} for a in ("assign_to", "assignee", "user")):
                return M
        except Exception:
            continue
    return None


def _assignee_filter_kwargs(model, user_id: int) -> dict:
    """Return a filter dict to match 'assigned to' user across naming variants."""
    names = ["assign_to_id", "assignee_id", "user_id", "owner_id"]
    for n in names:
        if n in {f.attname for f in model._meta.fields}:
            return {n: user_id}
    # Fallback: if relation is not _id style, try direct FK name
    names_fk = ["assign_to", "assignee", "user", "owner"]
    for n in names_fk:
        if n in {f.name for f in model._meta.fields}:
            return {n: user_id}
    return {}  # best effort


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_leave_to_tasks(leave) -> None:
    """
    For APPROVED leaves:
      - Daily-mode tasks on leave days → mark as *skipped* (not completed).
      - Non-daily tasks within the leave window → reschedule to next working day at 10:00 AM IST.
      - Delegations & help tickets: no change.
    All actions are logged to 'apps.tasks' and errors are suppressed (never raised to user).
    """
    try:
        if getattr(leave, "status", "") != "APPROVED":
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
        # Build date range filters depending on field type
        if is_dt_pd:
            # inclusive window
            filters_range = {f"{pd_field}__gte": start_at, f"{pd_field}__lte": end_at}
        else:
            filters_range = {f"{pd_field}__gte": start_date, f"{pd_field}__lte": end_date}

        # Filter by assignee
        assign_filter = _assignee_filter_kwargs(Checklist, leave.employee_id)
        if not assign_filter:
            logger.info("Task integration: couldn't detect assignee field on checklist; skipping.")
            return

        # Fetch relevant tasks
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

                # Non-daily → reschedule to next working day @ 10:00 IST (date-only or datetime)
                try:
                    current_pd: datetime | None
                    value = getattr(obj, pd_field)

                    if value is None:
                        # If missing, base on start_at
                        current_pd = start_at
                    else:
                        # Convert to datetime
                        if is_dt_pd:
                            current_pd = _aware(value)
                        else:
                            # Date field → treat as midnight local
                            current_pd = _aware(datetime.combine(value, time(0, 0)))

                    new_dt = next_working_day(current_pd)
                    # Set time to 10:00 IST
                    new_dt = new_dt.astimezone(IST or timezone.get_current_timezone()).replace(hour=ten_am.hour, minute=ten_am.minute, second=0, microsecond=0)

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


__all__ = ["apply_leave_to_tasks", "next_working_day"]
