# apps/tasks/recurrence.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date

import pytz
from dateutil.relativedelta import relativedelta
from django.utils import timezone

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Project timezone helpers
# -------------------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

# Valid recurring modes
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Public API (so imports remain stable)
__all__ = [
    "preserve_first_occurrence_time",
    "schedule_recurring_at_10am",
    "schedule_recurring_preserve_time",
    "get_next_planned_date",
    "next_working_day",
    "is_working_day",
    "normalize_mode",
    "RECURRING_MODES",
    "extract_ist_wallclock",
    "ist_wallclock_to_project_tz",
    "get_next_planned_datetime",
]

# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------
def _ensure_aware(dt: datetime | None) -> datetime | None:
    """Ensure datetime is timezone-aware in the current project timezone."""
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _to_ist(dt: datetime) -> datetime:
    """Convert datetime to IST (keeps wallclock if already IST)."""
    dt = _ensure_aware(dt)
    return dt.astimezone(IST)


def _from_ist(dt: datetime) -> datetime:
    """Convert IST datetime to the project's timezone."""
    return dt.astimezone(timezone.get_current_timezone())


def _holiday_model():
    """Import Holiday lazily to avoid app-registry timing issues."""
    try:
        from apps.settings.models import Holiday
        return Holiday
    except Exception:
        return None

# -------------------------------------------------------------------
# Mode normalization
# -------------------------------------------------------------------
def normalize_mode(mode: str | None) -> str:
    """
    Accepts: Day/Daily, Week/Weekly, Month/Monthly, Year/Yearly (case-insensitive).
    Returns canonical: Daily, Weekly, Monthly, Yearly (or empty string if invalid).
    """
    if not mode:
        return ""
    m = mode.strip().lower()
    if m in ("day", "daily"):
        return "Daily"
    if m in ("week", "weekly"):
        return "Weekly"
    if m in ("month", "monthly"):
        return "Monthly"
    if m in ("year", "yearly", "annually", "annual"):
        return "Yearly"
    return ""

# -------------------------------------------------------------------
# Working day rules
# -------------------------------------------------------------------
def is_working_day(d: date) -> bool:
    """Working days are Mon–Sat, excluding configured holidays."""
    try:
        if d.weekday() == 6:  # Sunday
            return False
        Holiday = _holiday_model()
        if Holiday is None:
            return True
        return not Holiday.objects.filter(date=d).exists()
    except Exception as e:
        # Fail-safe: only treat Sunday as non-working if DB is unavailable
        logger.debug("is_working_day fallback due to error: %s", e)
        return d.weekday() != 6


def next_working_day(d: date) -> date:
    """Move forward to the next working day (Mon–Sat, not holiday)."""
    tries = 0
    while not is_working_day(d) and tries < 31:
        d += timedelta(days=1)
        tries += 1
    return d

# -------------------------------------------------------------------
# First occurrence handling
# -------------------------------------------------------------------
def preserve_first_occurrence_time(planned_dt: datetime | None) -> datetime | None:
    """
    FIRST occurrence (manual add / bulk upload):
    - Keep the exact date+time the user supplied (planned time is authoritative).
    - If naive, interpret as IST and make aware.
    - Do NOT shift off Sundays/holidays.
    - Return as aware in project timezone.
    """
    if not planned_dt:
        return planned_dt

    if timezone.is_naive(planned_dt):
        # Treat naive user input as IST-local time
        planned_dt = IST.localize(planned_dt)

    # Convert IST -> project tz
    return _from_ist(planned_dt.astimezone(IST))

# -------------------------------------------------------------------
# Recurring occurrence handling
# -------------------------------------------------------------------
def schedule_recurring_at_10am(planned_dt: datetime | None) -> datetime | None:
    """
    Historical helper (kept for compatibility):
    - Schedule at 10:00 AM IST and push to next working day if needed.
    NOTE: For current product rules we prefer schedule_recurring_preserve_time()
    so delay is always counted from the user's planned time-of-day.
    """
    if not planned_dt:
        return planned_dt

    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()

    if not is_working_day(d):
        d = next_working_day(d)

    recur_ist = IST.localize(datetime.combine(d, dt_time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    return _from_ist(recur_ist)


def schedule_recurring_preserve_time(planned_dt: datetime | None) -> datetime | None:
    """
    For RECURRING occurrences:
    - Keep the same wall-clock time as the previous occurrence (e.g., 19:00).
    - If the date is Sunday/holiday, push forward (preserving the time).
    - Return aware datetime in the project timezone.

    This matches the final workflow: visibility may be gated at 10:00 AM on the day
    by the dashboard, but delay is always counted from the planned time.
    """
    if not planned_dt:
        return planned_dt

    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()
    t = dt_time(planned_ist.hour, planned_ist.minute, planned_ist.second, planned_ist.microsecond)

    if not is_working_day(d):
        d = next_working_day(d)

    recur_ist = IST.localize(datetime.combine(d, t))
    return _from_ist(recur_ist)


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    Compute the next planned datetime for a recurring task based on the *previous* occurrence.
    The next occurrence keeps the original planned time-of-day (e.g., 19:00), and will be
    shifted forward to the next working day if the calculated date is Sunday/holiday.

    Examples:
      - Daily + 1 : every day (same time-of-day), skip Sundays/holidays
      - Weekly + 2: every 2 weeks (same weekday/time), skip Sun/hol
      - Monthly + 4: every 4 months (same dom/time), calendar-safe
      - Yearly + 1 : same date next year (same time), calendar-safe
    """
    if not prev_dt:
        return None

    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    step = max(int(frequency or 1), 1)
    prev_ist = _to_ist(prev_dt)

    try:
        if m == "Daily":
            nxt_ist = prev_ist + relativedelta(days=step)
        elif m == "Weekly":
            nxt_ist = prev_ist + relativedelta(weeks=step)
        elif m == "Monthly":
            nxt_ist = prev_ist + relativedelta(months=step)
        elif m == "Yearly":
            nxt_ist = prev_ist + relativedelta(years=step)
        else:
            return None

        # Preserve time-of-day; shift only for non-working days
        return schedule_recurring_preserve_time(nxt_ist)

    except Exception as e:
        logger.error("Error calculating next planned date (mode=%s, freq=%s): %s", m, step, e)
        return None

# -------------------------------------------------------------------
# Compatibility utilities
# -------------------------------------------------------------------
def extract_ist_wallclock(dt: datetime) -> tuple[date, dt_time]:
    """Extract IST date and time components (for backward compatibility)."""
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), dt_time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    """Convert IST date/time to project timezone (for backward compatibility)."""
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt: datetime, mode: str, freq: int) -> datetime | None:
    """Legacy alias for get_next_planned_date (for backward compatibility)."""
    return get_next_planned_date(prev_dt, mode, freq)
