# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\recurrence.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date
from typing import Optional, Tuple

import pytz
from dateutil.relativedelta import relativedelta
from django.utils import timezone

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Project timezone helpers
# -------------------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")

# Dashboard gating anchor (kept for compatibility helpers)
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

# Fixed planned time for ALL FUTURE recurrences
EVENING_HOUR = 19  # 7:00 PM IST
EVENING_MINUTE = 0

# Valid recurring modes
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Public API (so existing imports remain stable)
__all__ = [
    "preserve_first_occurrence_time",
    "schedule_recurring_at_10am",
    "schedule_recurring_preserve_time",
    "schedule_recurring_at_7pm",
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
def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensure datetime is timezone-aware in the current project timezone."""
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _to_ist(dt: datetime) -> datetime:
    """Convert datetime to IST (keeps wall-clock if already IST)."""
    dt = _ensure_aware(dt)
    return dt.astimezone(IST)  # type: ignore[union-attr]


def _from_ist(dt: datetime) -> datetime:
    """Convert IST datetime to the project's timezone."""
    return dt.astimezone(timezone.get_current_timezone())


def _holiday_model():
    """Import Holiday lazily to avoid app-registry timing issues."""
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday
    except Exception:
        return None

# -------------------------------------------------------------------
# Mode normalization
# -------------------------------------------------------------------
def normalize_mode(mode: Optional[str]) -> str:
    """
    Accepts: Day/Daily, Week/Weekly, Month/Monthly, Year/Yearly/Annual/Annually (case-insensitive).
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
def preserve_first_occurrence_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    FIRST occurrence (manual add / bulk upload):
    - Keep the exact date+time the user supplied (planned time is authoritative).
    - If naive, interpret as IST and make aware.
    - Do NOT shift off Sundays/holidays.
    - Return as aware in project timezone.
    """
    if planned_dt is None:
        return None

    # If naive, treat as IST wall-clock; else respect given tz
    if timezone.is_naive(planned_dt):
        ist_dt = IST.localize(planned_dt)
    else:
        ist_dt = planned_dt.astimezone(IST)

    # Convert IST -> project tz
    return _from_ist(ist_dt)

# -------------------------------------------------------------------
# Recurring occurrence helpers
# -------------------------------------------------------------------
def schedule_recurring_at_10am(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    Historical helper (kept for compatibility):
    - Schedule at 10:00 AM IST and push to next working day if needed.
    NOTE: Not used for the new rule; kept so old imports keep working.
    """
    if not planned_dt:
        return planned_dt

    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()

    if not is_working_day(d):
        d = next_working_day(d)

    recur_ist = IST.localize(datetime.combine(d, dt_time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    return _from_ist(recur_ist)


def schedule_recurring_preserve_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    Compatibility helper (not used by the new rule):
    - Keep the same wall-clock time as the given datetime.
    - If the date is Sunday/holiday, push forward (preserving the time).
    - Return aware datetime in the project timezone.
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


def schedule_recurring_at_7pm(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    New helper for the final rule:
    - Set time to 19:00 IST (7 PM) on the same date.
    - If Sunday/holiday, push forward to the next working day at 19:00.
    - Return in project timezone.
    """
    if not planned_dt:
        return planned_dt

    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()

    if not is_working_day(d):
        d = next_working_day(d)

    recur_ist = IST.localize(datetime.combine(d, dt_time(EVENING_HOUR, EVENING_MINUTE)))
    return _from_ist(recur_ist)

# -------------------------------------------------------------------
# Next recurrence computation (ALWAYS 19:00 IST on working day)
# -------------------------------------------------------------------
def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> Optional[datetime]:
    """
    Compute the next planned datetime for a recurring task based on the *previous* occurrence.
    FINAL RULE:
      - The next occurrence is scheduled at **19:00 IST** on the calculated date.
      - If that date is Sunday/holiday, move to the next working day at **19:00 IST**.
      - Only the DATE advances per mode/frequency; the TIME is fixed at 19:00.
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

        # Force 19:00 IST on a working day
        nxt_date = nxt_ist.date()
        if not is_working_day(nxt_date):
            nxt_date = next_working_day(nxt_date)

        nxt_19_ist = IST.localize(datetime.combine(nxt_date, dt_time(EVENING_HOUR, EVENING_MINUTE)))
        return _from_ist(nxt_19_ist)

    except Exception as e:
        logger.error("Error calculating next planned date (mode=%s, freq=%s): %s", m, step, e)
        return None

# -------------------------------------------------------------------
# Compatibility utilities
# -------------------------------------------------------------------
def extract_ist_wallclock(dt: datetime) -> Tuple[date, dt_time]:
    """Extract IST date and time components (for backward compatibility)."""
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), dt_time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    """Convert IST date/time to project timezone (for backward compatibility)."""
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt: datetime, mode: str, freq: int) -> Optional[datetime]:
    """Legacy alias for get_next_planned_date (for backward compatibility)."""
    return get_next_planned_date(prev_dt, mode, freq)
