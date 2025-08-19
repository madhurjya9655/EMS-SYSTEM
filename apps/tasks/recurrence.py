from __future__ import annotations

from datetime import datetime, timedelta, time as dt_time, date
from dateutil.relativedelta import relativedelta
import pytz
import logging

from django.utils import timezone
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

# Project timezone helpers
IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

# Valid recurring modes
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Public API (so imports are stable)
__all__ = [
    "preserve_first_occurrence_time",
    "schedule_recurring_at_10am", 
    "get_next_planned_date",
    "next_working_day",
    "is_working_day",
    "normalize_mode",
    "RECURRING_MODES",
]


# ------------------------
# Utilities
# ------------------------
def _ensure_aware(dt: datetime | None) -> datetime | None:
    """Ensure datetime is timezone-aware"""
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _to_ist(dt: datetime) -> datetime:
    """Convert datetime to IST"""
    dt = _ensure_aware(dt)
    return dt.astimezone(IST)


def _from_ist(dt: datetime) -> datetime:
    """Convert IST datetime to project timezone"""
    return dt.astimezone(timezone.get_current_timezone())


def normalize_mode(mode: str | None) -> str:
    """
    Accepts: Day/Daily, Week/Weekly, Month/Monthly, Year/Yearly (case-insensitive).
    Returns canonical title-cased: Daily, Weekly, Monthly, Yearly.
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


# ------------------------
# Working day rules
# ------------------------
def is_working_day(d: date) -> bool:
    """Working days are Mon–Sat, excluding configured holidays."""
    # Monday=0, Sunday=6
    if d.weekday() == 6:
        return False
    return not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    """Move forward to the next working day (Mon–Sat, not holiday)."""
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


# ------------------------
# First occurrence handling
# ------------------------
def preserve_first_occurrence_time(planned_dt: datetime | None) -> datetime | None:
    """
    FIRST occurrence (manual add / bulk upload):
    - Keep exact time the user chose.
    - If naive, treat as IST and make aware.
    - If the date is Sunday/holiday, shift to next working day but *preserve the time*.
    """
    if not planned_dt:
        return planned_dt

    # Make aware in IST if naive
    if timezone.is_naive(planned_dt):
        planned_dt = IST.localize(planned_dt)

    planned_ist = planned_dt.astimezone(IST)
    d = planned_ist.date()
    t = planned_ist.time()

    if is_working_day(d):
        return _from_ist(planned_ist)

    d2 = next_working_day(d)
    next_dt_ist = IST.localize(datetime.combine(d2, t))
    return _from_ist(next_dt_ist)


# ------------------------
# Recurring occurrence handling
# ------------------------
def schedule_recurring_at_10am(planned_dt: datetime | None) -> datetime | None:
    """
    For RECURRING occurrences:
    - Always schedule at 10:00 AM IST.
    - If that date is Sunday/holiday, push to next working day (still 10:00 IST).
    - Return aware datetime in project timezone.
    """
    if not planned_dt:
        return planned_dt

    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()

    if not is_working_day(d):
        d = next_working_day(d)

    recur_ist = IST.localize(datetime.combine(d, dt_time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    return _from_ist(recur_ist)


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    Compute the next planned datetime for a recurring task based on the *previous* occurrence.
    The next occurrence is returned at 10:00 IST on the next valid working day.

    Examples:
      - Daily + 1 : every day at 10:00 IST (skip Sundays/holidays)
      - Weekly + 2: every 2 weeks on the same weekday, 10:00 IST
      - Monthly + 4: every 4 months on the same day-of-month, 10:00 IST (clamped by calendar rules)
      - Yearly + 1 : same date next year, 10:00 IST
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

        # Enforce the 10:00 IST rule and working-day shift
        return schedule_recurring_at_10am(nxt_ist)
    
    except Exception as e:
        logger.error(f"Error calculating next planned date for mode={m}, frequency={step}: {e}")
        return None


# ------------------------
# Additional utilities for compatibility
# ------------------------
def extract_ist_wallclock(dt: datetime) -> tuple[date, dt_time]:
    """Extract IST date and time components (for backward compatibility)"""
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), dt_time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    """Convert IST date/time to project timezone (for backward compatibility)"""
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt: datetime, mode: str, freq: int) -> datetime | None:
    """Legacy alias for get_next_planned_date (for backward compatibility)"""
    return get_next_planned_date(prev_dt, mode, freq)