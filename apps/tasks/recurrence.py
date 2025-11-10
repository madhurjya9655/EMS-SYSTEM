from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date
from typing import Optional, Tuple

import pytz
from dateutil.relativedelta import relativedelta
from django.utils import timezone

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

EVENING_HOUR = 19
EVENING_MINUTE = 0

RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

__all__ = [
    "preserve_first_occurrence_time",
    "schedule_recurring_at_10am",
    "schedule_recurring_preserve_time",
    "schedule_recurring_at_7pm",
    "get_next_planned_date",
    "get_next_planned_datetime",
    "get_next_fixed_7pm",
    "get_next_same_time",
    "next_working_day",
    "is_working_day",
    "normalize_mode",
    "RECURRING_MODES",
    "extract_ist_wallclock",
    "ist_wallclock_to_project_tz",
]


def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _to_ist(dt: datetime) -> datetime:
    dt = _ensure_aware(dt)
    return dt.astimezone(IST)  # type: ignore[union-attr]


def _from_ist(dt: datetime) -> datetime:
    return dt.astimezone(timezone.get_current_timezone())


def _holiday_model():
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday
    except Exception:
        return None


def normalize_mode(mode: Optional[str]) -> str:
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


def is_working_day(d: date) -> bool:
    try:
        if d.weekday() == 6:  # Sunday
            return False
        Holiday = _holiday_model()
        if Holiday is None:
            return True
        return not Holiday.objects.filter(date=d).exists()
    except Exception as e:
        logger.debug("is_working_day fallback due to error: %s", e)
        return d.weekday() != 6


def next_working_day(d: date) -> date:
    tries = 0
    while not is_working_day(d) and tries < 31:
        d += timedelta(days=1)
        tries += 1
    return d


# -------------------------------------------------------------------
# Pin first occurrence to 19:00 IST on SAME date user picked
# -------------------------------------------------------------------
def preserve_first_occurrence_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    if planned_dt is None:
        return None
    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()
    fixed_ist = IST.localize(datetime.combine(d, dt_time(EVENING_HOUR, EVENING_MINUTE)))
    return _from_ist(fixed_ist)


# -------------------------------------------------------------------
# Schedulers for 10 AM / preserve-time / 7 PM
# -------------------------------------------------------------------
def schedule_recurring_at_10am(planned_dt: Optional[datetime]) -> Optional[datetime]:
    if not planned_dt:
        return planned_dt
    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()
    if not is_working_day(d):
        d = next_working_day(d)
    recur_ist = IST.localize(datetime.combine(d, dt_time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    return _from_ist(recur_ist)


def schedule_recurring_preserve_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    if not planned_dt:
        return planned_dt
    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()
    t = dt_time(
        planned_ist.hour,
        planned_ist.minute,
        planned_ist.second,
        planned_ist.microsecond,
    )
    if not is_working_day(d):
        d = next_working_day(d)
    recur_ist = IST.localize(datetime.combine(d, t))
    return _from_ist(recur_ist)


def schedule_recurring_at_7pm(planned_dt: Optional[datetime]) -> Optional[datetime]:
    if not planned_dt:
        return planned_dt
    planned_ist = _to_ist(planned_dt)
    d = planned_ist.date()
    recur_ist = IST.localize(datetime.combine(d, dt_time(EVENING_HOUR, EVENING_MINUTE)))
    return _from_ist(recur_ist)


# -------------------------------------------------------------------
# Core recurrence: next working day at 19:00 IST
# -------------------------------------------------------------------
def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> Optional[datetime]:
    """
    Step forward by `frequency` units in the given `mode`,
    then shift to the next working day (if needed) at 19:00 IST.
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

        nxt_date = nxt_ist.date()
        if not is_working_day(nxt_date):
            nxt_date = next_working_day(nxt_date)

        nxt_19_ist = IST.localize(
            datetime.combine(nxt_date, dt_time(EVENING_HOUR, EVENING_MINUTE))
        )
        return _from_ist(nxt_19_ist)
    except Exception as e:
        logger.error(
            "Error calculating next planned date (mode=%s, freq=%s): %s", m, step, e
        )
        return None


# -------------------------------------------------------------------
# Helpers expected by signals.py
# -------------------------------------------------------------------
def get_next_fixed_7pm(
    prev_dt: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Wrapper used by signals:

      • Steps according to mode/frequency
      • Shifts Sun/holiday → next working day
      • Pins time to 19:00 IST
      • Ignores end_date (kept for API compatibility)
    """
    return get_next_planned_date(prev_dt, mode, frequency)


def get_next_same_time(
    prev_dt: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Step forward preserving IST wall-clock time instead of forcing 19:00.

    Still applies working-day shift, but keeps the same time-of-day
    (useful if any code wants "same hour/minute" recurrence).
    """
    if not prev_dt:
        return None
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    step = max(int(frequency or 1), 1)
    prev_ist = _to_ist(prev_dt)
    t = dt_time(
        prev_ist.hour,
        prev_ist.minute,
        prev_ist.second,
        prev_ist.microsecond,
    )

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

        nxt_date = nxt_ist.date()
        if not is_working_day(nxt_date):
            nxt_date = next_working_day(nxt_date)

        nxt_ist_fixed = IST.localize(datetime.combine(nxt_date, t))
        return _from_ist(nxt_ist_fixed)
    except Exception as e:
        logger.error(
            "Error calculating next same-time datetime (mode=%s, freq=%s): %s",
            m,
            step,
            e,
        )
        return None


# -------------------------------------------------------------------
# Misc helpers
# -------------------------------------------------------------------
def extract_ist_wallclock(dt: datetime) -> Tuple[date, dt_time]:
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), dt_time(
        dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond
    )


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt: datetime, mode: str, freq: int) -> Optional[datetime]:
    """Alias kept for backwards compatibility."""
    return get_next_planned_date(prev_dt, mode, freq)
