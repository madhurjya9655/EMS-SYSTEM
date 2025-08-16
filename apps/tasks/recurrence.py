# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\recurrence.py
from __future__ import annotations

from datetime import datetime, timedelta, time as dt_time, date
from dateutil.relativedelta import relativedelta
import pytz

from django.utils import timezone

from apps.settings.models import Holiday

RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Fixed business rule
IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0


def _ensure_aware(dt: datetime) -> datetime:
    """Make a datetime timezone-aware in the current Django TZ if naive."""
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def is_working_day(d: date) -> bool:
    """Working days are Mon–Sat, excluding configured admin holidays."""
    # Sunday == 6
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    """Move forward to the next working day (Mon–Sat and not a holiday)."""
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def keep_first_occurrence(user_dt: datetime | None) -> datetime | None:
    """
    For the *first* manually created/uploaded task:
    - Respect the chosen date and time if present.
    - If the time looks 'date-only' (00:00), normalize to 10:00 IST for consistency.
    - Do NOT shift off Sundays/holidays here; only recurrences must skip them.
    """
    if not user_dt:
        return None

    dt_aw = _ensure_aware(user_dt)

    # If user gave a "date" (00:00), normalize to 10:00 IST
    if dt_aw.hour == 0 and dt_aw.minute == 0 and dt_aw.second == 0 and dt_aw.microsecond == 0:
        # convert to IST, set 10:00, convert back to project TZ
        ist_dt = dt_aw.astimezone(IST)
        ist_dt = ist_dt.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)
        dt_aw = ist_dt.astimezone(timezone.get_current_timezone())

    return dt_aw


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    Compute the next planned datetime for a recurring Checklist based on the previous
    occurrence. The returned datetime is ALWAYS at 10:00 IST on the next valid
    working day (Mon–Sat, skipping configured holidays).

    Examples:
      - Daily + 1 : every day at 10:00 IST (skip Sundays/holidays)
      - Weekly + 2: every 2 weeks on the same weekday as the seed, 10:00 IST
      - Monthly + 4: every 4 months on the same calendar day, 10:00 IST
    """
    if (mode or "") not in RECURRING_MODES or not prev_dt:
        return None

    step = max(int(frequency or 1), 1)

    # Work entirely in IST to avoid DST/offset weirdness for the 10:00 rule
    prev_aw = _ensure_aware(prev_dt)
    prev_ist = prev_aw.astimezone(IST)

    if mode == "Daily":
        next_ist = prev_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = prev_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = prev_ist + relativedelta(months=step)
    elif mode == "Yearly":
        next_ist = prev_ist + relativedelta(years=step)
    else:
        return None

    # Enforce the 10:00 IST rule
    next_ist = next_ist.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)

    # Skip Sundays & holidays
    next_ist_date = next_ist.date()
    next_ist_date = next_working_day(next_ist_date)
    next_ist = next_ist.replace(year=next_ist_date.year, month=next_ist_date.month, day=next_ist_date.day)

    # Return in project TZ
    return next_ist.astimezone(timezone.get_current_timezone())
