# apps/tasks/recurrence.py
import pytz
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from django.utils import timezone
from apps.settings.models import Holiday

IST = pytz.timezone('Asia/Kolkata')

# ---- Working-day helpers -----------------------------------------------------

def is_working_day(d):
    """Sunday (6) and DB holidays are non-working."""
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()

def next_working_day(d):
    """Advance forward until we land on a working date."""
    if hasattr(d, "date"):
        d = d.date()
    while not is_working_day(d):
        d += timedelta(days=1)
    return d

# ---- Timezone helpers --------------------------------------------------------

def extract_ist_wallclock(dt):
    """
    Given a datetime (aware or naive), return (IST date, IST time-of-day).
    Naive inputs are treated as IST.
    """
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)

def ist_wallclock_to_project_tz(d, t):
    """
    Build an aware IST datetime from date+time and convert to the project's timezone.
    """
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())

# ---- First occurrence helper -------------------------------------------------

def keep_first_occurrence(dt):
    """
    FIRST OCCURRENCE ONLY:
    - Keep EXACT date & time from manual entry or bulk upload.
    - Make timezone-aware (assume IST if naive).
    - Store/return in project timezone without Sunday/holiday shift.
    """
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    return dt.astimezone(timezone.get_current_timezone())

# ---- Recurrence calculation (future instances at 10:00 AM IST) ---------------

def get_next_planned_date(prev_dt, mode, freq, orig_weekday=None, orig_day=None):
    """
    Compute the next planned datetime for a recurring series:

      - Steps forward by mode/freq.
      - FUTURE OCCURRENCES always at 10:00 AM IST.
      - Skips Sundays & holidays for DATE (time stays 10:00).
      - Returns aware datetime in project timezone.
    """
    if mode not in ("Daily", "Weekly", "Monthly", "Yearly"):
        # Fallback: just roll to next working day at 10:00
        base_date_ist, _ = extract_ist_wallclock(prev_dt)
        next_date = next_working_day(base_date_ist)
        return ist_wallclock_to_project_tz(next_date, time(10, 0))

    # Step 1: Extract IST date from previous occurrence (ignore its time)
    base_date_ist, _ = extract_ist_wallclock(prev_dt)

    # Step 2: Build seed IST datetime at 10:00
    seed_ist = IST.localize(datetime.combine(base_date_ist, time(10, 0)))

    # Step 3: Step forward by frequency
    step = max(int(freq or 1), 1)
    if mode == "Daily":
        next_ist = seed_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = seed_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = seed_ist + relativedelta(months=step)
    else:  # Yearly
        next_ist = seed_ist + relativedelta(years=step)

    # Step 4: Skip Sundays/holidays (keep 10:00)
    next_date = next_working_day(next_ist.date())

    # Step 5: Return in project timezone
    return ist_wallclock_to_project_tz(next_date, time(10, 0))
