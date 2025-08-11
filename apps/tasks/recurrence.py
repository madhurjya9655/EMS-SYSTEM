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


# ---- Recurrence calculation (preserve time) ---------------------------------

def get_next_planned_date(prev_dt, mode, freq, orig_weekday=None, orig_day=None):
    """
    Compute the next planned datetime for a recurring series:

      - Adds by mode/freq using relativedelta
      - PRESERVES the IST wall-clock time from prev_dt
      - If the resulting date is non-working (Sun/Holiday), rolls the DATE
        forward to the next working day but KEEPS the same time
      - RETURNS an aware datetime in the project's timezone

    The legacy parameters `orig_weekday` and `orig_day` are accepted for
    compatibility but are not required under the preserved-time model.
    """
    if mode not in ("Daily", "Weekly", "Monthly", "Yearly"):
        # Fallback: keep the same date/time, just ensure working day
        base_date_ist, base_time_ist = extract_ist_wallclock(prev_dt)
        next_date = next_working_day(base_date_ist)
        return ist_wallclock_to_project_tz(next_date, base_time_ist)

    # 1) Extract IST date + time from seed (preserve this time across recurrences)
    base_date_ist, base_time_ist = extract_ist_wallclock(prev_dt)

    # 2) Build seed IST datetime
    seed_ist = IST.localize(datetime.combine(base_date_ist, base_time_ist))

    # 3) Step forward by frequency using relativedelta
    step = max(int(freq or 1), 1)
    if mode == "Daily":
        next_ist = seed_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = seed_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = seed_ist + relativedelta(months=step)
    else:  # "Yearly"
        next_ist = seed_ist + relativedelta(years=step)

    # 4) If non-working, roll DATE forward but keep the same time
    next_date = next_working_day(next_ist.date())

    # 5) Convert to project timezone for storage/return
    return ist_wallclock_to_project_tz(next_date, base_time_ist)
