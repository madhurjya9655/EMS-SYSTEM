import pytz
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from django.urls import reverse

from .models import Checklist
from apps.settings.models import Holiday
from .email_utils import (
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
)

IST = pytz.timezone("Asia/Kolkata")
TEN_AM = time(10, 0, 0)

# ---- Working day helpers -----------------------------------------------------

def is_working_day(d):
    """Accepts date or datetime; Sundays and DB holidays are non-working."""
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

# ---- Recurrence calculation (future = 10:00 IST, skip Sun/holidays) ---------

def get_next_planned_datetime(prev_dt, mode, freq):
    """
    Compute the next occurrence:
      - Step by mode/freq using relativedelta
      - Set TIME to 10:00 AM IST (future normalization)
      - If non-working, roll DATE forward (keep 10:00)
      - Return aware datetime in the project's timezone
    """
    if not prev_dt or mode not in ("Daily", "Weekly", "Monthly", "Yearly"):
        return None

    # Use IST wall-clock date as the stepping seed (ignore prior time)
    base_date_ist, _ = extract_ist_wallclock(prev_dt)
    seed_ist = IST.localize(datetime.combine(base_date_ist, TEN_AM))

    step = max(int(freq or 1), 1)
    if mode == "Daily":
        next_ist = seed_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = seed_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = seed_ist + relativedelta(months=step)
    else:  # "Yearly"
        next_ist = seed_ist + relativedelta(years=step)

    # Roll forward to next working date (keep 10:00)
    next_date = next_working_day(next_ist.date())
    return ist_wallclock_to_project_tz(next_date, TEN_AM)

# ---- Signal: auto-create next recurring checklist ---------------------------

@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance, created, **kwargs):
    """
    When a recurring checklist item is saved and either:
      - it was completed, or
      - it's at/past due (not a future pending),
    ensure there is ONE future pending item for the same series.

    Future recurrences are normalized to 10:00 AM IST and skip Sundays/holidays.
    """
    # Not a recurring series? Stop.
    if not hasattr(instance, "is_recurring") or not instance.is_recurring():
        return

    now = timezone.now()

    # Do nothing if it's a future pending task (already scheduled)
    if instance.status != "Completed" and instance.planned_date > now:
        return

    # If there is already a future pending item for this series, stop
    future_exists = Checklist.objects.filter(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        group_name=getattr(instance, 'group_name', None),
        planned_date__gt=now,
        status='Pending',
    ).exists()
    if future_exists:
        return

    # Compute next planned datetime (aware, project tz)
    next_planned = get_next_planned_datetime(instance.planned_date, instance.mode, instance.frequency)
    if not next_planned:
        return

    # Safety: if next is still not in the future, advance until it is (series catch-up)
    safety = 0
    while next_planned and next_planned <= now and safety < 730:
        next_planned = get_next_planned_datetime(next_planned, instance.mode, instance.frequency)
        safety += 1
    if not next_planned:
        return

    # Dupe guard (±1 minute) within the same series key
    dupe_exists = Checklist.objects.filter(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        group_name=getattr(instance, 'group_name', None),
        planned_date__gte=next_planned - timedelta(minutes=1),
        planned_date__lt=next_planned + timedelta(minutes=1),
        status='Pending',
    ).exists()
    if dupe_exists:
        return

    # Create the next item (copy relevant fields)
    new_obj = Checklist.objects.create(
        assign_by=instance.assign_by,
        task_name=instance.task_name,
        message=instance.message,
        assign_to=instance.assign_to,
        planned_date=next_planned,  # aware in project tz
        priority=instance.priority,
        attachment_mandatory=instance.attachment_mandatory,
        mode=instance.mode,
        frequency=instance.frequency,
        time_per_task_minutes=instance.time_per_task_minutes,
        remind_before_days=instance.remind_before_days,
        assign_pc=instance.assign_pc,
        notify_to=instance.notify_to,
        set_reminder=instance.set_reminder,
        reminder_mode=instance.reminder_mode,
        reminder_frequency=instance.reminder_frequency,
        reminder_starting_time=instance.reminder_starting_time,
        checklist_auto_close=instance.checklist_auto_close,
        checklist_auto_close_days=instance.checklist_auto_close_days,
        group_name=getattr(instance, 'group_name', None),
        actual_duration_minutes=0,
        status='Pending',
    )

    # Emails for auto-generated recurrence
    site_url = "https://ems-system-d26q.onrender.com"
    complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
    send_checklist_assignment_to_user(task=new_obj, complete_url=complete_url, subject_prefix="Recurring Checklist Generated")
    send_checklist_admin_confirmation(task=new_obj, subject_prefix="Recurring Checklist Generated")
