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
    Given an aware datetime, return (IST date, IST time-of-day).
    """
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d, t):
    """
    Build an aware IST datetime from date+time and convert to the project's timezone.
    """
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


# ---- Recurrence calculation (preserve time) ---------------------------------

def get_next_planned_datetime(prev_dt, mode, freq):
    """
    Compute the next occurrence:
      - Add by mode/freq using relativedelta
      - KEEP the same IST time-of-day as prev_dt
      - If non-working, roll DATE forward (keep time)
      - Return aware datetime in the project's timezone
    """
    if not prev_dt or mode not in ("Daily", "Weekly", "Monthly", "Yearly"):
        return None

    # Extract seed wall-clock in IST
    prev_date_ist, prev_time_ist = extract_ist_wallclock(prev_dt)

    # Start from prev_dt in IST, add frequency
    cur_ist = IST.localize(datetime.combine(prev_date_ist, prev_time_ist))

    step = max(int(freq or 1), 1)
    if mode == "Daily":
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == "Weekly":
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == "Yearly":
        cur_ist = cur_ist + relativedelta(years=step)

    # If resulting date is non-working, roll date forward but keep the time
    next_date = cur_ist.date()
    if not is_working_day(next_date):
        next_date = next_working_day(next_date)

    # Convert to project timezone for storage
    return ist_wallclock_to_project_tz(next_date, prev_time_ist)


# ---- Signal: auto-create next recurring checklist ---------------------------

@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance, created, **kwargs):
    """
    When a recurring checklist item is saved and either:
      - it was completed, or
      - it's in the past/now (not a future pending),
    ensure there is a future pending item for the same series.

    This preserves the original IST time-of-day across recurrences and
    rolls non-working dates forward without changing the time.
    """
    # The model must provide this; if not recurring, stop
    if not hasattr(instance, "is_recurring") or not instance.is_recurring():
        return

    # Do nothing if it's a future pending task (already scheduled)
    if instance.status != "Completed" and instance.planned_date > timezone.now():
        return

    # If there is already a future pending item in this series, stop
    future_exists = Checklist.objects.filter(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        planned_date__gt=instance.planned_date,
        status='Pending',
    ).exists()
    if future_exists:
        return

    # Compute next planned datetime (aware, project tz)
    next_planned = get_next_planned_datetime(instance.planned_date, instance.mode, instance.frequency)
    if not next_planned:
        return

    # Double-check no item already exists around that timestamp (±1 minute)
    dupe_exists = Checklist.objects.filter(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        planned_date__gte=next_planned - timedelta(minutes=1),
        planned_date__lt=next_planned + timedelta(minutes=1),
        status='Pending',
    ).exists()
    if dupe_exists:
        return

    # Create the next item (copy only existing fields)
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
