import pytz
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from django.urls import reverse
from django.conf import settings
import logging

from .models import Checklist
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
TEN_AM = time(10, 0, 0)

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", False)


def is_working_day(d):
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()


def next_working_day(d):
    if hasattr(d, "date"):
        d = d.date()
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def extract_ist_wallclock(dt):
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d, t):
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt, mode, freq):
    if not prev_dt or mode not in ("Daily", "Weekly", "Monthly", "Yearly"):
        return None

    base_date_ist, _ = extract_ist_wallclock(prev_dt)
    seed_ist = IST.localize(datetime.combine(base_date_ist, TEN_AM))

    step = max(int(freq or 1), 1)
    if mode == "Daily":
        next_ist = seed_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = seed_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = seed_ist + relativedelta(months=step)
    else:
        next_ist = seed_ist + relativedelta(years=step)

    next_date = next_working_day(next_ist.date())
    return ist_wallclock_to_project_tz(next_date, TEN_AM)


def _send_recurring_emails_safely(checklist_obj):
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    try:
        from .utils import (
            send_checklist_assignment_to_user,
            send_checklist_admin_confirmation,
        )
        
        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[checklist_obj.id])}"
        
        send_checklist_assignment_to_user(
            task=checklist_obj, 
            complete_url=complete_url, 
            subject_prefix="Recurring Checklist Generated"
        )
        send_checklist_admin_confirmation(
            task=checklist_obj, 
            subject_prefix="Recurring Checklist Generated"
        )
    except Exception as e:
        logger.error(f"Failed to send emails for recurring checklist {checklist_obj.id}: {e}")


@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance, created, **kwargs):
    if not hasattr(instance, "is_recurring") or not instance.is_recurring():
        return

    if instance.status != "Completed":
        return

    now = timezone.now()

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

    next_planned = get_next_planned_datetime(instance.planned_date, instance.mode, instance.frequency)
    if not next_planned:
        return

    safety = 0
    while next_planned and next_planned <= now and safety < 730:
        next_planned = get_next_planned_datetime(next_planned, instance.mode, instance.frequency)
        safety += 1
    if not next_planned:
        return

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

    try:
        new_obj = Checklist.objects.create(
            assign_by=instance.assign_by,
            task_name=instance.task_name,
            message=instance.message,
            assign_to=instance.assign_to,
            planned_date=next_planned,
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

        transaction.on_commit(lambda: _send_recurring_emails_safely(new_obj))
        logger.info(f"Created recurring checklist {new_obj.id} for series {instance.task_name}")
        
    except Exception as e:
        logger.error(f"Failed to create recurring checklist for {instance.id}: {e}")


@receiver(post_save, sender=Checklist)
def log_checklist_completion(sender, instance, created, **kwargs):
    if not created and instance.status == 'Completed':
        logger.info(f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}")


from .models import Delegation, HelpTicket

@receiver(post_save, sender=Delegation)
def log_delegation_completion(sender, instance, created, **kwargs):
    if not created and instance.status == 'Completed':
        logger.info(f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}")


@receiver(post_save, sender=HelpTicket)
def log_helpticket_completion(sender, instance, created, **kwargs):
    if not created and instance.status == 'Closed':
        logger.info(f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}")