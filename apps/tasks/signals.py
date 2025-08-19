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
from . import utils as _utils

from .models import Checklist, Delegation, HelpTicket
from .recurrence import get_next_planned_date, schedule_recurring_at_10am, RECURRING_MODES
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
TEN_AM = time(10, 0, 0)

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)


def is_working_day(d):
    """Check if date is a working day"""
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()


def next_working_day(d):
    """Find next working day"""
    if hasattr(d, "date"):
        d = d.date()
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def _send_recurring_emails_safely(checklist_obj):
    """Send emails for automatically created recurring tasks"""
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
        logger.info(f"Sent emails for recurring checklist {checklist_obj.id}")
    except Exception as e:
        logger.error(f"Failed to send emails for recurring checklist {checklist_obj.id}: {e}")


@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance, created, **kwargs):
    """
    Create next recurring checklist when current one is completed.
    This only triggers on completion, not during bulk upload.
    """
    # Only process if this is a recurring task
    if not hasattr(instance, "is_recurring") or not instance.is_recurring():
        return

    # Only trigger when a task is completed (not when created)
    if instance.status != "Completed":
        return

    # Skip if this is a newly created task (bulk upload, etc.)
    if created:
        return

    logger.info(f"Processing completion of recurring checklist {instance.id}: {instance.task_name}")

    now = timezone.now()

    # Check if future occurrence already exists for this series
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
        logger.info(f"Future occurrence already exists for series {instance.task_name}")
        return

    # Calculate next planned date using the consistent recurrence system
    next_planned = get_next_planned_date(instance.planned_date, instance.mode, instance.frequency)
    if not next_planned:
        logger.warning(f"Could not calculate next planned date for {instance.task_name}")
        return

    # Ensure next occurrence is in the future (catch up if needed)
    safety = 0
    while next_planned and next_planned <= now and safety < 730:
        next_planned = get_next_planned_date(next_planned, instance.mode, instance.frequency)
        safety += 1
    
    if not next_planned:
        logger.warning(f"Could not find future date for {instance.task_name} after {safety} iterations")
        return

    # Check for duplicates (within 1 minute window)
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
        logger.info(f"Duplicate check prevented creation for {instance.task_name} at {next_planned}")
        return

    # Create the next recurring occurrence
    try:
        with transaction.atomic():
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

            # Send emails after successful creation
            transaction.on_commit(lambda: _send_recurring_emails_safely(new_obj))
            logger.info(f" recurring checklist {new_obj.id} '{new_obj.task_name}' for {new_obj.assign_to} at {new_obj.planned_date}")
            
    except Exception as e:
        logger.error(f"Failed to create recurring checklist for {instance.id}: {e}")


@receiver(post_save, sender=Checklist)
def log_checklist_completion(sender, instance, created, **kwargs):
    """Log checklist completion for monitoring"""
    if not created and instance.status == 'Completed':
        logger.info(f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}")


@receiver(post_save, sender=Delegation)  
def log_delegation_completion(sender, instance, created, **kwargs):
    """Log delegation completion for monitoring"""
    if not created and instance.status == 'Completed':
        logger.info(f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}")


@receiver(post_save, sender=HelpTicket)
def log_helpticket_completion(sender, instance, created, **kwargs):
    """Log help ticket completion for monitoring"""
    if not created and instance.status == 'Closed':
        logger.info(f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}")


# Additional signal for bulk upload monitoring
@receiver(post_save, sender=Checklist)
def log_checklist_creation(sender, instance, created, **kwargs):
    """Log checklist creation for bulk upload monitoring"""
    if created:
        logger.debug(f" checklist {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}")


@receiver(post_save, sender=Delegation)
def log_delegation_creation(sender, instance, created, **kwargs):
    """Log delegation creation for bulk upload monitoring"""
    if created:
        logger.debug(f" Created delegation {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}")