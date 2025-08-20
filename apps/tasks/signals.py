# apps/tasks/signals.py
import logging
from datetime import timedelta

import pytz
from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from .recurrence import get_next_planned_date, schedule_recurring_at_10am, RECURRING_MODES
from . import utils as _utils  # email helpers & console-safe logging

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    """
    True if now (IST) is within [10:00 - leeway, 10:00 + leeway].
    Default window: 09:55–10:05 IST.
    """
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _send_recurring_emails_safely(checklist_obj: Checklist) -> None:
    """
    Send emails for automatically created recurring tasks, respecting settings gates.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    if SEND_RECUR_EMAILS_ONLY_AT_10AM and not _within_10am_ist_window():
        logger.info(
            _utils._safe_console_text(
                f"Skipping immediate recurring email for {checklist_obj.id}; "
                f"outside 10:00 IST window."
            )
        )
        return

    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[checklist_obj.id])}"
        _utils.send_checklist_assignment_to_user(
            task=checklist_obj,
            complete_url=complete_url,
            subject_prefix="Recurring Checklist Generated",
        )
        _utils.send_checklist_admin_confirmation(
            task=checklist_obj,
            subject_prefix="Recurring Checklist Generated",
        )
        logger.info(_utils._safe_console_text(f"Sent recurring emails for checklist {checklist_obj.id}"))
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Failed to send recurring emails for {checklist_obj.id}: {e}"))


@receiver(post_save, sender=Checklist)
def create_next_recurring_checklist(sender, instance: Checklist, created: bool, **kwargs):
    """
    When a recurring checklist is marked 'Completed', create the next occurrence:
      • Valid only for modes in RECURRING_MODES
      • Trigger on update (not initial create)
      • Next occurrence scheduled via recurrence helpers (10:00 IST; skip Sun/holidays)
      • Prevent duplicates within a 1-minute window
    """
    # Only recurring series
    if (instance.mode or "") not in RECURRING_MODES:
        return
    # Only when status transitioned to Completed
    if instance.status != "Completed":
        return
    # Ignore the initial create
    if created:
        return

    now = timezone.now()

    # If a future pending of this same series already exists, don't create another.
    series_filter = dict(
        assign_to=instance.assign_to,
        task_name=instance.task_name,
        mode=instance.mode,
        frequency=instance.frequency,
        group_name=getattr(instance, "group_name", None),
    )
    if Checklist.objects.filter(status="Pending", planned_date__gt=now, **series_filter).exists():
        return

    # Compute next planned date using consistent recurrence system
    next_dt = get_next_planned_date(instance.planned_date, instance.mode, instance.frequency)
    if not next_dt:
        logger.warning(_utils._safe_console_text(f"No next date for recurring checklist {instance.id}"))
        return

    # Catch-up loop: ensure next occurrence lands in the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:
        next_dt = get_next_planned_date(next_dt, instance.mode, instance.frequency)
        safety += 1
    if not next_dt:
        logger.warning(_utils._safe_console_text(f"Could not find a future date for series '{instance.task_name}'"))
        return

    # Force to 10:00 IST and ensure working-day shift (idempotent if already 10:00 & valid)
    next_dt = schedule_recurring_at_10am(next_dt)

    # Duplicate guard (±1 minute window)
    dupe_exists = Checklist.objects.filter(
        status="Pending",
        planned_date__gte=next_dt - timedelta(minutes=1),
        planned_date__lt=next_dt + timedelta(minutes=1),
        **series_filter,
    ).exists()
    if dupe_exists:
        logger.info(_utils._safe_console_text(f"Duplicate prevented for '{instance.task_name}' at {next_dt}"))
        return

    try:
        with transaction.atomic():
            new_obj = Checklist.objects.create(
                assign_by=instance.assign_by,
                task_name=instance.task_name,
                message=instance.message,
                assign_to=instance.assign_to,
                planned_date=next_dt,
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
                group_name=getattr(instance, "group_name", None),
                actual_duration_minutes=0,
                status="Pending",
            )

            # After commit, send emails (policy gated)
            transaction.on_commit(lambda: _send_recurring_emails_safely(new_obj))

            logger.info(
                _utils._safe_console_text(
                    f"Created next recurring checklist {new_obj.id} '{new_obj.task_name}' at {new_obj.planned_date}"
                )
            )
    except Exception as e:
        logger.error(_utils._safe_console_text(f"Failed to create recurring checklist for {instance.id}: {e}"))


@receiver(post_save, sender=Checklist)
def log_checklist_completion(sender, instance, created, **kwargs):
    """Log checklist completion for monitoring."""
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Delegation)
def log_delegation_completion(sender, instance, created, **kwargs):
    """Log delegation completion for monitoring."""
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=HelpTicket)
def log_helpticket_completion(sender, instance, created, **kwargs):
    """Log help ticket completion for monitoring."""
    if not created and instance.status == "Closed":
        logger.info(_utils._safe_console_text(
            f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Checklist)
def log_checklist_creation(sender, instance, created, **kwargs):
    """Log checklist creation for bulk upload monitoring."""
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created checklist {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))


@receiver(post_save, sender=Delegation)
def log_delegation_creation(sender, instance, created, **kwargs):
    """Log delegation creation for bulk upload monitoring."""
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created delegation {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))
