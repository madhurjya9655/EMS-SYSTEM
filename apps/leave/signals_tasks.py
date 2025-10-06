from __future__ import annotations

import logging
from typing import Optional

from django.apps import apps
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

log = logging.getLogger(__name__)

DONE_VALUES = {"completed", "closed", "done"}  # case-insensitive

def _status_is_done(instance) -> bool:
    val = getattr(instance, "status", None)
    if not isinstance(val, str):
        return False
    return val.strip().lower() in DONE_VALUES

def _handover_for(instance):
    """Return active LeaveHandover row for this task instance, or None."""
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    LeaveStatus = apps.get_model("leave", "LeaveStatus")
    if not LeaveHandover or not LeaveStatus:
        return None

    # infer type from model class name
    model_name = instance.__class__.__name__.lower()
    if "checklist" in model_name:
        tt = "checklist"
    elif "delegation" in model_name:
        tt = "delegation"
    elif "help" in model_name or "ticket" in model_name:
        tt = "help_ticket"
    else:
        return None

    today = timezone.localdate()

    try:
        return (
            LeaveHandover.objects
            .select_related("leave_request", "original_assignee", "new_assignee")
            .filter(
                task_type=tt,
                original_task_id=instance.id,
                is_active=True,
                effective_start_date__lte=today,
                effective_end_date__gte=today,
                leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
            )
            .first()
        )
    except Exception:
        log.exception("Failed finding handover for %r", instance)
        return None

def _deactivate_reminders(handover):
    try:
        DelegationReminder = apps.get_model("leave", "DelegationReminder")
        if DelegationReminder:
            DelegationReminder.objects.filter(leave_handover=handover, is_active=True).update(is_active=False)
    except Exception:
        log.exception("Could not deactivate reminders for handover id=%s", getattr(handover, "id", None))

def _notify_completion(handover):
    try:
        from apps.leave.services.notifications import send_handover_completion_email
        send_handover_completion_email(handover)
    except Exception:
        log.exception("Failed sending completion email for handover id=%s", getattr(handover, "id", None))

def _connect_sender(sender_label: str):
    Model = apps.get_model("tasks", sender_label)
    if not Model:
        return

    @receiver(post_save, sender=Model, dispatch_uid=f"leave-complete-{sender_label}")
    def _on_task_saved(sender, instance, created, **kwargs):
        # Only when status flips (or is saved as) a done-like value
        try:
            if not _status_is_done(instance):
                return
            ho = _handover_for(instance)
            if not ho:
                return
            # stop reminders + notify original
            _deactivate_reminders(ho)
            _notify_completion(ho)
        except Exception:
            log.exception("Completion hook failed for %s", sender_label)

# connect for the three task models (if they exist)
def connect_all_task_completion_signals():
    for label in ("Checklist", "Delegation", "HelpTicket"):
        try:
            _connect_sender(label)
        except Exception:
            log.exception("Could not connect completion signal for %s", label)
