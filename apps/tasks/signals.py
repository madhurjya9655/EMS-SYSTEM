# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\signals.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time

import pytz
from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from . import utils as _utils  # email helpers & console-safe logging

# Leave-blocking helpers
from apps.tasks.services.blocking import guard_assign
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# If Celery fanout/digests are enabled, DO NOT send duplicate emails from signals.
ENABLE_CELERY_EMAIL = getattr(settings, "ENABLE_CELERY_EMAIL", False)

# Email policy toggles
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


def _on_commit(fn):
    """Safe on_commit: if no transaction context, call immediately."""
    try:
        transaction.on_commit(fn)
    except Exception:
        try:
            fn()
        except Exception:
            logger.exception("on_commit fallback failed")


# -----------------------------------------------------------------------------
# Scheduling policy
# -----------------------------------------------------------------------------
def _schedule_10am_email_for_checklist(obj: Checklist) -> None:
    """
    CHECKLISTS:
      Never send per-task checklist emails from signals.
      Consolidated checklist reminders are sent per-employee at 10:00 IST by:
        apps/tasks/tasks.py::send_due_today_assignments
    """
    logger.info(
        _utils._safe_console_text(
            f"[SIGNALS] Checklist per-task signal email suppressed for CL-{getattr(obj, 'id', '?')} (using consolidated 10:00 digest)."
        )
    )


def _schedule_10am_email_for_delegation(obj: Delegation) -> None:
    """
    Delegations:
      - Immediate "New Delegation Assigned" is sent on create (below).
      - Day-of 10:00 IST reminders are handled by Celery (send_due_today_assignments).
      - Therefore: NEVER send 10:00 reminder from signals (avoid duplicates).
    """
    return


def _send_delegation_assignment_immediate(obj: Delegation) -> None:
    """
    One-time "New Delegation Assigned" email, sent immediately after creation.

    🔒 Guards:
      - suppress self-assign (assigner == assignee)
      - suppress if assignee has no email
      - suppress if assignee is blocked NOW (leave at current instant in IST)
      - if ENABLE_CELERY_EMAIL is on, DO NOT send from signals at all
    """
    if ENABLE_CELERY_EMAIL or not SEND_EMAILS_FOR_AUTO_RECUR:
        return

    try:
        if obj.assign_by_id and obj.assign_by_id == obj.assign_to_id:
            logger.info(_utils._safe_console_text(f"[SIGNALS] Immediate delegation email suppressed for DL-{obj.id}: self-assigned."))
            return
    except Exception:
        pass

    to_email = (getattr(getattr(obj, "assign_to", None), "email", "") or "").strip()
    if not to_email:
        logger.info(_utils._safe_console_text(f"[SIGNALS] Immediate delegation email skipped for DL-{obj.id}: no assignee email."))
        return

    try:
        if is_user_blocked_at(obj.assign_to, timezone.now().astimezone(IST)):
            logger.info(_utils._safe_console_text(f"[SIGNALS] Immediate delegation email suppressed for DL-{obj.id}: assignee on leave now."))
            return
    except Exception:
        # fail-safe: if we cannot evaluate, do not send
        return

    def _send_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
        except Exception:
            complete_url = SITE_URL

        _utils.send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix=f"New Delegation Assigned – {obj.task_name}",
        )

    _on_commit(_send_now)


# -----------------------------------------------------------------------------
# Force planned datetime to 19:00 IST (NO shift on save)
# -----------------------------------------------------------------------------
@receiver(pre_save, sender=Checklist, dispatch_uid="tasks.checklist.presave.force19")
def force_checklist_planned_time(sender, instance: Checklist, **kwargs):
    """
    Checklist planned datetime MUST be 19:00 IST on the SAME date user chose.
    NO shifting off Sundays/holidays at entry time.
    """
    try:
        if not instance.planned_date:
            return
        dt = instance.planned_date
        tz = timezone.get_current_timezone()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, tz)
        dt_ist = dt.astimezone(IST)
        d = dt_ist.date()
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_checklist_planned_time failed: {e}"))


@receiver(pre_save, sender=Delegation, dispatch_uid="tasks.delegation.presave.force19")
def force_delegation_planned_time(sender, instance: Delegation, **kwargs):
    """
    Delegation planned datetime MUST be 19:00 IST on the SAME date user chose.
    NO shifting on save.
    """
    try:
        if not instance.planned_date:
            return
        dt = instance.planned_date
        tz = timezone.get_current_timezone()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, tz)
        dt_ist = dt.astimezone(IST)
        d = dt_ist.date()
        new_ist = IST.localize(datetime.combine(d, dt_time(19, 0, 0)))
        instance.planned_date = new_ist.astimezone(tz)
    except Exception as e:
        logger.error(_utils._safe_console_text(f"force_delegation_planned_time failed: {e}"))


# -----------------------------------------------------------------------------
# Recurrence: DISABLED in signals (moved to Celery generator in tasks.py)
# -----------------------------------------------------------------------------
@receiver(post_save, sender=Checklist, dispatch_uid="tasks.checklist.postsave.spawn_next")
def create_next_recurring_checklist(sender, instance: Checklist, created: bool, **kwargs):
    """
    IMPORTANT:
      Recurrence creation is handled centrally by:
        apps/tasks/tasks.py::_generate_recurring_checklists_sync (and run_pre10am_unblock_and_generate)
      Leaving a signal-based generator enabled causes duplicate spawns.

    This receiver is intentionally a NO-OP.
    """
    return


# -----------------------------------------------------------------------------
# Checklist creation: no per-task emails (digest handles at 10:00)
# -----------------------------------------------------------------------------
@receiver(post_save, sender=Checklist, dispatch_uid="tasks.checklist.postsave.schedule_create")
def schedule_checklist_email_on_create(sender, instance: Checklist, created: bool, **kwargs):
    if not created:
        return
    if bool(getattr(instance, "is_skipped_due_to_leave", False)):
        return
    if ENABLE_CELERY_EMAIL:
        return

    # keep as NO-OP (explicitly)
    try:
        _on_commit(lambda: _schedule_10am_email_for_checklist(instance))
    except Exception:
        _schedule_10am_email_for_checklist(instance)


# -----------------------------------------------------------------------------
# Delegation: on creation, immediate assignment email ONLY
# -----------------------------------------------------------------------------
@receiver(post_save, sender=Delegation, dispatch_uid="tasks.delegation.postsave.schedule_create")
def schedule_delegation_email_on_create(sender, instance: Delegation, created: bool, **kwargs):
    if not created:
        return
    if bool(getattr(instance, "is_skipped_due_to_leave", False)):
        return
    if ENABLE_CELERY_EMAIL:
        return

    # 1) Immediate "New Delegation Assigned" email (with leave guard)
    try:
        _on_commit(lambda: _send_delegation_assignment_immediate(instance))
    except Exception:
        _send_delegation_assignment_immediate(instance)

    # 2) Day-of 10:00 reminders are handled by Celery fan-out; never schedule here.
    # _schedule_10am_email_for_delegation(instance) intentionally NOT called.


# -----------------------------------------------------------------------------
# HelpTicket: immediate email on assignment (created) — leave-guarded
# -----------------------------------------------------------------------------
@receiver(post_save, sender=HelpTicket, dispatch_uid="tasks.helpticket.postsave.send_create")
def send_help_ticket_email_on_create(sender, instance: HelpTicket, created: bool, **kwargs):
    if not created:
        return
    if bool(getattr(instance, "is_skipped_due_to_leave", False)):
        return

    # If assignee is on leave NOW, or planned timestamp lies within leave window, suppress.
    try:
        assignee = getattr(instance, "assign_to", None)
        if assignee and is_user_blocked_at(assignee, timezone.now().astimezone(IST)):
            logger.info(_utils._safe_console_text(
                f"[SIGNALS] HelpTicket HT-{getattr(instance, 'id', '?')} email suppressed: assignee on leave now."
            ))
            return
        if assignee and getattr(instance, "planned_date", None):
            planned_dt = instance.planned_date
            if timezone.is_naive(planned_dt):
                planned_dt = timezone.make_aware(planned_dt, timezone.get_current_timezone())
            planned_ist = timezone.localtime(planned_dt, IST)
            if is_user_blocked_at(assignee, planned_ist):
                logger.info(_utils._safe_console_text(
                    f"[SIGNALS] HelpTicket HT-{getattr(instance, 'id', '?')} email suppressed: planned time within leave."
                ))
                return
    except Exception:
        # fail-safe: do not send if we cannot evaluate
        return

    if ENABLE_CELERY_EMAIL:
        # If you later move HelpTicket mailers to Celery, this prevents duplicates.
        return

    def _send_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:help_ticket_detail', args=[instance.id])}"
        except Exception:
            complete_url = SITE_URL

        _utils.send_help_ticket_assignment_to_user(
            ticket=instance,
            complete_url=complete_url,
            subject_prefix="Help Ticket Assigned",
        )

    _on_commit(_send_now)


# -----------------------------------------------------------------------------
# Logging only
# -----------------------------------------------------------------------------
@receiver(post_save, sender=Checklist, dispatch_uid="tasks.checklist.postsave.log_complete")
def log_checklist_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Checklist {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Delegation, dispatch_uid="tasks.delegation.postsave.log_complete")
def log_delegation_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Completed":
        logger.info(_utils._safe_console_text(
            f"Delegation {instance.id} '{instance.task_name}' completed by {instance.assign_to}"
        ))


@receiver(post_save, sender=HelpTicket, dispatch_uid="tasks.helpticket.postsave.log_close")
def log_helpticket_completion(sender, instance, created, **kwargs):
    if not created and instance.status == "Closed":
        logger.info(_utils._safe_console_text(
            f"Help Ticket {instance.id} '{instance.title}' closed by {instance.assign_to}"
        ))


@receiver(post_save, sender=Checklist, dispatch_uid="tasks.checklist.postsave.log_create")
def log_checklist_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created checklist {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))


@receiver(post_save, sender=Delegation, dispatch_uid="tasks.delegation.postsave.log_create")
def log_delegation_creation(sender, instance, created, **kwargs):
    if created:
        logger.debug(_utils._safe_console_text(
            f"Created delegation {instance.id} '{instance.task_name}' for {instance.assign_to} at {instance.planned_date}"
        ))
