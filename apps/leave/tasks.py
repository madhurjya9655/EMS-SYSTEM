# File: apps/leave/tasks.py
from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from celery import shared_task
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3, name="leave.send_delegation_reminders")
def send_delegation_reminders(self):
    """
    Send reminder emails for active delegation handovers that are due.
    """
    try:
        from .models import DelegationReminder, LeaveDecisionAudit, DecisionAction
        from .services.notifications import send_delegation_reminder_email

        reminders_to_send = (
            DelegationReminder.objects.filter(
                is_active=True,
                next_run_at__lte=timezone.now(),
            )
            .select_related(
                "leave_handover__leave_request",
                "leave_handover__new_assignee",
                "leave_handover__original_assignee",
            )
        )

        count = 0
        for reminder in reminders_to_send:
            try:
                if reminder.should_send_reminder():
                    with transaction.atomic():
                        send_delegation_reminder_email(reminder)
                        reminder.mark_sent()
                        count += 1

                        if LeaveDecisionAudit and DecisionAction:
                            LeaveDecisionAudit.log(
                                reminder.leave_handover.leave_request,
                                DecisionAction.REMINDER_EMAIL_SENT,
                                extra={
                                    "reminder_id": reminder.id,
                                    "assignee_id": reminder.leave_handover.new_assignee.id,
                                    "total_sent": reminder.total_sent,
                                },
                            )
                else:
                    # Deactivate if task is completed or leave ended
                    if not reminder.leave_handover.is_currently_active:
                        reminder.deactivate()
            except Exception as e:
                logger.error("Failed to process reminder #%s: %s", reminder.id, e)

        logger.info("Sent %s delegation reminder email(s).", count)
        return f"Sent {count} delegation reminders"

    except Exception as exc:
        logger.error("Error in send_delegation_reminders task: %s", exc)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=2, name="leave.cleanup_expired_handovers")
def cleanup_expired_handovers(self):
    """
    Deactivate handovers for leaves that have ended.
    """
    try:
        from .services.task_handover import deactivate_expired_handovers

        count = deactivate_expired_handovers()
        logger.info("Deactivated %s expired handover(s).", count)
        return f"Deactivated {count} expired handovers"

    except Exception as exc:
        logger.error("Error in cleanup_expired_handovers task: %s", exc)
        raise self.retry(exc=exc, countdown=300)


@shared_task(bind=True, max_retries=3, name="leave.send_leave_emails_async")
def send_leave_emails_async(self, leave_id: int):
    """
    Send leave request emails asynchronously.
    Respects employee-selected CCs and admin default CCs (multi) + legacy single CC.
    """
    try:
        from .models import LeaveRequest, LeaveDecisionAudit, DecisionAction
        from .services.notifications import send_leave_request_email

        leave = (
            LeaveRequest.objects.select_related("employee", "reporting_person", "cc_person", "leave_type")
            .prefetch_related("cc_users")
            .get(id=leave_id)
        )

        # CC from M2M + admin-managed default CCs + legacy single CC snapshot
        # 1) employee-selected CCs
        user_cc_emails = [u.email for u in leave.cc_users.all() if getattr(u, "email", None)]

        # 2) admin-managed defaults (multi) via resolver
        admin_multi_cc: List[str] = []
        try:
            _rp, cc_users = LeaveRequest.resolve_routing_multi_for(leave.employee)
            admin_multi_cc = [u.email for u in cc_users if getattr(u, "email", None)]
        except Exception:
            admin_multi_cc = []

        # 3) legacy single snapshot on the row
        legacy_cc = [leave.cc_person.email] if getattr(leave.cc_person, "email", None) else []

        # merge & dedupe (case-insensitive)
        seen = set()
        all_cc: List[str] = []
        for e in (admin_multi_cc + legacy_cc + user_cc_emails):
            low = (e or "").strip().lower()
            if low and low not in seen:
                seen.add(low)
                all_cc.append(e)

        manager_email = leave.reporting_person.email if getattr(leave.reporting_person, "email", None) else None

        send_leave_request_email(leave, manager_email=manager_email, cc_list=all_cc)

        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT)

        logger.info("Sent leave request email for leave #%s", leave_id)
        return f"Sent leave request email for leave {leave_id}"

    except Exception as exc:
        logger.error("Error sending leave emails for #%s: %s", leave_id, exc)
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=3, name="leave.send_handover_emails_async")
def send_handover_emails_async(self, leave_id: int, handover_ids: Optional[Iterable[int]] = None):
    """
    Send handover emails asynchronously.

    Groups handovers by assignee and sends one email per assignee.
    Also ensures default 2-day reminders exist for each handover.
    """
    try:
        from datetime import timedelta

        from .models import (
            LeaveRequest,
            LeaveHandover,
            LeaveDecisionAudit,
            DecisionAction,
            DelegationReminder,
        )
        from .services.notifications import send_handover_email

        leave = LeaveRequest.objects.get(id=leave_id)

        qs = LeaveHandover.objects.filter(leave_request=leave).select_related(
            "new_assignee", "original_assignee"
        )
        if handover_ids:
            qs = qs.filter(id__in=list(handover_ids))

        # Group by assignee (skip rows without a valid assignee)
        by_assignee: dict[int, List[LeaveHandover]] = {}
        for ho in qs:
            if not getattr(ho, "new_assignee_id", None):
                continue
            by_assignee.setdefault(ho.new_assignee_id, []).append(ho)

        if not by_assignee:
            logger.info("No handover emails to send for leave #%s", leave_id)
            return "No handover emails to send"

        sent_to = 0
        for assignee_id, items in by_assignee.items():
            try:
                assignee = items[0].new_assignee
                send_handover_email(leave, assignee, items)
                sent_to += 1

                if LeaveDecisionAudit and DecisionAction:
                    LeaveDecisionAudit.log(
                        leave,
                        DecisionAction.HANDOVER_EMAIL_SENT,
                        extra={"assignee_id": assignee_id},
                    )

                # Ensure reminders exist (default every 2 days)
                for ho in items:
                    try:
                        DelegationReminder.objects.get_or_create(
                            leave_handover=ho,
                            defaults={
                                "interval_days": 2,
                                "next_run_at": timezone.now() + timedelta(days=2),
                                "is_active": True,
                            },
                        )
                    except Exception as e:
                        logger.error("Failed to create reminder for handover #%s: %s", ho.id, e)
            except Exception as e:
                logger.error("Failed to send handover email to assignee #%s: %s", assignee_id, e)

        logger.info("Sent handover emails to %s assignee(s) for leave #%s", sent_to, leave_id)
        return f"Sent handover emails to {sent_to} assignees"

    except Exception as exc:
        logger.error("Error sending handover emails for leave #%s: %s", leave_id, exc)
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=3, name="leave.send_leave_decision_email_async")
def send_leave_decision_email_async(self, leave_id: int):
    """
    Send leave decision email (approve/reject) asynchronously.
    """
    try:
        from .models import LeaveRequest
        from .services.notifications import send_leave_decision_email

        leave = LeaveRequest.objects.select_related("employee", "approver", "leave_type").get(id=leave_id)
        send_leave_decision_email(leave)

        logger.info("Sent leave decision email for leave #%s", leave_id)
        return f"Sent leave decision email for leave {leave_id}"

    except Exception as exc:
        logger.error("Error sending leave decision email for #%s: %s", leave_id, exc)
        raise self.retry(exc=exc, countdown=60)
