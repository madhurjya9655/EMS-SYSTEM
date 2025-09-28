# apps/leave/tasks.py
from __future__ import annotations

import logging

from celery import shared_task
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def send_delegation_reminders(self):
    """Send reminder emails for active delegation handovers."""
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
            if reminder.should_send_reminder():
                try:
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
                except Exception as e:
                    logger.error(f"Failed to send reminder for {reminder.id}: {e}")
                    continue
            else:
                # Deactivate if task is completed or leave ended
                if not reminder.leave_handover.is_currently_active:
                    reminder.deactivate()

        logger.info(f"Sent {count} delegation reminder emails")
        return f"Sent {count} delegation reminders"

    except Exception as exc:
        logger.error(f"Error in send_delegation_reminders task: {exc}")
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=2)
def cleanup_expired_handovers(self):
    """Deactivate handovers for leaves that have ended."""
    try:
        from .services.task_handover import deactivate_expired_handovers

        count = deactivate_expired_handovers()
        logger.info(f"Deactivated {count} expired handovers")
        return f"Deactivated {count} expired handovers"

    except Exception as exc:
        logger.error(f"Error in cleanup_expired_handovers task: {exc}")
        raise self.retry(exc=exc, countdown=300)


@shared_task(bind=True, max_retries=3)
def send_leave_emails_async(self, leave_id: int):
    """Send leave request emails asynchronously."""
    try:
        from .models import LeaveRequest, LeaveDecisionAudit, DecisionAction
        from .services.notifications import send_leave_request_email

        leave = (
            LeaveRequest.objects.select_related("employee", "reporting_person", "cc_person")
            .prefetch_related("cc_users")
            .get(id=leave_id)
        )

        # CC from M2M + admin CC
        cc_emails = [u.email for u in leave.cc_users.all() if u.email]
        manager_email = leave.reporting_person.email if (leave.reporting_person and leave.reporting_person.email) else None
        admin_cc_list = [leave.cc_person.email] if (leave.cc_person and leave.cc_person.email) else []
        all_cc = list(set(admin_cc_list + cc_emails))

        send_leave_request_email(leave, manager_email=manager_email, cc_list=all_cc)

        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT)

        logger.info(f"Sent leave request email for leave {leave_id}")
        return f"Sent leave request email for leave {leave_id}"

    except Exception as exc:
        logger.error(f"Error sending leave emails for {leave_id}: {exc}")
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True, max_retries=3)
def send_handover_emails_async(self, leave_id: int, handover_ids: list[int]):
    """Send handover emails asynchronously."""
    try:
        from datetime import timedelta

        from .models import LeaveRequest, LeaveHandover, LeaveDecisionAudit, DecisionAction, DelegationReminder
        from .services.notifications import send_handover_email

        leave = LeaveRequest.objects.get(id=leave_id)
        handovers = LeaveHandover.objects.filter(id__in=handover_ids).select_related(
            "new_assignee", "original_assignee"
        )

        # Group by assignee
        assignee_map: dict[int, list[LeaveHandover]] = {}
        for ho in handovers:
            aid = ho.new_assignee.id
            assignee_map.setdefault(aid, []).append(ho)

        count = 0
        for assignee_id, user_handovers in assignee_map.items():
            try:
                assignee = user_handovers[0].new_assignee
                send_handover_email(leave, assignee, user_handovers)
                count += 1

                if LeaveDecisionAudit and DecisionAction:
                    LeaveDecisionAudit.log(
                        leave,
                        DecisionAction.HANDOVER_EMAIL_SENT,
                        extra={"assignee_id": assignee_id},
                    )

                # Ensure reminders exist (default every 2 days)
                for ho in user_handovers:
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
                        logger.error(f"Failed to create reminder for handover {ho.id}: {e}")
            except Exception as e:
                logger.error(f"Failed to send handover email to assignee {assignee_id}: {e}")
                continue

        logger.info(f"Sent handover emails to {count} assignees for leave {leave_id}")
        return f"Sent handover emails to {count} assignees"

    except Exception as exc:
        logger.error(f"Error sending handover emails for {leave_id}: {exc}")
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


@shared_task(bind=True)
def send_leave_decision_email_async(self, leave_id: int):
    """Send leave decision email asynchronously."""
    try:
        from .models import LeaveRequest
        from .services.notifications import send_leave_decision_email

        leave = LeaveRequest.objects.select_related("employee", "approver").get(id=leave_id)
        send_leave_decision_email(leave)

        logger.info(f"Sent leave decision email for leave {leave_id}")
        return f"Sent leave decision email for leave {leave_id}"

    except Exception as exc:
        logger.error(f"Error sending leave decision email for {leave_id}: {exc}")
        raise self.retry(exc=exc, countdown=60)
