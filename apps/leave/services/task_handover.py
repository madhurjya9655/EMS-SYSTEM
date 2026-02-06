#apps/leave/services/task_handover.py
from __future__ import annotations

import logging
from typing import Optional
from django.utils import timezone

from apps.leave.models import LeaveHandover, LeaveStatus, DelegationReminder

# Re-export actual implementations to satisfy legacy imports
from .handover import apply_handover_for_leave, send_handover_email  # noqa: F401

logger = logging.getLogger(__name__)


def deactivate_expired_handovers(now_date: Optional[timezone.datetime] = None) -> int:
    """
    Deactivate all expired LeaveHandover rows and stop their DelegationReminder rows.
    A handover is considered expired when:
      • effective_end_date is in the past (IST date), or
      • the linked leave is not in PENDING/APPROVED.
    Returns the number of handovers deactivated.
    """
    try:
        # Use queryset helper if available; otherwise compute here.
        if hasattr(LeaveHandover.objects, "deactivate_expired"):
            count = LeaveHandover.objects.deactivate_expired()
            logger.info("Expired handovers deactivated via QS helper: %s", count)
            return int(count)

        today = (now_date.date() if now_date else timezone.now().date())

        qs = LeaveHandover.objects.filter(is_active=True).exclude(
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED]
        ) | LeaveHandover.objects.filter(is_active=True, effective_end_date__lt=today)

        ids = list(qs.values_list("id", flat=True))
        if not ids:
            return 0

        updated = LeaveHandover.objects.filter(id__in=ids, is_active=True).update(is_active=False)
        DelegationReminder.objects.filter(leave_handover_id__in=ids, is_active=True).update(is_active=False)

        logger.info("Expired handovers deactivated: %s", updated)
        return int(updated)
    except Exception:
        logger.exception("Failed during deactivate_expired_handovers()")
        return 0


__all__ = [
    "apply_handover_for_leave",
    "send_handover_email",
    "deactivate_expired_handovers",
]
