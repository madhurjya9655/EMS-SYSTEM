from __future__ import annotations

import logging
from datetime import date
from typing import List, Dict, Any, Optional
from django.contrib.auth import get_user_model
from django.utils import timezone

from apps.leave.models import LeaveHandover, LeaveRequest

logger = logging.getLogger(__name__)
User = get_user_model()

def get_active_handovers_for_date(target_date: date) -> Dict[int, List[LeaveHandover]]:
    """
    Returns a dictionary mapping task IDs to their handover objects that are active on the specified date.
    This is used by task modules to determine current task assignees.
    
    Returns:
        Dict[int, List[LeaveHandover]]: Mapping of task_id to handover objects
    """
    # Get all leaves covering this date
    active_leaves = LeaveRequest.objects.active_for_blocking().covering_ist_date(target_date)
    
    # No active leaves, no handovers
    if not active_leaves.exists():
        return {}
    
    # Get all active handovers for these leaves
    handovers = LeaveHandover.objects.filter(
        leave_request__in=active_leaves,
        is_active=True
    ).select_related('leave_request', 'original_assignee', 'new_assignee')
    
    # Group by task type and ID for easy lookup
    result = {}
    for handover in handovers:
        key = (handover.task_type, handover.original_task_id)
        if key not in result:
            result[key] = []
        result[key].append(handover)
    
    return result

def get_current_assignee_for_task(task_type: str, task_id: int, original_assignee: User, current_date: Optional[date] = None) -> User:
    """
    Determines the current assignee for a task, taking into account any active handovers.
    
    Args:
        task_type: Type of task ('checklist', 'delegation', 'help_ticket')
        task_id: ID of the task
        original_assignee: Original user assigned to the task
        current_date: Date to check (defaults to today)
    
    Returns:
        User: Current assignee (either original or temporary)
    """
    if current_date is None:
        current_date = timezone.now().date()
    
    # Check if the original assignee is on leave and has handed over this task
    handovers = LeaveHandover.objects.filter(
        task_type=task_type,
        original_task_id=task_id,
        original_assignee=original_assignee,
        is_active=True,
        leave_request__status='APPROVED',
        leave_request__start_date__lte=current_date,
        leave_request__end_date__gte=current_date
    ).select_related('new_assignee')
    
    # Return the new assignee if there's an active handover
    if handovers.exists():
        return handovers.first().new_assignee
    
    # Otherwise return the original assignee
    return original_assignee

def deactivate_expired_handovers() -> int:
    """
    Deactivates handovers for leaves that have ended.
    This should be called by a daily cron job.
    
    Returns:
        int: Number of handovers deactivated
    """
    today = timezone.now().date()
    
    # Find handovers for leaves that have ended
    count = LeaveHandover.objects.filter(
        is_active=True,
        leave_request__end_date__lt=today
    ).update(is_active=False)
    
    return count# apps/leave/services/task_handover.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

from django.contrib.auth import get_user_model
from django.utils import timezone

from apps.leave.models import (
    LeaveHandover,
    LeaveRequest,
    LeaveStatus,
)

logger = logging.getLogger(__name__)
User = get_user_model()


@dataclass(frozen=True)
class HandoverKey:
    """Dictionary key for quick lookup: (task_type, original_task_id)."""
    task_type: str
    original_task_id: int


def _today() -> date:
    """Project-wide 'today' (date) helper."""
    return timezone.now().date()


def get_active_handovers_for_date(target_date: date) -> Dict[HandoverKey, List[LeaveHandover]]:
    """
    Return all active handovers on a given calendar date, grouped by (task_type, task_id).

    A handover is considered active when:
      - leave_request.status in {PENDING, APPROVED}
      - leave_request.start_date <= target_date <= leave_request.end_date
      - handover.is_active is True
      - (if set) effective_start_date <= target_date <= effective_end_date

    Args:
        target_date: The calendar date to evaluate activity.

    Returns:
        dict mapping (task_type, original_task_id) -> list[LeaveHandover]
    """
    # Leaves covering this date (and that block)
    leaves = (
        LeaveRequest.objects.active_for_blocking()
        .covering_ist_date(target_date)
        .only("id", "status", "start_date", "end_date")
    )

    if not leaves.exists():
        return {}

    qs = (
        LeaveHandover.objects.filter(
            is_active=True,
            leave_request__in=leaves,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
        )
        .select_related("leave_request", "original_assignee", "new_assignee")
        .only(
            "id", "task_type", "original_task_id", "is_active",
            "effective_start_date", "effective_end_date",
            "leave_request__id", "leave_request__status",
            "leave_request__start_date", "leave_request__end_date",
            "original_assignee__id", "new_assignee__id",
        )
    )

    result: Dict[HandoverKey, List[LeaveHandover]] = {}
    for ho in qs:
        # Respect effective start/end if set on the handover row
        if ho.effective_start_date and target_date < ho.effective_start_date:
            continue
        if ho.effective_end_date and target_date > ho.effective_end_date:
            continue

        key = HandoverKey(ho.task_type, int(ho.original_task_id))
        result.setdefault(key, []).append(ho)

    return result


def get_current_assignee_for_task(
    task_type: str,
    task_id: int,
    original_assignee: User,
    current_date: Optional[date] = None,
) -> User:
    """
    Compute who currently owns the task, accounting for an active handover.

    Args:
        task_type: 'checklist' | 'delegation' | 'help_ticket'
        task_id:   Primary key of the task
        original_assignee: The normal assignee on record
        current_date: Date for evaluation (defaults to today)

    Returns:
        User: The delegate if a matching active handover exists; otherwise the original_assignee.
    """
    d = current_date or _today()

    ho = (
        LeaveHandover.objects.filter(
            task_type=task_type,
            original_task_id=task_id,
            original_assignee=original_assignee,
            is_active=True,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
            leave_request__start_date__lte=d,
            leave_request__end_date__gte=d,
        )
        .select_related("new_assignee")
        .order_by("-id")
        .first()
    )

    # Also respect the handover's effective dates if present
    if ho:
        if (ho.effective_start_date and d < ho.effective_start_date) or (
            ho.effective_end_date and d > ho.effective_end_date
        ):
            return original_assignee
        return ho.new_assignee

    return original_assignee


def deactivate_expired_handovers() -> int:
    """
    Deactivate handovers whose leaves have ended before today.

    Returns:
        int: Number of handover rows updated (is_active=False).
    """
    today = _today()
    updated = (
        LeaveHandover.objects.filter(
            is_active=True,
            leave_request__end_date__lt=today,
        )
        .update(is_active=False)
    )
    return int(updated)


# ---------- Optional helpers used by views/templates (non-breaking) ----------

def mark_handover_badge(tasks: List[object], task_type: str, today: Optional[date] = None) -> None:
    """
    Mark each task instance with an attribute `is_handover` (bool) for UI badges.
    This is a convenience so templates can show a 'Handover' badge without
    re-querying. It mutates items in-place; safe for Django queryset materialized to list.

    Args:
        tasks:      List of task objects with `.id` and `.assign_to`
        task_type:  'checklist' | 'delegation' | 'help_ticket'
        today:      Date to consider (defaults to today)
    """
    if not tasks:
        return
    d = today or _today()
    ids = [getattr(t, "id", None) for t in tasks if getattr(t, "id", None) is not None]
    if not ids:
        return

    active_map: Dict[int, bool] = {i: False for i in ids}

    for ho in LeaveHandover.objects.filter(
        is_active=True,
        task_type=task_type,
        original_task_id__in=ids,
        leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
        leave_request__start_date__lte=d,
        leave_request__end_date__gte=d,
    ).only("original_task_id", "effective_start_date", "effective_end_date"):
        # check effective window
        if ho.effective_start_date and d < ho.effective_start_date:
            continue
        if ho.effective_end_date and d > ho.effective_end_date:
            continue
        active_map[int(ho.original_task_id)] = True

    for t in tasks:
        try:
            setattr(t, "is_handover", bool(active_map.get(int(getattr(t, "id")))))
        except Exception:
            setattr(t, "is_handover", False)
