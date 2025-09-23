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
    
    return count