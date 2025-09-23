# apps/tasks/leave_integration.py
"""
Task integration with leave handover system.
This file should be imported in apps/tasks/apps.py to register signal handlers.
"""

import logging
from django.dispatch import receiver
from django.contrib.auth import get_user_model
from django.utils import timezone

logger = logging.getLogger(__name__)
User = get_user_model()

# Import signals from leave app
try:
    from apps.leave.signals import handover_activated, handover_deactivated
except ImportError:
    logger.warning("Leave handover signals not available")
    handover_activated = None
    handover_deactivated = None


@receiver(handover_activated)
def handle_handover_activated(sender, handover_id, original_assignee_id, new_assignee_id, **kwargs):
    """
    Handle task reassignment when a handover is activated.
    This temporarily reassigns tasks to the new assignee during leave.
    """
    try:
        # Get the handover record to understand which task to reassign
        from apps.leave.models import LeaveHandover
        handover = LeaveHandover.objects.get(id=handover_id)
        
        # Get users
        original_user = User.objects.get(id=original_assignee_id)
        new_user = User.objects.get(id=new_assignee_id)
        
        logger.info(f"Activating handover: {handover.task_type} task #{handover.original_task_id} from {original_user} to {new_user}")
        
        # Handle different task types
        if handover.task_type == 'checklist':
            _reassign_checklist_task(handover.original_task_id, new_user)
        elif handover.task_type == 'delegation':
            _reassign_delegation_task(handover.original_task_id, new_user)
        elif handover.task_type == 'help_ticket':
            _reassign_help_ticket(handover.original_task_id, new_user)
        else:
            logger.warning(f"Unknown task type for handover: {handover.task_type}")
            
    except Exception as e:
        logger.exception(f"Error handling handover activation for handover #{handover_id}: {e}")


@receiver(handover_deactivated)
def handle_handover_deactivated(sender, handover_id, original_assignee_id, new_assignee_id, **kwargs):
    """
    Handle task reassignment when a handover is deactivated.
    This reassigns tasks back to the original assignee when leave ends.
    """
    try:
        # Get the handover record
        from apps.leave.models import LeaveHandover
        handover = LeaveHandover.objects.get(id=handover_id)
        
        # Get users
        original_user = User.objects.get(id=original_assignee_id)
        new_user = User.objects.get(id=new_assignee_id)
        
        logger.info(f"Deactivating handover: {handover.task_type} task #{handover.original_task_id} from {new_user} back to {original_user}")
        
        # Handle different task types - reassign back to original user
        if handover.task_type == 'checklist':
            _reassign_checklist_task(handover.original_task_id, original_user)
        elif handover.task_type == 'delegation':
            _reassign_delegation_task(handover.original_task_id, original_user)
        elif handover.task_type == 'help_ticket':
            _reassign_help_ticket(handover.original_task_id, original_user)
        else:
            logger.warning(f"Unknown task type for handover: {handover.task_type}")
            
    except Exception as e:
        logger.exception(f"Error handling handover deactivation for handover #{handover_id}: {e}")


def _reassign_checklist_task(task_id, new_assignee):
    """Reassign a checklist task to a new user"""
    try:
        from apps.tasks.models import Checklist
        task = Checklist.objects.get(id=task_id)
        old_assignee = task.assign_to
        task.assign_to = new_assignee
        task.save(update_fields=['assign_to'])
        logger.info(f"Checklist task #{task_id} '{task.task_name}' reassigned from {old_assignee} to {new_assignee}")
    except Exception as e:
        logger.error(f"Failed to reassign checklist task #{task_id}: {e}")


def _reassign_delegation_task(task_id, new_assignee):
    """Reassign a delegation task to a new user"""
    try:
        from apps.tasks.models import Delegation
        task = Delegation.objects.get(id=task_id)
        old_assignee = task.assign_to
        task.assign_to = new_assignee
        task.save(update_fields=['assign_to'])
        logger.info(f"Delegation task #{task_id} '{task.task_name}' reassigned from {old_assignee} to {new_assignee}")
    except Exception as e:
        logger.error(f"Failed to reassign delegation task #{task_id}: {e}")


def _reassign_help_ticket(task_id, new_assignee):
    """Reassign a help ticket to a new user"""
    try:
        from apps.tasks.models import HelpTicket
        task = HelpTicket.objects.get(id=task_id)
        old_assignee = task.assign_to
        task.assign_to = new_assignee
        task.save(update_fields=['assign_to'])
        logger.info(f"Help ticket #{task_id} '{task.title}' reassigned from {old_assignee} to {new_assignee}")
    except Exception as e:
        logger.error(f"Failed to reassign help ticket #{task_id}: {e}")


def get_user_tasks_for_handover(user):
    """
    Get all active tasks for a user that can be handed over.
    Returns a list of (task_id, task_name) tuples where task_id is in format "type:id"
    """
    tasks = []
    
    try:
        # Checklist tasks
        from apps.tasks.models import Checklist
        checklist_items = Checklist.objects.filter(
            assign_to=user, 
            status='Pending'
        ).values_list('id', 'task_name')
        tasks.extend([
            (f"checklist:{item[0]}", f"Checklist: {item[1]}") 
            for item in checklist_items
        ])
    except ImportError:
        pass
        
    try:
        # Delegation tasks
        from apps.tasks.models import Delegation
        delegated_tasks = Delegation.objects.filter(
            assign_to=user,
            status='Pending'
        ).values_list('id', 'task_name')
        tasks.extend([
            (f"delegation:{item[0]}", f"Delegation: {item[1]}")
            for item in delegated_tasks
        ])
    except ImportError:
        pass
        
    try:
        # Help tickets
        from apps.tasks.models import HelpTicket
        help_tickets = HelpTicket.objects.filter(
            assign_to=user,
            status__in=['Open', 'In Progress']
        ).values_list('id', 'title')
        tasks.extend([
            (f"help_ticket:{item[0]}", f"Help Ticket: {item[1]}")
            for item in help_tickets
        ])
    except ImportError:
        pass
        
    return tasks