from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Send delegation reminders for active handovers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be sent without actually sending emails',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Maximum number of reminders to process (default: 50)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        limit = options['limit']
        
        now = timezone.now()
        
        # Get reminders that should be sent
        try:
            from apps.leave.models import DelegationReminder, DecisionAction, LeaveDecisionAudit
            from apps.leave.services.notifications import send_delegation_reminder_email
            
            reminders_to_send = DelegationReminder.objects.filter(
                is_active=True,
                next_run_at__lte=now
            ).select_related(
                'leave_handover',
                'leave_handover__leave_request',
                'leave_handover__new_assignee',
                'leave_handover__original_assignee'
            )[:limit]
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error loading reminders: {e}'))
            return
        
        if not reminders_to_send.exists():
            self.stdout.write(self.style.SUCCESS('No reminders to send'))
            return
        
        sent_count = 0
        deactivated_count = 0
        error_count = 0
        
        for reminder in reminders_to_send:
            try:
                # Double-check if reminder should still be sent
                if not reminder.should_send_reminder():
                    reminder.deactivate()
                    deactivated_count += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f'Deactivated reminder {reminder.id} (task completed or leave ended)'
                        )
                    )
                    continue
                
                if dry_run:
                    self.stdout.write(
                        f'Would send reminder to {reminder.leave_handover.new_assignee.email} '
                        f'for task: {reminder.leave_handover.get_task_title()}'
                    )
                    sent_count += 1
                else:
                    with transaction.atomic():
                        # Send the reminder email
                        send_delegation_reminder_email(reminder)
                        
                        # Mark as sent and schedule next
                        reminder.mark_sent()
                        
                        # Log the reminder
                        LeaveDecisionAudit.log(
                            reminder.leave_handover.leave_request,
                            DecisionAction.REMINDER_EMAIL_SENT,
                            extra={
                                'reminder_id': reminder.id,
                                'assignee_id': reminder.leave_handover.new_assignee.id,
                                'total_sent': reminder.total_sent,
                            }
                        )
                        
                        sent_count += 1
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'Sent reminder {reminder.id} to {reminder.leave_handover.new_assignee.email} '
                                f'(#{reminder.total_sent} total)'
                            )
                        )
                        
            except Exception as e:
                error_count += 1
                self.stdout.write(
                    self.style.ERROR(
                        f'Failed to send reminder {reminder.id}: {str(e)}'
                    )
                )
                logger.exception(f'Failed to send delegation reminder {reminder.id}')
        
        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Processed {sent_count + deactivated_count + error_count} reminders:'))
        self.stdout.write(f'  Sent: {sent_count}')
        self.stdout.write(f'  Deactivated: {deactivated_count}')
        self.stdout.write(f'  Errors: {error_count}')
        
        if dry_run:
            self.stdout.write(self.style.WARNING('This was a dry run - no emails were actually sent'))