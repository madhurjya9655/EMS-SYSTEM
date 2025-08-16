from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
from datetime import timedelta
import logging

from apps.tasks.models import Checklist, Delegation, HelpTicket

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Archive or clean up old completed tasks to improve dashboard performance'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days-old',
            type=int,
            default=30,
            help='Archive tasks completed more than X days ago (default: 30)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be archived without actually doing it',
        )
        parser.add_argument(
            '--delete',
            action='store_true',
            help='Delete instead of archive (DANGEROUS - use with caution)',
        )

    def handle(self, *args, **options):
        days_old = options['days_old']
        dry_run = options['dry_run']
        delete_mode = options['delete']
        
        cutoff_date = timezone.now() - timedelta(days=days_old)
        
        self.stdout.write(f"Processing tasks completed before {cutoff_date}")
        
        # Checklist tasks
        completed_checklists = Checklist.objects.filter(
            status='Completed',
            completed_at__lt=cutoff_date
        )
        
        # Delegation tasks
        completed_delegations = Delegation.objects.filter(
            status='Completed',
            completed_at__lt=cutoff_date
        )
        
        # Help tickets
        closed_tickets = HelpTicket.objects.filter(
            status='Closed',
            resolved_at__lt=cutoff_date
        )
        
        chk_count = completed_checklists.count()
        del_count = completed_delegations.count()
        ticket_count = closed_tickets.count()
        total_count = chk_count + del_count + ticket_count
        
        self.stdout.write(f"Found {total_count} old completed tasks:")
        self.stdout.write(f"  - Checklists: {chk_count}")
        self.stdout.write(f"  - Delegations: {del_count}")
        self.stdout.write(f"  - Help Tickets: {ticket_count}")
        
        if total_count == 0:
            self.stdout.write(self.style.SUCCESS("No old completed tasks to process"))
            return
            
        if dry_run:
            action = "DELETE" if delete_mode else "ARCHIVE"
            self.stdout.write(
                self.style.WARNING(f"[DRY RUN] Would {action} {total_count} completed tasks")
            )
            return
            
        if delete_mode:
            confirm = input(f"Are you sure you want to DELETE {total_count} completed tasks? Type 'yes' to confirm: ")
            if confirm.lower() != 'yes':
                self.stdout.write("Aborted")
                return
                
            with transaction.atomic():
                completed_checklists.delete()
                completed_delegations.delete()
                closed_tickets.delete()
                
            self.stdout.write(
                self.style.SUCCESS(f"Deleted {total_count} old completed tasks")
            )
        else:
            # Archive mode - add an 'archived' field or move to different table
            # For now, we'll just add a flag (you may need to add this field to models)
            try:
                with transaction.atomic():
                    completed_checklists.update(archived=True)
                    completed_delegations.update(archived=True) 
                    closed_tickets.update(archived=True)
                    
                self.stdout.write(
                    self.style.SUCCESS(f"Archived {total_count} old completed tasks")
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"Archive failed (archived field may not exist): {e}")
                )
                self.stdout.write("Consider adding 'archived = models.BooleanField(default=False)' to your models")
                
        return f"Processed {total_count} old completed tasks"