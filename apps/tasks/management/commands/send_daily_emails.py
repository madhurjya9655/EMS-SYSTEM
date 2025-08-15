# apps/tasks/management/commands/send_daily_emails.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.urls import reverse
from django.conf import settings
from datetime import timedelta
import pytz

from apps.tasks.models import Checklist
from apps.tasks.utils import send_checklist_assignment_to_user

IST = pytz.timezone('Asia/Kolkata')


class Command(BaseCommand):
    help = 'Send daily reminder emails for tasks scheduled at 10:00 AM IST'

    def handle(self, *args, **options):
        # Get current time in IST
        now_ist = timezone.now().astimezone(IST)
        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
        
        # Check if it's around 10:00 AM IST (within 5 minutes)
        if not (9, 55) <= (now_ist.hour, now_ist.minute) <= (10, 5):
            self.stdout.write(
                self.style.WARNING(
                    f'Current time {now_ist.strftime("%H:%M")} IST is not within email sending window (09:55-10:05)'
                )
            )
            return

        # Get today's date in IST
        today_ist = now_ist.date()
        
        # Find all pending checklist tasks scheduled for today at 10:00 AM IST
        start_time = IST.localize(timezone.datetime.combine(today_ist, timezone.time(10, 0)))
        end_time = start_time + timedelta(minutes=1)
        
        # Convert to project timezone for query
        project_tz = timezone.get_current_timezone()
        start_project = start_time.astimezone(project_tz)
        end_project = end_time.astimezone(project_tz)
        
        tasks = Checklist.objects.filter(
            status='Pending',
            planned_date__gte=start_project,
            planned_date__lt=end_project,
            mode__in=['Daily', 'Weekly', 'Monthly', 'Yearly']  # Only recurring tasks
        )
        
        sent_count = 0
        for task in tasks:
            if task.assign_to and task.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[task.id])}"
                try:
                    send_checklist_assignment_to_user(
                        task=task,
                        complete_url=complete_url,
                        subject_prefix="Daily Reminder - Recurring Checklist",
                    )
                    sent_count += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Sent reminder for task "{task.task_name}" to {task.assign_to.email}'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(
                            f'Failed to send reminder for task "{task.task_name}": {str(e)}'
                        )
                    )
        
        self.stdout.write(
            self.style.SUCCESS(f'Daily email reminders sent: {sent_count} emails')
        )