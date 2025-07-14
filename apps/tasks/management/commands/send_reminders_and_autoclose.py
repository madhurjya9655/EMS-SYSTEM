from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from datetime import timedelta, datetime
from apps.tasks.models import Checklist
from django.core.mail import send_mail

def should_send_reminder(task, now):
    if not task.set_reminder:
        return False
    if task.status != 'Pending':
        return False
    planned_dt = task.planned_date
    reminder_start = planned_dt - timedelta(days=task.reminder_before_days)
    now_local = timezone.localtime(now)
    reminder_time = task.reminder_starting_time
    # Only trigger if current time >= reminder_start + starting_time and < planned_dt
    if now_local.date() < reminder_start.date() or now_local > planned_dt:
        return False
    if reminder_time:
        if now_local.hour != reminder_time.hour or now_local.minute != reminder_time.minute:
            return False
    # Frequency and mode
    mode = task.reminder_mode or 'Daily'
    freq = task.reminder_frequency or 1
    total_days = (now_local.date() - reminder_start.date()).days
    if mode == 'Daily':
        return total_days % freq == 0
    if mode == 'Weekly':
        total_weeks = total_days // 7
        return now_local.weekday() == reminder_start.weekday() and total_weeks % freq == 0
    if mode == 'Monthly':
        total_months = (now_local.year - reminder_start.year) * 12 + now_local.month - reminder_start.month
        return now_local.day == reminder_start.day and total_months % freq == 0
    if mode == 'Yearly':
        total_years = now_local.year - reminder_start.year
        return now_local.month == reminder_start.month and now_local.day == reminder_start.day and total_years % freq == 0
    return False

def send_reminder_email(task):
    subject = f"Checklist Reminder: {task.task_name}"
    msg = f"""Hi {task.assign_to.get_full_name() or task.assign_to.username},

This is a reminder for your checklist task:
Task: {task.task_name}
Message: {task.message}
Planned Date: {task.planned_date.strftime('%Y-%m-%d %H:%M')}

Please complete your task on time.
"""
    send_mail(
        subject,
        msg,
        settings.DEFAULT_FROM_EMAIL,
        [task.assign_to.email],
        fail_silently=True,
    )

def should_autoclose(task, now):
    if not task.checklist_auto_close or task.status != 'Pending':
        return False
    planned_date = task.planned_date
    auto_days = task.checklist_auto_close_days
    if auto_days < 1:
        return False
    deadline = planned_date + timedelta(days=auto_days)
    return now >= deadline

class Command(BaseCommand):
    help = 'Send checklist reminders and auto-close overdue checklists.'

    def handle(self, *args, **kwargs):
        now = timezone.now()
        qs = Checklist.objects.filter(status='Pending')
        for task in qs:
            # REMINDER
            if should_send_reminder(task, now):
                send_reminder_email(task)
                self.stdout.write(self.style.SUCCESS(
                    f"Sent reminder to {task.assign_to.email} for task {task.task_name} ({task.id})"
                ))
            # AUTO CLOSE
            if should_autoclose(task, now):
                task.status = 'Completed'
                task.completed_at = now
                task.save()
                self.stdout.write(self.style.WARNING(
                    f"Auto-closed checklist {task.task_name} ({task.id})"
                ))
