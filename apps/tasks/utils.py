# apps/tasks/utils.py
import pytz
from datetime import datetime, timedelta, date as dt_date, time as dt_time
from django.utils import timezone
from apps.tasks.models import Checklist

def create_missing_recurring_checklist_tasks_for_user(user):
    """
    For each recurring checklist assigned to this user, create all missing due Checklist rows (up to today).
    Only one instance per (task_name, assign_to, mode, planned_date) per due date is ever created.
    """
    ist = pytz.timezone('Asia/Kolkata')
    today = timezone.localdate()
    all_templates = (
        Checklist.objects.filter(assign_to=user)
        .order_by('planned_date')
        .distinct('task_name', 'mode')
    )

    for template in all_templates:
        mode = template.mode
        freq = template.frequency or 1
        task_name = template.task_name
        base_dt = template.planned_date
        if isinstance(base_dt, datetime):
            base_date = base_dt.date()
            planned_time = base_dt.time()
        else:
            base_date = base_dt
            planned_time = dt_time(10, 0)

        gen_date = base_date
        # Generate each due date up to today
        while gen_date <= today:
            # Only create if not already present
            exists = Checklist.objects.filter(
                assign_to=user,
                task_name=task_name,
                mode=mode,
                planned_date__date=gen_date,
            ).exists()
            if not exists:
                planned_dt = datetime.combine(gen_date, planned_time)
                planned_dt = ist.localize(planned_dt)
                Checklist.objects.create(
                    assign_by=template.assign_by,
                    task_name=task_name,
                    assign_to=user,
                    planned_date=planned_dt,
                    priority=template.priority,
                    attachment_mandatory=template.attachment_mandatory,
                    mode=mode,
                    frequency=freq,
                    time_per_task_minutes=template.time_per_task_minutes,
                    remind_before_days=template.remind_before_days,
                    message=template.message,
                    assign_pc=template.assign_pc,
                    group_name=template.group_name,
                    notify_to=template.notify_to,
                    auditor=template.auditor,
                    set_reminder=template.set_reminder,
                    reminder_mode=template.reminder_mode,
                    reminder_frequency=template.reminder_frequency,
                    reminder_before_days=template.reminder_before_days,
                    reminder_starting_time=template.reminder_starting_time,
                    checklist_auto_close=template.checklist_auto_close,
                    checklist_auto_close_days=template.checklist_auto_close_days,
                    actual_duration_minutes=0,
                )
            # Calculate next due date based on mode
            if mode == 'Daily':
                gen_date += timedelta(days=freq)
            elif mode == 'Weekly':
                gen_date += timedelta(days=7 * freq)
            elif mode == 'Monthly':
                # Simple monthly increment, handles day overflow
                year = gen_date.year + ((gen_date.month + freq - 1) // 12)
                month = (gen_date.month + freq - 1) % 12 + 1
                day = min(gen_date.day, 28)  # To prevent invalid dates
                gen_date = dt_date(year, month, day)
            elif mode == 'Yearly':
                gen_date = dt_date(gen_date.year + freq, gen_date.month, gen_date.day)
            else:
                break  # Non-recurring
