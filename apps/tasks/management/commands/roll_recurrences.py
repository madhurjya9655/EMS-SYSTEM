from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta

from apps.tasks.models import Checklist
from apps.tasks.recurrence import get_next_planned_date

RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']


class Command(BaseCommand):
    help = (
        "Ensure each recurring series has exactly one future pending occurrence. "
        "Future recurrences are generated at 10:00 AM IST and skip Sundays/holidays."
    )

    def handle(self, *args, **opts):
        now = timezone.now()

        # Identify series by (assignee, task_name, mode, frequency, group_name)
        seeds = (
            Checklist.objects
            .filter(mode__in=RECURRING_MODES)
            .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
            .distinct()
        )

        created = 0
        for s in seeds:
            # Latest item in the series (any status)
            last = (
                Checklist.objects
                .filter(**s)
                .order_by('-planned_date', '-id')
                .first()
            )
            if not last:
                continue

            # If there is already a future pending item for this series, skip
            if Checklist.objects.filter(status='Pending', planned_date__gt=now, **s).exists():
                continue

            # Compute the next planned datetime.
            # NOTE: get_next_planned_date sets time to 10:00 AM IST and skips Sundays/holidays.
            next_planned = get_next_planned_date(last.planned_date, last.mode, last.frequency)
            if not next_planned:
                continue

            # De-dupe guard (±1 minute) for this series key
            dupe = Checklist.objects.filter(
                assign_to_id=s['assign_to_id'],
                task_name=s['task_name'],
                mode=s['mode'],
                frequency=s['frequency'],
                group_name=s['group_name'],
                planned_date__gte=next_planned - timedelta(minutes=1),
                planned_date__lt=next_planned + timedelta(minutes=1),
                status='Pending',
            ).exists()
            if dupe:
                continue

            Checklist.objects.create(
                assign_by=last.assign_by,
                task_name=last.task_name,
                message=last.message,
                assign_to=last.assign_to,
                planned_date=next_planned,
                priority=last.priority,
                attachment_mandatory=last.attachment_mandatory,
                mode=last.mode,
                frequency=last.frequency,
                time_per_task_minutes=last.time_per_task_minutes,
                remind_before_days=last.remind_before_days,
                assign_pc=last.assign_pc,
                notify_to=last.notify_to,
                set_reminder=last.set_reminder,
                reminder_mode=last.reminder_mode,
                reminder_frequency=last.reminder_frequency,
                reminder_starting_time=last.reminder_starting_time,
                checklist_auto_close=last.checklist_auto_close,
                checklist_auto_close_days=last.checklist_auto_close_days,
                group_name=getattr(last, 'group_name', None),
                actual_duration_minutes=0,
                status='Pending',
            )
            created += 1

        self.stdout.write(self.style.SUCCESS(f"Created {created} next occurrences"))
