from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta

from apps.tasks.models import Checklist
from apps.tasks.recurrence import get_next_planned_date


class Command(BaseCommand):
    help = (
        "Ensure one future 'Pending' checklist per recurring series exists. "
        "Future recurrences are generated at 10:00 AM IST and skip Sundays/holidays."
    )

    def handle(self, *args, **kwargs):
        now = timezone.now()

        # Identify distinct recurring series (include group_name to avoid collisions)
        groups = (
            Checklist.objects.filter(
                mode__in=['Daily', 'Weekly', 'Monthly', 'Yearly'],
                frequency__gte=1,
            )
            .values('assign_to', 'task_name', 'mode', 'frequency', 'group_name')
            .distinct()
        )

        created = 0
        for g in groups:
            # Latest item in the series (any status), by most recent planned_date
            instance = (
                Checklist.objects.filter(
                    assign_to=g['assign_to'],
                    task_name=g['task_name'],
                    mode=g['mode'],
                    frequency=g['frequency'],
                    group_name=g['group_name'],
                )
                .order_by('-planned_date', '-id')
                .first()
            )
            if not instance:
                continue

            # If there is already a future pending item for this series, skip
            qs_future = Checklist.objects.filter(
                assign_to=instance.assign_to,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gt=instance.planned_date,
                status='Pending',
            )
            if qs_future.exists():
                continue

            # If the latest item is already in the future and pending, skip
            if instance.status == 'Pending' and instance.planned_date > now:
                continue

            # Compute next planned datetime.
            # NOTE: get_next_planned_date now forces 10:00 IST and skips Sun/holidays.
            next_planned = get_next_planned_date(
                instance.planned_date, instance.mode, instance.frequency
            )

            # If next is not in the future, keep stepping until it is (catch up).
            if not next_planned or next_planned <= now:
                tmp = next_planned or instance.planned_date
                safety = 0
                # Cap at ~2 years of steps to avoid runaway loops on corrupt data.
                while tmp and tmp <= now and safety < 730:
                    tmp = get_next_planned_date(tmp, instance.mode, instance.frequency)
                    safety += 1
                next_planned = tmp

            if not next_planned:
                continue

            # Dupe guard (±1 minute)
            dupe = Checklist.objects.filter(
                assign_to=instance.assign_to,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gte=next_planned - timedelta(minutes=1),
                planned_date__lt=next_planned + timedelta(minutes=1),
                status='Pending',
            ).exists()
            if dupe:
                continue

            Checklist.objects.create(
                assign_by=instance.assign_by,
                task_name=instance.task_name,
                assign_to=instance.assign_to,
                planned_date=next_planned,
                priority=instance.priority,
                attachment_mandatory=instance.attachment_mandatory,
                mode=instance.mode,
                frequency=instance.frequency,
                time_per_task_minutes=instance.time_per_task_minutes,
                remind_before_days=instance.remind_before_days,
                message=instance.message,
                assign_pc=instance.assign_pc,
                group_name=getattr(instance, 'group_name', None),
                notify_to=instance.notify_to,
                auditor=getattr(instance, 'auditor', None),
                set_reminder=instance.set_reminder,
                reminder_mode=instance.reminder_mode,
                reminder_frequency=instance.reminder_frequency,
                reminder_before_days=getattr(instance, 'reminder_before_days', None),
                reminder_starting_time=instance.reminder_starting_time,
                checklist_auto_close=instance.checklist_auto_close,
                checklist_auto_close_days=instance.checklist_auto_close_days,
                actual_duration_minutes=0,
                status='Pending',
            )
            created += 1
            self.stdout.write(self.style.SUCCESS(
                f"Created next instance: {instance.task_name} for {instance.assign_to} at {next_planned}"
            ))

        if created == 0:
            self.stdout.write("No missed recurrences to create.")
