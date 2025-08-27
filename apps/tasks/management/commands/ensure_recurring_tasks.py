from datetime import timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

from apps.tasks.models import Checklist
from apps.tasks.recurrence import compute_next_planned_datetime, RECURRING_MODES


class Command(BaseCommand):
    help = "Ensure each recurring checklist series has exactly one future pending occurrence"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--user-id", type=int)

    def handle(self, *args, **options):
        dry_run = options.get("dry_run", False)
        user_id = options.get("user_id")
        now = timezone.now()
        created_count = 0

        # Filter seed tasks by recurrence modes (source of truth from recurrence.py)
        filters = {"mode__in": RECURRING_MODES}
        if user_id:
            filters["assign_to_id"] = user_id

        # Group by unique series keys
        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        for s in seeds:
            # Latest occurrence in this series
            last = (
                Checklist.objects
                .filter(**s)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not last:
                continue

            # If there is already a future pending in this series, skip
            if Checklist.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
                continue

            # Compute the next planned datetime using the canonical function
            next_dt = compute_next_planned_datetime(last.planned_date, last.mode, last.frequency)

            # Advance until it's in the future (safety guard to avoid infinite loops)
            safety = 0
            while next_dt and next_dt <= now and safety < 730:
                next_dt = compute_next_planned_datetime(next_dt, last.mode, last.frequency)
                safety += 1

            if not next_dt:
                # Invalid recurrence config; nothing to create
                continue

            # Avoid creating a near-duplicate within ±1 minute
            is_dupe = Checklist.objects.filter(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if is_dupe:
                continue

            if dry_run:
                self.stdout.write(f"Would create next occurrence for {s['task_name']} at {next_dt}")
            else:
                with transaction.atomic():
                    Checklist.objects.create(
                        assign_by=last.assign_by,
                        task_name=last.task_name,
                        message=last.message,
                        assign_to=last.assign_to,
                        planned_date=next_dt,
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
                        group_name=getattr(last, "group_name", None),
                        actual_duration_minutes=0,
                        status="Pending",
                    )
                created_count += 1
                self.stdout.write(self.style.SUCCESS(
                    f"Created next occurrence for {s['task_name']} at {next_dt}"
                ))

        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would have created {created_count} tasks"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Successfully created {created_count} tasks"))
