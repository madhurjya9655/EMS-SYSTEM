# apps/tasks/management/commands/generate_missed_recurrences.py
from __future__ import annotations

import logging
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.recurrence import (
    get_next_planned_date,
    schedule_recurring_at_10am,
    RECURRING_MODES,
)
from apps.tasks.utils import (
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Ensure one future 'Pending' checklist per recurring series exists. "
        "Future recurrences are generated at 10:00 AM IST and skip Sundays/holidays."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show without creating")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee")
        parser.add_argument("--no-email", action="store_true", help="Skip email notifications")

    def handle(self, *args, **opts):
        dry_run = opts.get("dry_run", False)
        user_id = opts.get("user_id")
        send_emails = not opts.get("no_email", False)

        now = timezone.now()
        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        groups = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        processed = 0

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created."))

        for g in groups:
            processed += 1
            instance = (
                Checklist.objects.filter(
                    assign_to_id=g["assign_to_id"],
                    task_name=g["task_name"],
                    mode=g["mode"],
                    frequency=g["frequency"],
                    group_name=g["group_name"],
                )
                .order_by("-planned_date", "-id")
                .first()
            )
            if not instance:
                continue

            # Already have a future pending? skip
            if Checklist.objects.filter(
                assign_to_id=instance.assign_to_id,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gt=now,
                status="Pending",
            ).exists():
                continue

            next_planned = get_next_planned_date(instance.planned_date, instance.mode, instance.frequency)

            # Catch up to the future
            safety = 0
            while next_planned and next_planned <= now and safety < 730:  # ~2 years
                next_planned = get_next_planned_date(next_planned, instance.mode, instance.frequency)
                safety += 1
            if not next_planned:
                continue

            # Normalize to 10:00 IST and skip Sunday/holidays (idempotent)
            next_planned = schedule_recurring_at_10am(next_planned)

            # Dupe guard (±1 minute)
            dupe = Checklist.objects.filter(
                assign_to_id=instance.assign_to_id,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gte=next_planned - timedelta(minutes=1),
                planned_date__lt=next_planned + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(f"[DRY RUN] Would create: {instance.task_name} at {next_planned}")
                continue

            try:
                with transaction.atomic():
                    kwargs = dict(
                        assign_by=instance.assign_by,
                        task_name=instance.task_name,
                        assign_to=instance.assign_to,
                        planned_date=next_planned,
                        priority=instance.priority,
                        attachment_mandatory=instance.attachment_mandatory,
                        mode=instance.mode,
                        frequency=instance.frequency,
                        status="Pending",
                        actual_duration_minutes=0,
                    )
                    # Optional fields that may or may not exist on your model
                    for opt in (
                        "message",
                        "time_per_task_minutes",
                        "remind_before_days",
                        "assign_pc",
                        "notify_to",
                        "auditor",
                        "set_reminder",
                        "reminder_mode",
                        "reminder_frequency",
                        "reminder_starting_time",
                        "checklist_auto_close",
                        "checklist_auto_close_days",
                        "group_name",
                    ):
                        if hasattr(instance, opt):
                            kwargs[opt] = getattr(instance, opt)
                    new_obj = Checklist.objects.create(**kwargs)

                created += 1

                if send_emails:
                    try:
                        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
                        send_checklist_assignment_to_user(
                            task=new_obj,
                            complete_url=complete_url,
                            subject_prefix="Recurring Checklist Generated",
                        )
                        send_checklist_admin_confirmation(
                            task=new_obj,
                            subject_prefix="Recurring Checklist Generated",
                        )
                    except Exception as e:
                        logger.exception("Email failure for recurring checklist %s: %s", new_obj.id, e)

                self.stdout.write(self.style.SUCCESS(f"✅ Created: {new_obj.task_name} at {next_planned}"))
            except Exception as e:
                logger.exception("Failed to create recurrence for %s: %s", instance.task_name, e)
                self.stdout.write(self.style.ERROR(f"❌ Failed: {instance.task_name} - {e}"))

        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} tasks"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} tasks from {processed} series"))
        if created == 0:
            self.stdout.write("No missed recurrences needed to be created.")
