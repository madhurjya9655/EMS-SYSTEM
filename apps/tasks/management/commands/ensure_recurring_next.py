# apps/tasks/management/commands/ensure_recurring_next.py
from __future__ import annotations

import logging
import pytz
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.recurrence import get_next_planned_date, RECURRING_MODES

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


class Command(BaseCommand):
    help = (
        "Ensure each recurring Checklist series has exactly ONE future Pending occurrence. "
        "Next is always scheduled at 19:00 IST on a working day (Sun/holiday -> next working day). "
        "Incomplete past tasks DO NOT block generating the next."
    )

    def add_arguments(self, parser):
        parser.add_argument("--user-id", type=int, help="Limit to specific assignee id.")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **opts):
        user_id = opts.get("user_id")
        dry = bool(opts.get("dry_run"))
        now = timezone.now()

        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        for s in seeds:
            series_key = dict(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            latest = (
                Checklist.objects.filter(**series_key)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not latest:
                continue

            # If a future Pending exists, do nothing (we only ever keep one)
            if Checklist.objects.filter(status="Pending", planned_date__gt=now, **series_key).exists():
                continue

            # Compute next at 19:00 IST on a working day; hop forward until strictly > now
            next_dt = get_next_planned_date(latest.planned_date, latest.mode, latest.frequency or 1)
            hops = 0
            while next_dt and next_dt <= now and hops < 730:
                next_dt = get_next_planned_date(next_dt, latest.mode, latest.frequency or 1)
                hops += 1
            if not next_dt:
                continue

            if dry:
                created += 1
                self.stdout.write(
                    f"[DRY RUN] Would create '{latest.task_name}' for user_id={s['assign_to_id']} at {next_dt.astimezone(IST):%Y-%m-%d %H:%M IST}"
                )
                continue

            try:
                with transaction.atomic():
                    Checklist.objects.create(
                        assign_by=latest.assign_by,
                        task_name=latest.task_name,
                        message=latest.message,
                        assign_to=latest.assign_to,
                        planned_date=next_dt,
                        priority=latest.priority,
                        attachment_mandatory=latest.attachment_mandatory,
                        mode=latest.mode,
                        frequency=latest.frequency,
                        recurrence_end_date=getattr(latest, "recurrence_end_date", None),
                        time_per_task_minutes=latest.time_per_task_minutes,
                        remind_before_days=latest.remind_before_days,
                        assign_pc=latest.assign_pc,
                        notify_to=latest.notify_to,
                        auditor=getattr(latest, "auditor", None),
                        set_reminder=latest.set_reminder,
                        reminder_mode=latest.reminder_mode,
                        reminder_frequency=latest.reminder_frequency,
                        reminder_starting_time=latest.reminder_starting_time,
                        checklist_auto_close=latest.checklist_auto_close,
                        checklist_auto_close_days=latest.checklist_auto_close_days,
                        group_name=getattr(latest, "group_name", None),
                        actual_duration_minutes=0,
                        status="Pending",
                    )
                created += 1
            except Exception as e:
                logger.exception("Failed to create next occurrence for %s: %s", series_key, e)

        if dry:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} task(s)"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} task(s)"))
