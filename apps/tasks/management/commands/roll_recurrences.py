# apps/tasks/management/commands/roll_recurrences.py
from __future__ import annotations

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

from apps.tasks.models import Checklist, Delegation
from apps.tasks.recurrence import (
    get_next_planned_date,
    schedule_recurring_at_10am,
    RECURRING_MODES,
)

logger = logging.getLogger(__name__)


def _has_field(model, field_name: str) -> bool:
    return any(getattr(f, "name", None) == field_name for f in model._meta.get_fields())


class Command(BaseCommand):
    help = "Advanced recurrence management: roll forward, cleanup, and validate recurring tasks."

    def add_arguments(self, parser):
        parser.add_argument(
            "--action",
            choices=["roll", "cleanup", "validate", "all"],
            default="all",
            help="Which action to perform.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without making changes.",
        )
        parser.add_argument(
            "--days-ahead",
            type=int,
            default=30,
            help="Generate recurrences up to this many days ahead (for roll).",
        )
        parser.add_argument(
            "--cleanup-completed-days",
            type=int,
            default=90,
            help="Delete completed items older than this many days (for cleanup).",
        )
        parser.add_argument(
            "--user-id",
            type=int,
            help="Limit to a specific assignee (user id) for roll/validate.",
        )

    def handle(self, *args, **opts):
        action = opts["action"]
        dry_run = opts.get("dry_run", False)

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no data will be modified.\n"))

        if action in ("roll", "all"):
            count_chk, count_dlg = self._roll_forward(opts, dry_run)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Rolled recurrences — Checklist: {count_chk}, Delegation: {count_dlg}"
                )
            )

        if action in ("cleanup", "all"):
            deleted_chk, deleted_dlg = self._cleanup(opts, dry_run)
            if dry_run:
                self.stdout.write(
                    self.style.WARNING(
                        f"[DRY RUN] Would delete — Checklist: {deleted_chk}, Delegation: {deleted_dlg}"
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Deleted — Checklist: {deleted_chk}, Delegation: {deleted_dlg}"
                    )
                )

        if action in ("validate", "all"):
            issues = self._validate(opts)
            if issues:
                self.stdout.write(self.style.WARNING(f"Validation issues ({len(issues)}):"))
                for msg in issues:
                    self.stdout.write(f"  - {msg}")
            else:
                self.stdout.write(self.style.SUCCESS("All recurrence configurations look good."))

    # ------------------------------- ROLL -------------------------------- #
    def _roll_forward(self, opts, dry_run: bool):
        days_ahead = opts["days_ahead"]
        user_id = opts.get("user_id")
        target = timezone.now() + timedelta(days=days_ahead)

        chk_created = self._roll_model(
            model=Checklist,
            series_fields=("assign_to_id", "task_name", "mode", "frequency", "group_name"),
            target=target,
            dry_run=dry_run,
            user_id=user_id,
        )
        dlg_created = self._roll_model(
            model=Delegation,
            series_fields=("assign_to_id", "task_name", "mode", "frequency"),
            target=target,
            dry_run=dry_run,
            user_id=user_id,
        )
        return chk_created, dlg_created

    def _roll_model(self, model, series_fields, target, dry_run: bool, user_id=None) -> int:
        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        series = model.objects.filter(**filters).values(*series_fields).distinct()
        created_count = 0

        for s in series:
            latest = model.objects.filter(**s).order_by("-planned_date", "-id").first()
            if not latest:
                continue

            current = latest.planned_date
            safety = 0

            while current < target and safety < 365:  # cap at ~1 year of steps
                next_dt = get_next_planned_date(current, latest.mode, latest.frequency)
                if not next_dt or next_dt <= current:
                    break

                # Normalize to 10:00 AM IST and skip Sundays/holidays
                next_dt = schedule_recurring_at_10am(next_dt)

                # dupe guard (±1 minute)
                exists = model.objects.filter(
                    planned_date__gte=next_dt - timedelta(minutes=1),
                    planned_date__lt=next_dt + timedelta(minutes=1),
                    **s,
                ).exists()

                if not exists:
                    if dry_run:
                        created_count += 1
                        self.stdout.write(
                            f"[DRY RUN] Would create {model.__name__}: '{latest.task_name}' at {next_dt}"
                        )
                    else:
                        try:
                            with transaction.atomic():
                                kwargs = dict(
                                    assign_by=getattr(latest, "assign_by", None),
                                    task_name=latest.task_name,
                                    assign_to=latest.assign_to,
                                    planned_date=next_dt,
                                    priority=getattr(latest, "priority", None),
                                    attachment_mandatory=getattr(latest, "attachment_mandatory", False),
                                    mode=latest.mode,
                                    frequency=latest.frequency,
                                    status="Pending",
                                )
                                # optional fields
                                for opt in (
                                    "message",
                                    "time_per_task_minutes",
                                    "remind_before_days",
                                    "assign_pc",
                                    "notify_to",
                                    "set_reminder",
                                    "reminder_mode",
                                    "reminder_frequency",
                                    "reminder_starting_time",
                                    "checklist_auto_close",
                                    "checklist_auto_close_days",
                                    "group_name",
                                    "actual_duration_minutes",
                                ):
                                    if hasattr(latest, opt):
                                        kwargs[opt] = getattr(latest, opt)

                                model.objects.create(**kwargs)
                            created_count += 1
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"Created {model.__name__}: '{latest.task_name}' at {next_dt}"
                                )
                            )
                        except Exception as e:
                            logger.error("Failed to create %s: %s", model.__name__, e)

                current = next_dt or current
                safety += 1

        return created_count

    # ------------------------------ CLEANUP ------------------------------ #
    def _cleanup(self, opts, dry_run: bool):
        days = opts["cleanup_completed_days"]
        cutoff = timezone.now() - timedelta(days=days)

        deleted_chk = self._cleanup_model(Checklist, cutoff, dry_run)
        deleted_dlg = self._cleanup_model(Delegation, cutoff, dry_run)
        return deleted_chk, deleted_dlg

    def _cleanup_model(self, model, cutoff, dry_run: bool) -> int:
        if _has_field(model, "completed_at"):
            qs = model.objects.filter(status="Completed", completed_at__lt=cutoff)
        else:
            qs = model.objects.filter(status="Completed", planned_date__lt=cutoff)

        count = qs.count()
        if dry_run or count == 0:
            return count

        try:
            with transaction.atomic():
                deleted, _ = qs.delete()
            return deleted
        except Exception as e:
            logger.error("Cleanup failed for %s: %s", model.__name__, e)
            return 0

    # ------------------------------ VALIDATE ----------------------------- #
    def _validate(self, opts):
        user_id = opts.get("user_id")
        issues = []

        def _series_issues(model, name):
            f = {}
            if user_id:
                f["assign_to_id"] = user_id
            invalid = (
                model.objects.filter(mode__isnull=False)
                .exclude(mode__in=[m for m in RECURRING_MODES])
            )
            for obj in invalid:
                issues.append(f"{name} {obj.id}: invalid mode '{obj.mode}'")

            missing_freq = model.objects.filter(mode__in=RECURRING_MODES, frequency__isnull=True, **f)
            for obj in missing_freq:
                issues.append(f"{name} {obj.id}: missing frequency for mode '{obj.mode}'")

        _series_issues(Checklist, "Checklist")
        _series_issues(Delegation, "Delegation")
        return issues
