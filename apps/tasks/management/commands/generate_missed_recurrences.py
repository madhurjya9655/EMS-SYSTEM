# apps/tasks/management/commands/generate_missed_recurrences.py
from __future__ import annotations

import json
import logging

from django.core.management.base import BaseCommand, CommandError

from apps.tasks.services.recurring_series import generate_due_series


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Backfill missed recurring Checklist occurrences from series masters.

    This is a compatibility command. It uses the same centralized generator as
    Celery and generate_recurring_tasks, so legacy Checklist-row inference can
    no longer recreate a deleted series.
    """

    help = (
        "Generate missed Checklist occurrences from ChecklistRecurringSeries. "
        "Dry-run by default. Use --apply to persist."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user-id",
            type=int,
            default=None,
            help="Limit processing to one assignee user ID.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Persist changes. Without this flag the command is read-only.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help="Maximum number of active series to inspect. Default: 1000.",
        )
        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Print one JSON result line per processed series.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        apply_changes = bool(options.get("apply"))
        dry_run = not apply_changes
        show_details = bool(options.get("show_details"))

        try:
            limit = int(options.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            raise CommandError("--limit must be an integer.") from exc

        if limit < 1:
            raise CommandError("--limit must be greater than zero.")

        self.stdout.write(
            self.style.MIGRATE_HEADING(
                "Missed ChecklistRecurringSeries generation"
            )
        )
        self.stdout.write(
            f"Mode      : {'APPLY' if apply_changes else 'DRY-RUN'}"
        )
        self.stdout.write(
            f"User ID   : {user_id if user_id is not None else 'ALL'}"
        )
        self.stdout.write(f"Limit     : {limit}")
        self.stdout.write("")

        try:
            result = generate_due_series(
                user_id=user_id,
                dry_run=dry_run,
                limit=limit,
            )
        except Exception as exc:
            logger.exception(
                "generate_missed_recurrences failed: "
                "user_id=%s dry_run=%s limit=%s",
                user_id,
                dry_run,
                limit,
            )
            raise CommandError(
                f"Missed recurrence generation failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        rows = result.get("results") or []

        if show_details:
            for row in rows:
                self.stdout.write(
                    json.dumps(
                        {
                            "series_id": row.get("series_id"),
                            "created": bool(row.get("created")),
                            "occurrence_id": row.get("occurrence_id"),
                            "planned_date": row.get("planned_date"),
                            "reason": row.get("reason"),
                        },
                        default=str,
                        sort_keys=True,
                    )
                )

        checked = int(result.get("checked") or 0)
        created = int(result.get("created") or 0)
        would_create = sum(
            1
            for row in rows
            if row.get("reason") == "dry_run_would_create"
        )

        self.stdout.write("")
        self.stdout.write(f"Series checked : {checked}")
        self.stdout.write(
            f"{'Would create' if dry_run else 'Created'}      : "
            f"{would_create if dry_run else created}"
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run complete. Re-run with --apply after review."
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Missed recurrence run complete. "
                    f"Created {created} occurrence(s)."
                )
            )

        return result