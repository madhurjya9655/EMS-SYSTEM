# apps/tasks/management/commands/ensure_recurring_next.py
from __future__ import annotations

import json
import logging

from django.core.management.base import BaseCommand, CommandError

from apps.tasks.services.recurring_series import generate_due_series


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Compatibility command for generating Checklist occurrences from "
        "ChecklistRecurringSeries. Legacy Checklist-row inference is disabled."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user-id",
            type=int,
            default=None,
            help="Limit processing to one assignee user ID.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show actions without modifying the database.",
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help="Maximum number of active series to inspect.",
        )

        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Print one JSON result per processed series.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        dry_run = bool(options.get("dry_run"))
        show_details = bool(options.get("show_details"))

        try:
            limit = int(options.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            raise CommandError("--limit must be an integer.") from exc

        if limit < 1:
            raise CommandError("--limit must be greater than zero.")

        try:
            result = generate_due_series(
                user_id=user_id,
                dry_run=dry_run,
                limit=limit,
            )
        except Exception as exc:
            logger.exception(
                "ensure_recurring_next failed: "
                "user_id=%s dry_run=%s limit=%s",
                user_id,
                dry_run,
                limit,
            )
            raise CommandError(
                f"Generation failed: {type(exc).__name__}: {exc}"
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

        self.stdout.write(f"Series checked: {checked}")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"Dry-run complete. Would create {would_create} occurrence(s)."
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Created {created} occurrence(s)."
                )
            )

        return result