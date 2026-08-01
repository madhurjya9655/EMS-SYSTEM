# apps/tasks/management/commands/roll_recurrences.py
from __future__ import annotations

import json
import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from apps.tasks.services.recurring_series import generate_due_series


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Compatibility command for rolling recurring checklist occurrences.

    Recurrence source of truth:
        ChecklistRecurringSeries

    This command must never infer recurring series from Checklist rows.

    Production behavior:
    - generates only from active, non-deleted ChecklistRecurringSeries rows;
    - preserves completed Checklist history;
    - refuses to recreate deleted recurring series;
    - uses the centralized recurring-series service;
    - supports dry-run by default when --apply is not supplied;
    - optionally limits generation to one employee;
    - does not send checklist emails;
    - does not shift Sunday/holiday/leave occurrences.
    """

    help = (
        "Generate due checklist occurrences from ChecklistRecurringSeries. "
        "Dry-run by default. Use --apply to persist generated occurrences."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user-id",
            type=int,
            default=None,
            help="Limit generation to one assignee user ID.",
        )

        parser.add_argument(
            "--apply",
            action="store_true",
            help="Persist generated occurrences. Without this flag, the command is read-only.",
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help="Maximum number of recurring series to inspect. Default: 1000.",
        )

        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Print one result row for every processed recurring series.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        apply_changes = bool(options.get("apply"))
        show_details = bool(options.get("show_details"))

        try:
            limit = int(options.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            raise CommandError("--limit must be a valid integer.") from exc

        if limit < 1:
            raise CommandError("--limit must be greater than zero.")

        dry_run = not apply_changes

        self.stdout.write("")
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                "BOS Lakshya recurring checklist generation"
            )
        )
        self.stdout.write(
            f"Mode       : {'APPLY' if apply_changes else 'DRY-RUN'}"
        )
        self.stdout.write(
            f"User ID    : {user_id if user_id is not None else 'ALL'}"
        )
        self.stdout.write(f"Series limit: {limit}")
        self.stdout.write("")

        try:
            result = generate_due_series(
                user_id=user_id,
                dry_run=dry_run,
                limit=limit,
            )
        except Exception as exc:
            logger.exception(
                "roll_recurrences failed for user_id=%s dry_run=%s limit=%s",
                user_id,
                dry_run,
                limit,
            )
            raise CommandError(
                f"Recurring generation failed: {type(exc).__name__}: {exc}"
            ) from exc

        checked = int(result.get("checked") or 0)
        created = int(result.get("created") or 0)
        results = result.get("results") or []

        reason_counts: dict[str, int] = {}

        for item in results:
            reason = str(item.get("reason") or "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            if show_details:
                self._print_result(item)

        self.stdout.write("")
        self.stdout.write("Summary")
        self.stdout.write("-" * 70)
        self.stdout.write(f"Series checked : {checked}")

        if dry_run:
            would_create = sum(
                1
                for item in results
                if item.get("reason") == "dry_run_would_create"
            )
            self.stdout.write(f"Would create   : {would_create}")
        else:
            self.stdout.write(f"Created        : {created}")

        if reason_counts:
            self.stdout.write("")
            self.stdout.write("Result counts")
            self.stdout.write("-" * 70)

            for reason in sorted(reason_counts):
                self.stdout.write(
                    f"{reason:<35} {reason_counts[reason]}"
                )

        self.stdout.write("")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run complete. No Checklist rows or recurring-series "
                    "records were modified."
                )
            )
            self.stdout.write(
                "Run again with --apply only after reviewing the output."
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Recurring generation complete. "
                    f"Created {created} Checklist occurrence(s)."
                )
            )

        return result

    def _print_result(self, item: dict[str, Any]) -> None:
        """
        Print one recurring-series generation result in a stable format.
        """
        printable = {
            "series_id": item.get("series_id"),
            "created": bool(item.get("created")),
            "occurrence_id": item.get("occurrence_id"),
            "reason": item.get("reason"),
            "planned_date": item.get("planned_date"),
        }

        self.stdout.write(
            json.dumps(
                printable,
                default=str,
                sort_keys=True,
            )
        )