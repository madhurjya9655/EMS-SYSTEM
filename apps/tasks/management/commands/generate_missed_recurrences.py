# apps/tasks/management/commands/generate_missed_recurrences.py
from __future__ import annotations

import json
import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from apps.tasks.services.recurring_series import generate_due_series


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Backfill missed recurring Checklist occurrences from series masters.

    This command uses ChecklistRecurringSeries as the only source of truth.
    It never infers recurring-series identity from old Checklist rows.

    Safety rules:
    - inactive recurring masters are ignored;
    - deleted recurring masters are ignored permanently;
    - inactive employees are ignored by the centralized generator;
    - an existing active pending occurrence blocks duplicate generation;
    - completed history is preserved;
    - dry-run is the default;
    - no assignment emails are sent.
    """

    help = (
        "Generate missed Checklist occurrences from "
        "ChecklistRecurringSeries. Dry-run by default. "
        "Use --apply to persist changes."
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
            help=(
                "Persist generated Checklist occurrences. "
                "Without this flag the command is read-only."
            ),
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help=(
                "Maximum number of active recurring-series masters "
                "to inspect. Default: 1000."
            ),
        )

        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Print one JSON result line for every processed series.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        apply_changes = bool(options.get("apply"))
        dry_run = not apply_changes
        show_details = bool(options.get("show_details"))

        try:
            limit = int(options.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            raise CommandError(
                "--limit must be a valid integer."
            ) from exc

        if limit < 1:
            raise CommandError(
                "--limit must be greater than zero."
            )

        self.stdout.write("")
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                "Missed Checklist recurring-series generation"
            )
        )

        self.stdout.write(
            f"Mode        : {'APPLY' if apply_changes else 'DRY-RUN'}"
        )
        self.stdout.write(
            f"User ID     : "
            f"{user_id if user_id is not None else 'ALL'}"
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
                "generate_missed_recurrences failed: "
                "user_id=%s dry_run=%s limit=%s",
                user_id,
                dry_run,
                limit,
            )

            raise CommandError(
                "Missed recurrence generation failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        checked = int(result.get("checked") or 0)
        created = int(result.get("created") or 0)

        rows: list[dict[str, Any]] = list(
            result.get("results") or []
        )

        reason_counts: dict[str, int] = {}

        for row in rows:
            reason = str(row.get("reason") or "unknown")
            reason_counts[reason] = (
                reason_counts.get(reason, 0) + 1
            )

            if show_details:
                self.stdout.write(
                    json.dumps(
                        {
                            "series_id": row.get("series_id"),
                            "created": bool(row.get("created")),
                            "occurrence_id": row.get(
                                "occurrence_id"
                            ),
                            "planned_date": row.get(
                                "planned_date"
                            ),
                            "reason": reason,
                            "skipped_steps": int(
                                row.get("skipped_steps") or 0
                            ),
                        },
                        default=str,
                        sort_keys=True,
                    )
                )

        would_create = sum(
            1
            for row in rows
            if row.get("reason") == "dry_run_would_create"
        )

        errors = sum(
            count
            for reason, count in reason_counts.items()
            if reason.startswith("error:")
        )

        self.stdout.write("")
        self.stdout.write("Summary")
        self.stdout.write("-" * 72)
        self.stdout.write(f"Series checked : {checked}")

        if dry_run:
            self.stdout.write(
                f"Would create   : {would_create}"
            )
        else:
            self.stdout.write(
                f"Created        : {created}"
            )

        self.stdout.write(f"Errors         : {errors}")

        if reason_counts:
            self.stdout.write("")
            self.stdout.write("Result counts")
            self.stdout.write("-" * 72)

            for reason in sorted(reason_counts):
                self.stdout.write(
                    f"{reason:<48} {reason_counts[reason]}"
                )

        self.stdout.write("")

        if errors:
            self.stdout.write(
                self.style.ERROR(
                    f"The generator reported {errors} error result(s). "
                    "Review the details before running with --apply."
                )
            )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run complete. No Checklist rows or "
                    "recurring-series records were modified."
                )
            )

            self.stdout.write(
                "Re-run with --apply only after reviewing "
                "the output."
            )

        else:
            self.stdout.write(
                self.style.SUCCESS(
                    "Missed recurrence run complete. "
                    f"Created {created} occurrence(s)."
                )
            )

        # Django management commands should return None or a string.
        # Returning the result dictionary causes:
        # AttributeError: 'dict' object has no attribute 'endswith'
        return None