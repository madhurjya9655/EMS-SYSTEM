# apps/tasks/management/commands/generate_recurring_tasks.py
from __future__ import annotations

import json
import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from apps.tasks.services.recurring_series import generate_due_series


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Generate recurring Checklist occurrences from ChecklistRecurringSeries.

    Recurring-series ownership belongs only to ChecklistRecurringSeries.
    Existing Checklist rows are occurrences/history and are not used to infer
    series identity.

    Safety rules:
    - inactive series are ignored;
    - deleted series are ignored permanently;
    - completed Checklist history is preserved;
    - an existing active Pending occurrence blocks another occurrence;
    - Sunday, holiday and leave-blocked occurrences are skipped centrally;
    - no Checklist assignment email is sent by this command;
    - dry-run does not write to the database.

    The --no-email option is retained only so old Render/Cron commands do not
    fail. This command does not send Checklist emails in either mode.
    """

    help = (
        "Generate Checklist occurrences from ChecklistRecurringSeries. "
        "Use --dry-run to inspect actions without writing to the database."
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
            help="Show what would happen without creating Checklist rows.",
        )

        parser.add_argument(
            "--no-email",
            action="store_true",
            help=(
                "Retained for backward compatibility with existing schedules. "
                "This command never sends Checklist emails."
            ),
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help=(
                "Maximum number of active recurring-series masters to inspect. "
                "Default: 1000."
            ),
        )

        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Print one JSON result line for every processed series.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        dry_run = bool(options.get("dry_run"))
        show_details = bool(options.get("show_details"))

        try:
            limit = int(options.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            raise CommandError("--limit must be a valid integer.") from exc

        if limit < 1:
            raise CommandError("--limit must be greater than zero.")

        self.stdout.write("")
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                "Checklist recurring-series generator"
            )
        )
        self.stdout.write(
            f"Mode       : {'DRY-RUN' if dry_run else 'APPLY'}"
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
                "generate_recurring_tasks failed: "
                "user_id=%s dry_run=%s limit=%s",
                user_id,
                dry_run,
                limit,
            )

            raise CommandError(
                "Recurring generation failed: "
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
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            if show_details:
                self.stdout.write(
                    json.dumps(
                        {
                            "series_id": row.get("series_id"),
                            "created": bool(row.get("created")),
                            "occurrence_id": row.get("occurrence_id"),
                            "planned_date": row.get("planned_date"),
                            "reason": reason,
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

        self.stdout.write("")
        self.stdout.write("Summary")
        self.stdout.write("-" * 72)
        self.stdout.write(f"Series checked : {checked}")

        if dry_run:
            self.stdout.write(f"Would create   : {would_create}")
        else:
            self.stdout.write(f"Created        : {created}")

        if reason_counts:
            self.stdout.write("")
            self.stdout.write("Result counts")
            self.stdout.write("-" * 72)

            for reason in sorted(reason_counts):
                self.stdout.write(
                    f"{reason:<40} {reason_counts[reason]}"
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
                "Run without --dry-run only after reviewing the output."
            )

        else:
            self.stdout.write(
                self.style.SUCCESS(
                    "Recurring generation complete. "
                    f"Created {created} Checklist occurrence(s)."
                )
            )

        return result