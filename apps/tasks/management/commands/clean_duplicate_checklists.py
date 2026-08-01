# apps/tasks/management/commands/clean_duplicate_checklists.py
from __future__ import annotations

from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.timezone import localtime

from apps.tasks.models import Checklist


class Command(BaseCommand):
    help = (
        "Delete duplicate Pending Checklist occurrences only. "
        "Completed history is never deleted. Dry-run by default."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Delete duplicates. Without this flag the command is read-only.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options.get("apply"))
        grouped = defaultdict(list)

        queryset = (
            Checklist.objects
            .filter(
                status="Pending",
                is_deleted=False,
                is_active=True,
            )
            .select_related("recurring_series")
            .order_by("id")
        )

        for checklist in queryset.iterator(chunk_size=1000):
            planned_day = localtime(checklist.planned_date).date()

            if checklist.recurring_series_id:
                key = (
                    "series",
                    checklist.recurring_series_id,
                    planned_day,
                )
            else:
                key = (
                    "legacy",
                    checklist.assign_to_id,
                    checklist.task_name,
                    checklist.mode,
                    int(checklist.frequency or 1),
                    checklist.group_name or "",
                    planned_day,
                )

            grouped[key].append(checklist)

        duplicate_ids = []

        for key, rows in grouped.items():
            if len(rows) <= 1:
                continue

            rows.sort(key=lambda item: (item.planned_date, item.id))

            keep = rows[0]
            duplicates = rows[1:]
            ids = [row.id for row in duplicates]

            duplicate_ids.extend(ids)

            self.stdout.write(
                f"{key}: keep={keep.id}, duplicates={ids}"
            )

        action = "Will delete" if apply_changes else "Would delete"

        self.stdout.write(
            f"{action} {len(duplicate_ids)} duplicate Pending occurrence(s)."
        )

        if not apply_changes or not duplicate_ids:
            return

        with transaction.atomic():
            deleted_count, _ = Checklist.objects.filter(
                id__in=duplicate_ids,
                status="Pending",
            ).delete()

        self.stdout.write(
            self.style.SUCCESS(
                f"Deleted Django objects: {deleted_count}"
            )
        )