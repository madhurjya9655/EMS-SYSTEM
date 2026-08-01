# apps/tasks/management/commands/backfill_checklist_series.py
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Iterable

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, ChecklistRecurringSeries
from apps.tasks.recurrence_utils import (
    RECURRING_MODES,
    get_next_planned_date,
    normalize_mode,
)


class Command(BaseCommand):
    help = (
        "Backfill ChecklistRecurringSeries from legacy recurring Checklist "
        "rows and link all matching occurrences. Dry-run by default; "
        "use --apply to write."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Create/update masters and link Checklist rows.",
        )
        parser.add_argument(
            "--task-name",
            action="append",
            default=[],
            help=(
                "Restrict to an exact task name. "
                "May be supplied multiple times."
            ),
        )
        parser.add_argument(
            "--assign-to-id",
            type=int,
            default=None,
            help="Restrict to one assignee ID.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Maximum number of legacy Checklist rows to inspect.",
        )
        parser.add_argument(
            "--only-unlinked",
            action="store_true",
            help="Inspect only Checklist rows with recurring_series=NULL.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options.get("apply"))
        assign_to_id = options.get("assign_to_id")
        only_unlinked = bool(options.get("only_unlinked"))

        task_names = [
            str(value).strip()
            for value in options.get("task_name", [])
            if str(value).strip()
        ]

        limit = options.get("limit")

        if limit is not None and limit < 1:
            raise CommandError("--limit must be greater than zero.")

        queryset = Checklist.objects.filter(mode__in=RECURRING_MODES)

        if only_unlinked:
            queryset = queryset.filter(recurring_series__isnull=True)

        if task_names:
            queryset = queryset.filter(task_name__in=task_names)

        if assign_to_id:
            queryset = queryset.filter(assign_to_id=assign_to_id)

        queryset = (
            queryset
            .select_related(
                "assign_by",
                "assign_to",
                "assign_pc",
                "notify_to",
                "auditor",
                "deleted_by",
                "recurring_series",
            )
            .order_by(
                "assign_to_id",
                "task_name",
                "mode",
                "frequency",
                "group_name",
                "planned_date",
                "id",
            )
        )

        if limit is not None:
            queryset = queryset[:limit]

        rows = list(queryset)
        grouped: dict[tuple, list[Checklist]] = defaultdict(list)

        for row in rows:
            mode = normalize_mode(row.mode)

            if mode not in RECURRING_MODES:
                continue

            try:
                frequency = max(int(row.frequency or 1), 1)
            except (TypeError, ValueError):
                frequency = 1

            key = (
                row.assign_to_id,
                str(row.task_name or "").strip(),
                mode,
                frequency,
                str(row.group_name or "").strip(),
            )
            grouped[key].append(row)

        self.stdout.write(
            f"Found {len(rows)} recurring Checklist row(s) "
            f"in {len(grouped)} logical series."
        )

        created_series = 0
        updated_series = 0
        linked_rows = 0

        for key, items in grouped.items():
            result = self._process_group(
                key=key,
                items=items,
                apply_changes=apply_changes,
            )
            created_series += result["created_series"]
            updated_series += result["updated_series"]
            linked_rows += result["linked_rows"]

        self.stdout.write("")
        self.stdout.write(f"Series to create/created : {created_series}")
        self.stdout.write(f"Series to update/updated : {updated_series}")
        self.stdout.write(f"Rows to link/linked      : {linked_rows}")

        if apply_changes:
            self.stdout.write(
                self.style.SUCCESS(
                    "Checklist recurring-series backfill completed."
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run only. No rows were modified. "
                    "Re-run with --apply after reviewing the output."
                )
            )

    @staticmethod
    def _sort_key(item: Checklist):
        planned = item.planned_date

        if planned is None:
            planned = datetime.min

        if timezone.is_aware(planned):
            planned = planned.astimezone(timezone.get_current_timezone()).replace(
                tzinfo=None
            )

        return planned, item.id

    @staticmethod
    def _normalized_priority(item: Checklist) -> str:
        """
        Return a valid non-blank priority for the series master.

        Legacy Checklist rows may contain NULL, blank, or an obsolete choice.
        ChecklistRecurringSeries.priority is validated during normal saves, so
        the backfill normalizes invalid legacy values.
        """
        field = ChecklistRecurringSeries._meta.get_field("priority")
        choices = list(getattr(field, "choices", ()) or ())
        valid_values = [str(value) for value, _label in choices]

        raw = str(getattr(item, "priority", "") or "").strip()

        if raw and (not valid_values or raw in valid_values):
            return raw

        if "Low" in valid_values:
            return "Low"

        if valid_values:
            return valid_values[0]

        default = field.get_default()
        default_text = str(default or "").strip()

        return default_text or "Low"

    @staticmethod
    def _local_date(value):
        if value is None:
            return None

        if timezone.is_naive(value):
            value = timezone.make_aware(
                value,
                timezone.get_current_timezone(),
            )

        return timezone.localtime(value).date()

    def _process_group(
        self,
        *,
        key: tuple,
        items: Iterable[Checklist],
        apply_changes: bool,
    ) -> dict[str, int]:
        (
            assign_to_id,
            task_name,
            mode,
            frequency,
            group_name,
        ) = key

        items = sorted(list(items), key=self._sort_key)

        if not items:
            return {
                "created_series": 0,
                "updated_series": 0,
                "linked_rows": 0,
            }

        first = items[0]
        latest = items[-1]

        completed_items = [
            item
            for item in items
            if item.status == "Completed"
            and not item.is_deleted
            and item.is_active
            and not item.is_skipped_due_to_leave
        ]

        pending_items = [
            item
            for item in items
            if item.status == "Pending"
            and not item.is_deleted
            and item.is_active
            and not item.is_skipped_due_to_leave
        ]

        latest_completed = completed_items[-1] if completed_items else None
        latest_pending = pending_items[-1] if pending_items else None

        # A legacy deleted occurrence acts as a permanent-series tombstone.
        permanently_deleted = any(item.is_deleted for item in items)

        is_active = (
            not permanently_deleted
            and any(
                item.is_active and not item.is_deleted
                for item in items
            )
        )

        schedule_anchor = latest_pending or latest_completed
        next_run_at = None

        if (
            is_active
            and schedule_anchor is not None
            and schedule_anchor.planned_date is not None
        ):
            next_run_at = get_next_planned_date(
                schedule_anchor.planned_date,
                mode,
                frequency,
            )

        end_dates = [
            item.recurrence_end_date
            for item in items
            if item.recurrence_end_date is not None
        ]
        recurrence_end_date = min(end_dates) if end_dates else None

        next_run_date = self._local_date(next_run_at)

        if (
            next_run_date is not None
            and recurrence_end_date is not None
            and next_run_date > recurrence_end_date
        ):
            next_run_at = None
            is_active = False

        deleted_at = next(
            (
                item.deleted_at
                for item in reversed(items)
                if item.deleted_at is not None
            ),
            None,
        )

        deleted_by_id = next(
            (
                item.deleted_by_id
                for item in reversed(items)
                if item.deleted_by_id is not None
            ),
            None,
        )

        delete_reason = next(
            (
                str(item.delete_reason).strip()
                for item in reversed(items)
                if str(item.delete_reason or "").strip()
            ),
            "",
        )

        priority = self._normalized_priority(latest)

        values = {
            "assign_by_id": latest.assign_by_id,
            "message": latest.message or "",
            "first_planned_date": first.planned_date,
            "next_run_at": next_run_at,
            "recurrence_end_date": recurrence_end_date,
            "priority": priority,
            "attachment_mandatory": bool(latest.attachment_mandatory),
            "time_per_task_minutes": latest.time_per_task_minutes or 0,
            "remind_before_days": latest.remind_before_days or 0,
            "assign_pc_id": latest.assign_pc_id,
            "notify_to_id": latest.notify_to_id,
            "auditor_id": latest.auditor_id,
            "set_reminder": bool(latest.set_reminder),
            "reminder_mode": latest.reminder_mode,
            "reminder_frequency": latest.reminder_frequency,
            "reminder_starting_time": latest.reminder_starting_time,
            "checklist_auto_close": bool(latest.checklist_auto_close),
            "checklist_auto_close_days": (
                latest.checklist_auto_close_days or 0
            ),
            "is_active": bool(is_active),
            "is_deleted": bool(permanently_deleted),
            "deleted_at": deleted_at,
            "deleted_by_id": deleted_by_id,
            "delete_reason": delete_reason,
        }

        existing = (
            ChecklistRecurringSeries.objects
            .filter(
                assign_to_id=assign_to_id,
                task_name=task_name,
                mode=mode,
                frequency=frequency,
                group_name=group_name,
            )
            .order_by("id")
            .first()
        )

        action = "update" if existing else "create"
        row_ids = [item.pk for item in items]

        currently_linked = sum(
            1
            for item in items
            if item.recurring_series_id is not None
        )

        self.stdout.write(
            str(
                {
                    "action": action,
                    "series_id": existing.pk if existing else None,
                    "assign_to_id": assign_to_id,
                    "task_name": task_name,
                    "mode": mode,
                    "frequency": frequency,
                    "group_name": group_name,
                    "rows": len(items),
                    "currently_linked": currently_linked,
                    "completed": len(completed_items),
                    "pending": len(pending_items),
                    "priority": priority,
                    "is_active": is_active,
                    "is_deleted": permanently_deleted,
                    "next_run_at": next_run_at,
                }
            )
        )

        if not apply_changes:
            return {
                "created_series": int(existing is None),
                "updated_series": int(existing is not None),
                "linked_rows": len(row_ids),
            }

        with transaction.atomic():
            if existing is None:
                # bulk_create bypasses the model's save()/full_clean() override.
                # This is intentional for historical data whose original date
                # may now be a holiday or leave date.
                series = ChecklistRecurringSeries(
                    assign_to_id=assign_to_id,
                    task_name=task_name,
                    mode=mode,
                    frequency=frequency,
                    group_name=group_name,
                    **values,
                )

                ChecklistRecurringSeries.objects.bulk_create([series])

                if series.pk is None:
                    series = ChecklistRecurringSeries.objects.get(
                        assign_to_id=assign_to_id,
                        task_name=task_name,
                        mode=mode,
                        frequency=frequency,
                        group_name=group_name,
                    )

                was_created = True
            else:
                series = (
                    ChecklistRecurringSeries.objects
                    .select_for_update()
                    .get(pk=existing.pk)
                )

                # QuerySet.update bypasses save()/full_clean(), which is needed
                # for historical rows whose old dates may no longer validate.
                ChecklistRecurringSeries.objects.filter(pk=series.pk).update(
                    **values
                )
                series.refresh_from_db()
                was_created = False

            linked = (
                Checklist.objects
                .filter(pk__in=row_ids)
                .exclude(recurring_series=series)
                .update(recurring_series=series)
            )

        return {
            "created_series": int(was_created),
            "updated_series": int(not was_created),
            "linked_rows": linked,
        }