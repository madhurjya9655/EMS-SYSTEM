# apps/tasks/management/commands/stop_checklist_series.py
from __future__ import annotations

from django.contrib.auth import get_user_model
from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import (
    Checklist,
    ChecklistRecurringSeries,
)


class Command(BaseCommand):
    help = (
        "Permanently stop one recurring Checklist series, hide/remove its "
        "Pending occurrences, and preserve all Completed history. "
        "Dry-run by default; use --apply to write."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--email",
            required=True,
            help="Assignee email address.",
        )

        parser.add_argument(
            "--task-name",
            required=True,
            help="Exact recurring task name.",
        )

        parser.add_argument(
            "--series-id",
            type=int,
            default=None,
            help=(
                "Required when more than one series matches the employee "
                "and task name."
            ),
        )

        parser.add_argument(
            "--reason",
            default="No longer required",
            help="Permanent stop reason.",
        )

        parser.add_argument(
            "--deleted-by-id",
            type=int,
            default=None,
            help="Optional user ID responsible for the stop operation.",
        )

        parser.add_argument(
            "--hard-delete-pending",
            action="store_true",
            help=(
                "Physically delete Pending occurrences. "
                "Without this flag they are safely soft-deleted."
            ),
        )

        parser.add_argument(
            "--apply",
            action="store_true",
            help="Apply the permanent stop.",
        )

    def handle(self, *args, **options):
        User = get_user_model()

        email = str(options["email"] or "").strip()
        task_name = str(
            options["task_name"] or ""
        ).strip()
        series_id = options.get("series_id")
        reason = str(
            options.get("reason") or "No longer required"
        ).strip()[:255]
        deleted_by_id = options.get("deleted_by_id")
        hard_delete_pending = bool(
            options.get("hard_delete_pending")
        )
        apply_changes = bool(options.get("apply"))

        if not email:
            raise CommandError(
                "--email cannot be blank."
            )

        if not task_name:
            raise CommandError(
                "--task-name cannot be blank."
            )

        try:
            employee = User.objects.get(
                email__iexact=email,
            )
        except User.DoesNotExist as exc:
            raise CommandError(
                f"No employee found for {email!r}."
            ) from exc
        except User.MultipleObjectsReturned as exc:
            raise CommandError(
                f"More than one employee uses email {email!r}."
            ) from exc

        deleted_by = None

        if deleted_by_id is not None:
            try:
                deleted_by = User.objects.get(
                    pk=deleted_by_id,
                )
            except User.DoesNotExist as exc:
                raise CommandError(
                    f"No user found for --deleted-by-id "
                    f"{deleted_by_id}."
                ) from exc

        matches = ChecklistRecurringSeries.objects.filter(
            assign_to=employee,
            task_name=task_name,
        )

        if series_id:
            matches = matches.filter(
                pk=series_id,
            )

        series_rows = list(
            matches.order_by("id")
        )

        if not series_rows:
            raise CommandError(
                "No matching recurring series was found."
            )

        if len(series_rows) > 1 and not series_id:
            self.stdout.write(
                self.style.WARNING(
                    "More than one recurring series matched:"
                )
            )

            for series in series_rows:
                self.stdout.write(
                    str(
                        {
                            "series_id": series.id,
                            "task_name": series.task_name,
                            "mode": series.mode,
                            "frequency": series.frequency,
                            "group_name": series.group_name,
                            "is_active": series.is_active,
                            "is_deleted": series.is_deleted,
                            "next_run_at": series.next_run_at,
                        }
                    )
                )

            raise CommandError(
                "Re-run with --series-id to identify the exact series."
            )

        series = series_rows[0]

        pending_queryset = Checklist.objects.filter(
            recurring_series=series,
            status="Pending",
        )

        completed_queryset = Checklist.objects.filter(
            recurring_series=series,
            status="Completed",
        )

        pending_count = pending_queryset.count()
        completed_count = completed_queryset.count()

        self.stdout.write(
            str(
                {
                    "series_id": series.id,
                    "employee": employee.email,
                    "task_name": series.task_name,
                    "mode": series.mode,
                    "frequency": series.frequency,
                    "group_name": series.group_name,
                    "pending_to_remove": pending_count,
                    "completed_to_preserve": completed_count,
                    "pending_removal_mode": (
                        "hard_delete"
                        if hard_delete_pending
                        else "soft_delete"
                    ),
                    "reason": reason,
                    "apply": apply_changes,
                }
            )
        )

        if not apply_changes:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run only. Nothing was changed. "
                    "Add --apply after reviewing the counts."
                )
            )
            return

        now = timezone.now()

        with transaction.atomic():
            locked_series = (
                ChecklistRecurringSeries.objects
                .select_for_update()
                .get(pk=series.pk)
            )

            locked_pending = (
                Checklist.objects
                .select_for_update()
                .filter(
                    recurring_series=locked_series,
                    status="Pending",
                )
            )

            locked_series.is_active = False
            locked_series.is_deleted = True
            locked_series.next_run_at = None
            locked_series.deleted_at = now
            locked_series.deleted_by = deleted_by
            locked_series.delete_reason = reason

            locked_series.save(
                update_fields=[
                    "is_active",
                    "is_deleted",
                    "next_run_at",
                    "deleted_at",
                    "deleted_by",
                    "delete_reason",
                    "updated_at",
                ]
            )

            if hard_delete_pending:
                removed_count, deletion_details = (
                    locked_pending.delete()
                )

                removal_description = (
                    f"hard-deleted Django objects={removed_count}; "
                    f"details={deletion_details}"
                )
            else:
                pending_ids = list(
                    locked_pending.values_list(
                        "id",
                        flat=True,
                    )
                )

                removed_count = Checklist.objects.filter(
                    id__in=pending_ids,
                    recurring_series=locked_series,
                    status="Pending",
                ).update(
                    is_active=False,
                    is_deleted=True,
                    is_skipped_due_to_leave=True,
                    deleted_at=now,
                    deleted_by=deleted_by,
                    delete_reason=reason,
                    skip_reason=(
                        "Recurring series permanently stopped"
                    ),
                )

                removal_description = (
                    f"soft-deleted pending rows={removed_count}"
                )

        preserved_count = Checklist.objects.filter(
            recurring_series=series,
            status="Completed",
        ).count()

        self.stdout.write(
            self.style.SUCCESS(
                f"Recurring series {series.id} stopped permanently. "
                f"{removal_description}. "
                f"Completed history preserved={preserved_count}."
            )
        )