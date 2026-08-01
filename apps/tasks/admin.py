# apps/tasks/admin.py
from __future__ import annotations

from django.contrib import admin, messages
from django.db import transaction
from django.utils import timezone

from .models import (
    BulkUpload,
    Checklist,
    ChecklistRecurringSeries,
    Delegation,
    FMS,
    HelpTicket,
)


def _soft_delete_checklist_rows(queryset, *, deleted_by, reason: str) -> int:
    """Permanently retire Checklist rows without physically removing audit data."""
    return queryset.update(
        is_deleted=True,
        is_active=False,
        is_skipped_due_to_leave=True,
        deleted_at=timezone.now(),
        deleted_by=deleted_by,
        delete_reason=(reason or "Task permanently deleted by admin")[:255],
    )


def _delete_recurring_series(series_id: int, *, deleted_by, reason: str) -> int:
    """
    Stop a recurring master and retire every occurrence belonging to it.

    Clearing next_run_at and marking the master deleted ensures Celery, cron,
    management commands and the today materializer can never recreate it.
    """
    with transaction.atomic():
        series = (
            ChecklistRecurringSeries.objects
            .select_for_update()
            .get(pk=series_id)
        )
        series.soft_delete(user=deleted_by, reason=reason)
        return _soft_delete_checklist_rows(
            Checklist.objects.filter(recurring_series_id=series.pk),
            deleted_by=deleted_by,
            reason=reason,
        )


def _delete_checklist(checklist_id: int, *, deleted_by, reason: str) -> int:
    with transaction.atomic():
        checklist = (
            Checklist.objects
            .select_for_update()
            .only("pk", "recurring_series_id")
            .get(pk=checklist_id)
        )

        if checklist.recurring_series_id:
            return _delete_recurring_series(
                checklist.recurring_series_id,
                deleted_by=deleted_by,
                reason=reason,
            )

        return _soft_delete_checklist_rows(
            Checklist.objects.filter(pk=checklist.pk),
            deleted_by=deleted_by,
            reason=reason,
        )


class PermanentTaskDeleteAdminMixin:
    """Replace Django Admin physical deletes with irreversible task retirement."""

    delete_reason = "Task permanently deleted through Django Admin"

    def _soft_delete_object(self, obj, request) -> int:
        if isinstance(obj, Checklist):
            return _delete_checklist(
                obj.pk,
                deleted_by=request.user,
                reason=self.delete_reason,
            )

        if isinstance(obj, ChecklistRecurringSeries):
            return _delete_recurring_series(
                obj.pk,
                deleted_by=request.user,
                reason="Recurring task permanently deleted through Django Admin",
            )

        if hasattr(obj, "soft_delete"):
            obj.soft_delete(
                user=request.user,
                reason=self.delete_reason,
            )
            return 1

        raise TypeError(
            f"{type(obj).__name__} does not implement permanent task deletion."
        )

    def delete_model(self, request, obj):
        affected = self._soft_delete_object(obj, request)
        self.message_user(
            request,
            f"Task permanently retired. {affected} row(s) updated.",
            level=messages.SUCCESS,
        )

    def delete_queryset(self, request, queryset):
        affected = 0
        processed_series_ids: set[int] = set()

        for obj in queryset.iterator(chunk_size=200):
            if isinstance(obj, Checklist) and obj.recurring_series_id:
                if obj.recurring_series_id in processed_series_ids:
                    continue
                processed_series_ids.add(obj.recurring_series_id)

            if isinstance(obj, ChecklistRecurringSeries):
                if obj.pk in processed_series_ids:
                    continue
                processed_series_ids.add(obj.pk)

            affected += self._soft_delete_object(obj, request)

        self.message_user(
            request,
            f"Selected task records permanently retired. {affected} row(s) updated.",
            level=messages.SUCCESS,
        )


@admin.register(Checklist)
class ChecklistAdmin(PermanentTaskDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "task_name",
        "assign_to",
        "planned_date",
        "status",
        "is_active",
        "is_deleted",
        "recurring_series",
    )
    list_filter = (
        "status",
        "is_active",
        "is_deleted",
        "mode",
        "priority",
    )
    search_fields = (
        "task_name",
        "assign_to__username",
        "assign_to__email",
        "assign_by__username",
    )
    list_select_related = ("assign_to", "assign_by", "recurring_series")
    ordering = ("-planned_date", "-id")


@admin.register(ChecklistRecurringSeries)
class ChecklistRecurringSeriesAdmin(
    PermanentTaskDeleteAdminMixin,
    admin.ModelAdmin,
):
    list_display = (
        "task_name",
        "assign_to",
        "mode",
        "frequency",
        "next_run_at",
        "is_active",
        "is_deleted",
    )
    list_filter = ("mode", "is_active", "is_deleted", "priority")
    search_fields = (
        "task_name",
        "assign_to__username",
        "assign_to__email",
        "assign_by__username",
    )
    list_select_related = ("assign_to", "assign_by")
    ordering = ("next_run_at", "id")


@admin.register(Delegation)
class DelegationAdmin(PermanentTaskDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "task_name",
        "assign_to",
        "planned_date",
        "status",
        "is_active",
        "is_deleted",
    )
    list_filter = ("status", "is_active", "is_deleted", "priority")
    search_fields = (
        "task_name",
        "assign_to__username",
        "assign_to__email",
    )
    list_select_related = ("assign_to", "assign_by")
    ordering = ("-planned_date", "-id")


@admin.register(FMS)
class FMSAdmin(PermanentTaskDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "task_name",
        "assign_to",
        "planned_date",
        "status",
        "is_active",
        "is_deleted",
    )
    list_filter = ("status", "is_active", "is_deleted", "priority")
    search_fields = (
        "task_name",
        "assign_to__username",
        "assign_to__email",
    )
    list_select_related = ("assign_to", "assign_by")
    ordering = ("-planned_date", "-id")


@admin.register(HelpTicket)
class HelpTicketAdmin(PermanentTaskDeleteAdminMixin, admin.ModelAdmin):
    list_display = (
        "title",
        "assign_to",
        "planned_date",
        "status",
        "is_active",
        "is_deleted",
    )
    list_filter = ("status", "is_active", "is_deleted", "priority")
    search_fields = (
        "title",
        "description",
        "assign_to__username",
        "assign_to__email",
    )
    list_select_related = ("assign_to", "assign_by")
    ordering = ("-planned_date", "-id")


@admin.register(BulkUpload)
class BulkUploadAdmin(admin.ModelAdmin):
    list_display = ("form_type", "uploaded_at")
    list_filter = ("form_type",)
    ordering = ("-uploaded_at",)