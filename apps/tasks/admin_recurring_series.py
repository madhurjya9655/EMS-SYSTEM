#apps\tasks\admin_recurring_series.py
from django.contrib import admin

from apps.tasks.models import ChecklistRecurringSeries


@admin.register(ChecklistRecurringSeries)
class ChecklistRecurringSeriesAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "task_name",
        "assign_to",
        "mode",
        "frequency",
        "group_name",
        "next_run_at",
        "is_active",
        "is_deleted",
    )
    list_filter = ("mode", "is_active", "is_deleted")
    search_fields = (
        "task_name",
        "assign_to__username",
        "assign_to__email",
        "group_name",
    )
    readonly_fields = ("created_at", "updated_at", "deleted_at")
    autocomplete_fields = (
        "assign_by",
        "assign_to",
        "assign_pc",
        "notify_to",
        "auditor",
        "deleted_by",
    )
