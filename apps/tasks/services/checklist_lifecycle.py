from __future__ import annotations

"""Shared lifecycle guards for recurring checklist series.

This module intentionally does not change recurrence date/business logic.
It only makes every caller use the same rules for:
- identifying legacy recurring series;
- excluding permanently deleted/inactive rows;
- blocking future generation after permanent deletion.
"""

from django.db.models import Q, QuerySet

from apps.tasks.models import Checklist

RECURRING_MODES = ("Daily", "Weekly", "Monthly", "Yearly")


def checklist_has_field(field_name: str) -> bool:
    try:
        Checklist._meta.get_field(field_name)
        return True
    except Exception:
        return False


def normalized_frequency(value) -> int:
    try:
        return max(int(value or 1), 1)
    except Exception:
        return 1


def recurring_series_q(
    *,
    assign_to_id: int,
    task_name: str,
    mode: str,
    frequency,
    group_name,
) -> Q:
    """Build one legacy-tolerant series filter.

    Legacy compatibility:
    - NULL frequency and frequency=1 are treated as the same series.
    - NULL group_name and empty group_name are treated as the same series.
    """
    freq = normalized_frequency(frequency)

    q = Q(
        assign_to_id=assign_to_id,
        task_name=task_name,
        mode=mode,
        frequency__in=[freq, None],
    )

    if group_name:
        q &= Q(group_name=group_name)
    else:
        q &= Q(group_name__in=["", None])

    return q


def active_occurrence_q() -> Q:
    q = Q()
    if checklist_has_field("is_deleted"):
        q &= Q(is_deleted=False)
    if checklist_has_field("is_active"):
        q &= Q(is_active=True)
    if checklist_has_field("is_skipped_due_to_leave"):
        q &= Q(is_skipped_due_to_leave=False)
    return q


def active_occurrences(qs: QuerySet) -> QuerySet:
    return qs.filter(active_occurrence_q())


def series_is_permanently_deleted(series_q: Q) -> bool:
    """Return True when any row records permanent deletion of the series.

    The delete workflow marks all matching rows. Checking any tombstone also
    protects older/mixed data where one completed row may have escaped an old
    delete implementation.
    """
    if checklist_has_field("is_deleted"):
        if Checklist.objects.filter(series_q, is_deleted=True).exists():
            return True

    if checklist_has_field("is_active") and checklist_has_field("deleted_at"):
        if Checklist.objects.filter(
            series_q,
            is_active=False,
            deleted_at__isnull=False,
        ).exists():
            return True

    return False


def apply_new_occurrence_lifecycle_defaults(data: dict) -> dict:
    data = dict(data)
    if checklist_has_field("is_deleted"):
        data["is_deleted"] = False
    if checklist_has_field("is_active"):
        data["is_active"] = True
    if checklist_has_field("is_skipped_due_to_leave"):
        data["is_skipped_due_to_leave"] = False
    if checklist_has_field("delete_reason"):
        data["delete_reason"] = ""
    if checklist_has_field("skip_reason"):
        data["skip_reason"] = ""
    return data