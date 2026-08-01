# apps/tasks/services/employee_task_cleanup.py
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from django.apps import apps
from django.contrib.auth import get_user_model
from django.db import models, transaction
from django.utils import timezone

from apps.tasks.models import (
    Checklist,
    ChecklistRecurringSeries,
    Delegation,
    FMS,
    HelpTicket,
)


logger = logging.getLogger(__name__)
User = get_user_model()


@dataclass
class EmployeeTaskCleanupResult:
    user_id: int
    user_email: str
    recurring_series_stopped: int = 0
    checklist_rows_deleted: int = 0
    delegation_rows_deleted: int = 0
    help_ticket_rows_deleted: int = 0
    fms_rows_deleted: int = 0
    other_model_rows_deleted: dict[str, int] = field(default_factory=dict)
    unsupported_models: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _field_names(model) -> set[str]:
    return {
        field.name
        for field in model._meta.get_fields()
        if getattr(field, "concrete", False)
    }


def _soft_delete_values(
    model,
    *,
    deleted_by=None,
    reason: str,
) -> dict[str, Any]:
    """
    Build a safe soft-delete update dictionary for any assigned-task model.

    Different task models use different lifecycle fields. This helper updates
    only fields that actually exist on the model.
    """
    names = _field_names(model)
    now = timezone.now()
    values: dict[str, Any] = {}

    if "is_deleted" in names:
        values["is_deleted"] = True

    if "is_active" in names:
        values["is_active"] = False

    if "is_skipped_due_to_leave" in names:
        # Existing list/dashboard queries already hide rows with this flag.
        values["is_skipped_due_to_leave"] = True

    if "deleted_at" in names:
        values["deleted_at"] = now

    if "delete_reason" in names:
        values["delete_reason"] = reason[:255]

    if "skip_reason" in names:
        values["skip_reason"] = reason[:255]

    if "deleted_by" in names and deleted_by is not None:
        values["deleted_by"] = deleted_by

    return values


def _soft_delete_queryset(
    queryset,
    *,
    deleted_by=None,
    reason: str,
) -> int:
    model = queryset.model
    values = _soft_delete_values(
        model,
        deleted_by=deleted_by,
        reason=reason,
    )

    if not values:
        return 0

    return int(queryset.update(**values) or 0)


def _stop_recurring_series(
    user,
    *,
    deleted_by=None,
    reason: str,
) -> int:
    """
    Permanently stop every recurring master assigned to the employee.

    Once stopped:
      - is_active=False
      - is_deleted=True
      - next_run_at=None

    Every recurrence generator must already filter on:
      is_active=True, is_deleted=False
    """
    now = timezone.now()

    queryset = (
        ChecklistRecurringSeries.objects
        .select_for_update()
        .filter(assign_to=user)
    )

    series_ids = list(queryset.values_list("id", flat=True))

    if not series_ids:
        return 0

    values: dict[str, Any] = {
        "is_active": False,
        "is_deleted": True,
        "next_run_at": None,
        "deleted_at": now,
        "delete_reason": reason[:255],
    }

    if deleted_by is not None:
        values["deleted_by"] = deleted_by

    return int(
        ChecklistRecurringSeries.objects
        .filter(id__in=series_ids)
        .update(**values)
        or 0
    )


def _cleanup_known_task_models(
    user,
    *,
    deleted_by=None,
    reason: str,
) -> dict[str, int]:
    """
    Soft-delete all known task rows assigned to the employee.

    Checklist rows are selected both directly through assign_to and through the
    recurring master, so no occurrence remains visible after employee removal.
    """
    checklist_qs = Checklist.objects.filter(
        models.Q(assign_to=user)
        | models.Q(recurring_series__assign_to=user)
    ).distinct()

    delegation_qs = Delegation.objects.filter(assign_to=user)
    help_ticket_qs = HelpTicket.objects.filter(assign_to=user)
    fms_qs = FMS.objects.filter(assign_to=user)

    return {
        "checklist": _soft_delete_queryset(
            checklist_qs,
            deleted_by=deleted_by,
            reason=reason,
        ),
        "delegation": _soft_delete_queryset(
            delegation_qs,
            deleted_by=deleted_by,
            reason=reason,
        ),
        "help_ticket": _soft_delete_queryset(
            help_ticket_qs,
            deleted_by=deleted_by,
            reason=reason,
        ),
        "fms": _soft_delete_queryset(
            fms_qs,
            deleted_by=deleted_by,
            reason=reason,
        ),
    }


def _is_fk_to_user(field) -> bool:
    if not isinstance(field, (models.ForeignKey, models.OneToOneField)):
        return False

    remote_model = getattr(field.remote_field, "model", None)

    return remote_model is User


def _cleanup_other_assigned_models(
    user,
    *,
    deleted_by=None,
    reason: str,
) -> tuple[dict[str, int], list[str]]:
    """
    Best-effort coverage for other installed models that use an `assign_to`
    ForeignKey/OneToOneField pointing to the auth user model.

    Only models with an existing soft-delete field are modified. Models with no
    supported lifecycle field are reported instead of being physically deleted.
    """
    known_models = {
        Checklist,
        ChecklistRecurringSeries,
        Delegation,
        HelpTicket,
        FMS,
    }

    changed: dict[str, int] = {}
    unsupported: list[str] = []

    for model in apps.get_models():
        if model in known_models:
            continue

        try:
            field = model._meta.get_field("assign_to")
        except Exception:
            continue

        if not _is_fk_to_user(field):
            continue

        values = _soft_delete_values(
            model,
            deleted_by=deleted_by,
            reason=reason,
        )

        label = model._meta.label

        if not values:
            unsupported.append(label)
            logger.warning(
                "Assigned-task model %s has assign_to but no supported "
                "soft-delete fields.",
                label,
            )
            continue

        try:
            count = int(
                model._default_manager
                .filter(assign_to=user)
                .update(**values)
                or 0
            )
        except Exception:
            logger.exception(
                "Could not soft-delete assigned rows for model %s and user %s",
                label,
                user.pk,
            )
            unsupported.append(label)
            continue

        if count:
            changed[label] = count

    return changed, unsupported


@transaction.atomic
def soft_delete_all_tasks_for_employee(
    user,
    *,
    deleted_by=None,
    reason: str = "Employee deactivated or deleted",
) -> dict[str, Any]:
    """
    Central employee task cleanup.

    Call this BEFORE deactivating or deleting an employee account.

    Effects:
      1. Stops every recurring master assigned to the employee.
      2. Soft-deletes every Checklist occurrence assigned to the employee.
      3. Soft-deletes Delegation, HelpTicket and FMS rows.
      4. Soft-deletes rows in other installed models that use `assign_to`
         and expose a supported soft-delete field.
      5. Does not physically delete task rows.
      6. Does not reassign tasks to another employee.
    """
    if user is None or not getattr(user, "pk", None):
        raise ValueError("A saved employee user is required.")

    locked_user = User.objects.select_for_update().get(pk=user.pk)

    cleaned_reason = (
        str(reason or "Employee deactivated or deleted").strip()
        or "Employee deactivated or deleted"
    )[:255]

    result = EmployeeTaskCleanupResult(
        user_id=locked_user.pk,
        user_email=(locked_user.email or "").strip(),
    )

    result.recurring_series_stopped = _stop_recurring_series(
        locked_user,
        deleted_by=deleted_by,
        reason=cleaned_reason,
    )

    known = _cleanup_known_task_models(
        locked_user,
        deleted_by=deleted_by,
        reason=cleaned_reason,
    )

    result.checklist_rows_deleted = known["checklist"]
    result.delegation_rows_deleted = known["delegation"]
    result.help_ticket_rows_deleted = known["help_ticket"]
    result.fms_rows_deleted = known["fms"]

    other_changed, unsupported = _cleanup_other_assigned_models(
        locked_user,
        deleted_by=deleted_by,
        reason=cleaned_reason,
    )

    result.other_model_rows_deleted = other_changed
    result.unsupported_models = unsupported

    logger.info(
        "Employee task cleanup completed: %s",
        result.to_dict(),
    )

    return result.to_dict()