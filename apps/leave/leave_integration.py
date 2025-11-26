# apps/leave/leave_integration.py
from __future__ import annotations

import logging
from datetime import date as _date
from typing import Iterable

from django.apps import apps
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.utils import timezone

# We’ll listen to leave signals, if present.
try:
    from apps.leave.signals import leave_blocked, leave_unblocked
    from apps.leave.models import LeaveRequest
except Exception:  # leave app might not be ready at import-time
    leave_blocked = None
    leave_unblocked = None
    LeaveRequest = None  # type: ignore

log = logging.getLogger(__name__)

# ---- Helpers ---------------------------------------------------------------

def _task_models():
    """
    Lazily fetch task models (they may not exist in all installs).
    We only use fields that safely exist: id, assign_to, planned_date, status.
    """
    out = []
    for label in ("Checklist", "Delegation", "HelpTicket", "FMS"):
        try:
            Model = apps.get_model("tasks", label)
            if Model is not None:
                out.append(Model)
        except Exception:
            pass
    return out


def _has_field(model, name: str) -> bool:
    try:
        model._meta.get_field(name)  # type: ignore[attr-defined]
        return True
    except Exception:
        return False


def _is_datetime_field(model, name: str) -> bool:
    try:
        f = model._meta.get_field(name)
        return getattr(f, "get_internal_type", lambda: "")() in {"DateTimeField", "DateTimeFieldProxy"}
    except Exception:
        return False


def _normalize_dates(dates: Iterable[_date]) -> set[_date]:
    return {d for d in (dates or []) if isinstance(d, _date)}


# ---- Signal consumers (from leave app) ------------------------------------

def _apply_block_for_dates(employee_id: int, dates: set[_date]) -> int:
    """
    For each task model: mark tasks on those dates as skipped *if* the model has
    'is_skipped_due_to_leave'. Otherwise, we do nothing (dashboard filters can hide them).
    Returns total rows touched (best-effort).
    """
    if not dates:
        return 0

    total = 0
    for Model in _task_models():
        # must have assign_to and planned_date
        if not all(_has_field(Model, f) for f in ("assign_to", "planned_date")):
            continue

        try:
            # Build a date filter compatible with DateField or DateTimeField
            if _is_datetime_field(Model, "planned_date"):
                planned_filter = {"planned_date__date__in": dates}
            else:
                planned_filter = {"planned_date__in": dates}

            qs = Model.objects.filter(assign_to_id=employee_id, **planned_filter)
            if _has_field(Model, "status"):
                qs = qs.exclude(status__in=["Completed", "Closed", "Done", "COMPLETED", "CLOSED"])
        except Exception:
            log.exception("Leave block: query prepare failed for %s", Model.__name__)
            continue

        # If the boolean exists, set it so schedulers/UI can honor it.
        if _has_field(Model, "is_skipped_due_to_leave"):
            try:
                updated = qs.update(is_skipped_due_to_leave=True)
                total += int(updated)
            except Exception:
                log.exception("Leave block: update failed on %s", Model.__name__)
        # If the field does not exist, we don’t try to write. Dashboards must filter.
    return total


def _clear_block_for_dates(employee_id: int, dates: set[_date]) -> int:
    if not dates:
        return 0

    total = 0
    for Model in _task_models():
        if not _has_field(Model, "is_skipped_due_to_leave") or not _has_field(Model, "planned_date"):
            continue
        try:
            if _is_datetime_field(Model, "planned_date"):
                planned_filter = {"planned_date__date__in": dates}
            else:
                planned_filter = {"planned_date__in": dates}

            qs = Model.objects.filter(
                assign_to_id=employee_id,
                is_skipped_due_to_leave=True,
                **planned_filter,
            )
            updated = qs.update(is_skipped_due_to_leave=False)
            total += int(updated)
        except Exception:
            log.exception("Leave unblock: update failed on %s", Model.__name__)
    return total


if leave_blocked is not None:
    @receiver(leave_blocked)
    def _on_leave_blocked(sender, employee_id: int, dates, leave_id: int, **kwargs):
        try:
            touched = _apply_block_for_dates(employee_id, _normalize_dates(dates))
            log.info("leave_blocked: employee=%s leave=%s touched=%s", employee_id, leave_id, touched)
        except Exception:
            log.exception("leave_blocked handler failed")


if leave_unblocked is not None:
    @receiver(leave_unblocked)
    def _on_leave_unblocked(sender, employee_id: int, dates, leave_id: int, **kwargs):
        try:
            cleared = _clear_block_for_dates(employee_id, _normalize_dates(dates))
            log.info("leave_unblocked: employee=%s leave=%s cleared=%s", employee_id, leave_id, cleared)
        except Exception:
            log.exception("leave_unblocked handler failed")


# ---- Dashboard filter util --------------------------------------------------

def exclude_tasks_on_active_leave(qs, user):
    """
    Drop-in guard for any task queryset used to show “today’s” items.
    Usage:
        qs = exclude_tasks_on_active_leave(qs, request.user)

    Works even if your models don’t have 'is_skipped_due_to_leave'.
    """
    try:
        if not user or not getattr(user, "id", None):
            return qs

        today = timezone.localdate()

        # Prefer a model helper if available; otherwise fall back to service util.
        user_blocked = False
        try:
            if LeaveRequest and hasattr(LeaveRequest, "is_user_blocked_on"):
                user_blocked = bool(LeaveRequest.is_user_blocked_on(user, today))
        except Exception:
            user_blocked = False

        if not user_blocked:
            # Fallback to tasks integration helper if present.
            try:
                from apps.leave.services.tasks_integration import is_user_on_leave_for_date
                user_blocked = bool(is_user_on_leave_for_date(user, today))
            except Exception:
                user_blocked = False

        if user_blocked:
            if _has_field(qs.model, "planned_date"):
                if _is_datetime_field(qs.model, "planned_date"):
                    return qs.exclude(planned_date__date=today)
                else:
                    return qs.exclude(planned_date=today)
            # If planned_date doesn't exist, safest is to keep qs unchanged (not .none())
            # to avoid hiding unrelated items from mixed querysets.
            return qs

        # If the boolean exists, also hide flagged rows regardless of date
        if _has_field(qs.model, "is_skipped_due_to_leave"):
            return qs.exclude(is_skipped_due_to_leave=True)

        return qs
    except Exception:
        log.exception("exclude_tasks_on_active_leave failed on %s", getattr(getattr(qs, "model", None), "__name__", "QS"))
        return qs


# ---- Safety: log availability post-migrate ---------------------------------

@receiver(post_migrate)
def _report_wiring(sender, **kwargs):
    try:
        have_flag = []
        for M in _task_models():
            if _has_field(M, "is_skipped_due_to_leave"):
                have_flag.append(M.__name__)
        if have_flag:
            log.info("Leave integration: 'is_skipped_due_to_leave' present on %s", ", ".join(have_flag))
        else:
            log.info("Leave integration: running in filter-only mode (no is_skipped_due_to_leave fields).")
    except Exception:
        pass
