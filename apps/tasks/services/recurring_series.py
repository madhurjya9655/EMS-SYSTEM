# apps/tasks/services/recurring_series.py
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, ChecklistRecurringSeries
from apps.tasks.recurrence_utils import get_next_planned_date
from apps.tasks.services.blocking import guard_assign
from apps.tasks.services.holiday_guard import (
    get_holiday_status,
    is_holiday_for_user,
)


logger = logging.getLogger(__name__)

MAX_ADVANCE_STEPS = 730


@dataclass(frozen=True)
class GenerationResult:
    series_id: int
    created: bool
    occurrence_id: Optional[int]
    reason: str
    planned_date: Optional[str] = None
    skipped_steps: int = 0


def _aware(value: datetime) -> datetime:
    """
    Return an aware datetime using the project timezone for naive values.
    """
    if timezone.is_naive(value):
        return timezone.make_aware(
            value,
            timezone.get_current_timezone(),
        )

    return value


def _local_date(value: datetime):
    return timezone.localtime(_aware(value)).date()


def _date_after_end(
    series: ChecklistRecurringSeries,
    value: datetime,
) -> bool:
    if series.recurrence_end_date is None:
        return False

    return _local_date(value) > series.recurrence_end_date


def _assignee_is_available(
    series: ChecklistRecurringSeries,
    planned_dt: datetime,
) -> bool:
    """
    Return True only when the occurrence can safely be assigned.

    Fail closed:
    if holiday or leave validation fails, no occurrence is created.
    """
    planned_dt = _aware(planned_dt)

    try:
        if is_holiday_for_user(series.assign_to, planned_dt):
            return False
    except Exception:
        logger.exception(
            "Holiday validation failed for recurring series %s",
            series.pk,
        )
        return False

    try:
        return bool(
            guard_assign(
                series.assign_to,
                planned_dt,
            )
        )
    except Exception:
        logger.exception(
            "Leave validation failed for recurring series %s",
            series.pk,
        )
        return False


def calculate_next_run(
    series: ChecklistRecurringSeries,
    from_dt: datetime,
) -> Optional[datetime]:
    """
    Calculate the next recurrence step.

    This function calculates only the recurrence date. It does not shift
    holidays, Sundays or leave-blocked dates.
    """
    if from_dt is None:
        return None

    try:
        frequency = max(int(series.frequency or 1), 1)
    except (TypeError, ValueError):
        frequency = 1

    candidate = get_next_planned_date(
        _aware(from_dt),
        series.mode,
        frequency,
    )

    if candidate is None:
        return None

    candidate = _aware(candidate)

    if _date_after_end(series, candidate):
        return None

    return candidate


def create_occurrence_from_series(
    series: ChecklistRecurringSeries,
    planned_dt: datetime,
) -> Checklist:
    """
    Create one Checklist occurrence from a validated series master.

    Callers should lock the master with select_for_update before invoking this
    function when generation can happen concurrently.
    """
    if series.is_deleted or not series.is_active:
        raise ValueError(
            f"Recurring series {series.pk} is inactive or deleted."
        )

    if not series.assign_to_id or not series.assign_to.is_active:
        raise ValueError(
            f"Recurring series {series.pk} has no active assignee."
        )

    planned_dt = _aware(planned_dt)

    return Checklist.objects.create(
        recurring_series=series,
        assign_by=series.assign_by,
        task_name=series.task_name,
        message=series.message or "",
        assign_to=series.assign_to,
        planned_date=planned_dt,
        priority=series.priority,
        attachment_mandatory=series.attachment_mandatory,
        mode=series.mode,
        frequency=max(int(series.frequency or 1), 1),
        recurrence_end_date=series.recurrence_end_date,
        time_per_task_minutes=series.time_per_task_minutes or 0,
        remind_before_days=series.remind_before_days or 0,
        assign_pc=series.assign_pc,
        group_name=series.group_name or "",
        notify_to=series.notify_to,
        auditor=series.auditor,
        set_reminder=series.set_reminder,
        reminder_mode=series.reminder_mode,
        reminder_frequency=series.reminder_frequency,
        reminder_starting_time=series.reminder_starting_time,
        checklist_auto_close=series.checklist_auto_close,
        checklist_auto_close_days=(
            series.checklist_auto_close_days or 0
        ),
        actual_duration_minutes=0,
        status="Pending",
        is_skipped_due_to_leave=False,
        is_deleted=False,
        is_active=True,
        delete_reason="",
        skip_reason="",
    )


def _active_pending_exists(
    series: ChecklistRecurringSeries,
) -> bool:
    return Checklist.objects.filter(
        recurring_series=series,
        status="Pending",
        is_deleted=False,
        is_active=True,
        is_skipped_due_to_leave=False,
    ).exists()


def _latest_completed(
    series: ChecklistRecurringSeries,
) -> Optional[Checklist]:
    return (
        Checklist.objects
        .filter(
            recurring_series=series,
            status="Completed",
            is_deleted=False,
            is_active=True,
            is_skipped_due_to_leave=False,
        )
        .order_by("-planned_date", "-id")
        .first()
    )


def _occurrence_exists(
    series: ChecklistRecurringSeries,
    planned_dt: datetime,
) -> bool:
    """
    Treat any occurrence on the calculated datetime as already consumed.

    Deleted or skipped rows are included intentionally. This prevents a
    deleted occurrence from being resurrected for the same recurrence date.
    """
    return Checklist.objects.filter(
        recurring_series=series,
        planned_date=_aware(planned_dt),
    ).exists()


def _initial_candidate(
    series: ChecklistRecurringSeries,
    completed: Checklist,
) -> Optional[datetime]:
    """
    Determine where generation should resume.

    next_run_at remains authoritative when it is later than the latest
    completed occurrence. This is important after a holiday/leave occurrence
    was skipped because no Checklist row exists for that skipped date.
    """
    completed_dt = _aware(completed.planned_date)

    if series.next_run_at is not None:
        stored_candidate = _aware(series.next_run_at)

        if stored_candidate > completed_dt:
            if _date_after_end(series, stored_candidate):
                return None

            return stored_candidate

    return calculate_next_run(
        series,
        completed_dt,
    )


def _find_next_creatable_candidate(
    series: ChecklistRecurringSeries,
    initial_candidate: datetime,
) -> tuple[Optional[datetime], int, str]:
    """
    Advance until a valid future occurrence is found.

    Rules:

    - past recurrence dates are consumed and advanced;
    - an existing row consumes that recurrence date;
    - holidays, Sundays and leave-blocked dates are skipped;
    - invalid dates are never shifted to an arbitrary working day;
    - each advance follows the configured recurrence interval.
    """
    candidate = _aware(initial_candidate)
    now = timezone.now()
    skipped_steps = 0
    last_reason = "candidate_ready"

    for _ in range(MAX_ADVANCE_STEPS):
        if _date_after_end(series, candidate):
            return None, skipped_steps, "recurrence_finished"

        if candidate <= now:
            skipped_steps += 1
            last_reason = "past_occurrence_advanced"

            candidate = calculate_next_run(
                series,
                candidate,
            )

            if candidate is None:
                return None, skipped_steps, "recurrence_finished"

            continue

        if _occurrence_exists(series, candidate):
            skipped_steps += 1
            last_reason = "existing_occurrence_advanced"

            candidate = calculate_next_run(
                series,
                candidate,
            )

            if candidate is None:
                return None, skipped_steps, "recurrence_finished"

            continue

        if not _assignee_is_available(series, candidate):
            skipped_steps += 1
            last_reason = "holiday_or_leave_advanced"

            candidate = calculate_next_run(
                series,
                candidate,
            )

            if candidate is None:
                return None, skipped_steps, "recurrence_finished"

            continue

        return candidate, skipped_steps, last_reason

    logger.error(
        "Maximum recurrence advancement reached for series_id=%s",
        series.pk,
    )

    return None, skipped_steps, "advance_limit_reached"


def _save_series_schedule(
    series: ChecklistRecurringSeries,
    *,
    next_run_at: Optional[datetime],
    is_active: Optional[bool] = None,
) -> None:
    series.next_run_at = next_run_at

    update_fields = [
        "next_run_at",
        "updated_at",
    ]

    if is_active is not None:
        series.is_active = is_active
        update_fields.append("is_active")

    series.save(update_fields=update_fields)


@transaction.atomic
def generate_one_series(
    series_id: int,
    *,
    dry_run: bool = False,
) -> GenerationResult:
    """
    Generate at most one active future occurrence for one series.

    The series master is locked for the full decision and creation transaction.
    """
    try:
        # Lock only the recurring-series master row.
        #
        # Do not use select_related() on this locked query. Nullable related
        # user fields create outer joins, and PostgreSQL does not allow
        # FOR UPDATE on the nullable side of an outer join.
        series = (
            ChecklistRecurringSeries.objects
            .select_for_update()
            .get(pk=series_id)
        )
    except ChecklistRecurringSeries.DoesNotExist:
        return GenerationResult(
            series_id=series_id,
            created=False,
            occurrence_id=None,
            reason="series_not_found",
        )

    if series.is_deleted or not series.is_active:
        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="inactive_or_deleted",
        )

    # A recurring series assigned to a deactivated employee must never create
    # another occurrence. Stop the master permanently when encountered.
    if not series.assign_to_id or not series.assign_to.is_active:
        if not dry_run:
            series.is_active = False
            series.is_deleted = True
            series.next_run_at = None
            series.save(
                update_fields=[
                    "is_active",
                    "is_deleted",
                    "next_run_at",
                    "updated_at",
                ]
            )

        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="inactive_assignee",
        )

    if _active_pending_exists(series):
        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="pending_exists",
        )

    completed = _latest_completed(series)

    if completed is None or completed.planned_date is None:
        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="no_completed_source",
        )

    initial_candidate = _initial_candidate(
        series,
        completed,
    )

    if initial_candidate is None:
        if not dry_run:
            _save_series_schedule(
                series,
                next_run_at=None,
                is_active=False,
            )

        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="recurrence_finished",
        )

    candidate, skipped_steps, advance_reason = (
        _find_next_creatable_candidate(
            series,
            initial_candidate,
        )
    )

    if candidate is None:
        if not dry_run:
            _save_series_schedule(
                series,
                next_run_at=None,
                is_active=False,
            )

        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason=advance_reason,
            skipped_steps=skipped_steps,
        )

    if dry_run:
        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="dry_run_would_create",
            planned_date=candidate.isoformat(),
            skipped_steps=skipped_steps,
        )

    # Final duplicate check while the series master remains locked.
    if _occurrence_exists(series, candidate):
        next_run = calculate_next_run(
            series,
            candidate,
        )

        _save_series_schedule(
            series,
            next_run_at=next_run,
            is_active=next_run is not None,
        )

        return GenerationResult(
            series_id=series.id,
            created=False,
            occurrence_id=None,
            reason="duplicate_advanced",
            planned_date=candidate.isoformat(),
            skipped_steps=skipped_steps + 1,
        )

    occurrence = create_occurrence_from_series(
        series,
        candidate,
    )

    next_run = calculate_next_run(
        series,
        candidate,
    )

    _save_series_schedule(
        series,
        next_run_at=next_run,
        is_active=next_run is not None,
    )

    return GenerationResult(
        series_id=series.id,
        created=True,
        occurrence_id=occurrence.id,
        reason="created",
        planned_date=candidate.isoformat(),
        skipped_steps=skipped_steps,
    )


def generate_due_series(
    *,
    user_id: int | None = None,
    dry_run: bool = False,
    limit: int = 1000,
) -> dict:
    """
    Inspect active recurring-series masters and generate missing occurrences.

    Holiday Calendar rule:
    - The current IST date is checked once before any recurring-series query.
    - On an official holiday or Sunday, no series is inspected.
    - No Checklist occurrence is created.
    - No next_run_at value is changed.
    - No existing Checklist row is changed.

    All active masters are inspected on working days instead of filtering only
    by next_run_at <= now. This preserves the existing completion-gated
    recurrence behavior.
    """
    now = timezone.now()
    holiday_status = get_holiday_status(now)

    if holiday_status["is_off_day"]:
        logger.info(
            "Checklist generation skipped - Official Holiday | "
            "Holiday detected: %s | Holiday Name: %s | "
            "Scheduler: generate_due_series",
            holiday_status["date"].isoformat(),
            holiday_status["holiday_name"] or "Sunday",
        )

        return {
            "checked": 0,
            "created": 0,
            "dry_run": dry_run,
            "user_id": user_id,
            "skipped": True,
            "reason": holiday_status["reason"],
            "day": holiday_status["date"].isoformat(),
            "holiday_name": holiday_status["holiday_name"],
            "results": [],
        }

    try:
        limit = max(int(limit or 1000), 1)
    except (TypeError, ValueError):
        limit = 1000

    queryset = ChecklistRecurringSeries.objects.filter(
        is_active=True,
        is_deleted=False,
        assign_to__is_active=True,
    )

    if user_id:
        queryset = queryset.filter(
            assign_to_id=user_id,
        )

    series_ids = list(
        queryset
        .order_by("next_run_at", "id")
        .values_list("id", flat=True)[:limit]
    )

    results: list[dict] = []
    created = 0

    for series_id in series_ids:
        try:
            result = generate_one_series(
                series_id,
                dry_run=dry_run,
            )

        except Exception as exc:
            logger.exception(
                "Recurring generation failed for series_id=%s",
                series_id,
            )

            result = GenerationResult(
                series_id=series_id,
                created=False,
                occurrence_id=None,
                reason=(
                    f"error:{type(exc).__name__}:"
                    f"{str(exc)[:200]}"
                ),
            )

        results.append(asdict(result))
        created += int(result.created)

    return {
        "checked": len(series_ids),
        "created": created,
        "dry_run": dry_run,
        "user_id": user_id,
        "skipped": False,
        "day": holiday_status["date"].isoformat(),
        "results": results,
    }