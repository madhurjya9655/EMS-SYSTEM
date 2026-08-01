# apps/tasks/services/checklist_series_creation.py
from __future__ import annotations

from datetime import datetime

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, ChecklistRecurringSeries
from apps.tasks.recurrence_utils import (
    RECURRING_MODES,
    normalize_mode,
)
from apps.tasks.services.blocking import guard_assign
from apps.tasks.services.holiday_guard import is_holiday_for_user
from apps.tasks.services.recurring_series import (
    calculate_next_run,
    create_occurrence_from_series,
)


def _aware(value: datetime) -> datetime:
    if timezone.is_naive(value):
        return timezone.make_aware(
            value,
            timezone.get_current_timezone(),
        )

    return value


def _validate_initial_occurrence(
    *,
    assign_to,
    planned_date: datetime,
) -> None:
    planned_date = _aware(planned_date)

    try:
        holiday_blocked = bool(
            is_holiday_for_user(
                assign_to,
                planned_date,
            )
        )
    except Exception as exc:
        raise ValidationError(
            "Holiday validation failed. "
            "The recurring checklist was not created."
        ) from exc

    if holiday_blocked:
        raise ValidationError(
            "The first recurring occurrence cannot be created "
            "on a Sunday or configured holiday."
        )

    try:
        available = bool(
            guard_assign(
                assign_to,
                planned_date,
            )
        )
    except Exception as exc:
        raise ValidationError(
            "Leave validation failed. "
            "The recurring checklist was not created."
        ) from exc

    if not available:
        raise ValidationError(
            "The assignee is on leave during the first planned occurrence."
        )


@transaction.atomic
def create_recurring_checklist(
    *,
    assign_by,
    assign_to,
    task_name,
    planned_date,
    mode,
    frequency=1,
    group_name="",
    message="",
    priority="Low",
    attachment_mandatory=False,
    recurrence_end_date=None,
    time_per_task_minutes=0,
    remind_before_days=0,
    assign_pc=None,
    notify_to=None,
    auditor=None,
    set_reminder=False,
    reminder_mode=None,
    reminder_frequency=None,
    reminder_starting_time=None,
    checklist_auto_close=False,
    checklist_auto_close_days=0,
) -> tuple[ChecklistRecurringSeries, Checklist]:
    """
    Create one durable recurring-series master and its first occurrence.

    This is the only approved creation path for new recurring Checklists.
    """
    if assign_to is None:
        raise ValidationError(
            "An assignee is required."
        )

    if assign_by is None:
        raise ValidationError(
            "An assigner is required."
        )

    task_name = str(task_name or "").strip()

    if not task_name:
        raise ValidationError(
            "Task name is required."
        )

    if planned_date is None:
        raise ValidationError(
            "The first planned date is required."
        )

    planned_date = _aware(planned_date)
    normalized_mode = normalize_mode(mode)

    if normalized_mode not in RECURRING_MODES:
        raise ValidationError(
            f"Unsupported recurring mode: {mode!r}."
        )

    try:
        normalized_frequency = max(
            int(frequency or 1),
            1,
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            "Frequency must be a positive integer."
        ) from exc

    group_name = str(group_name or "").strip()

    if (
        recurrence_end_date is not None
        and timezone.localtime(planned_date).date()
        > recurrence_end_date
    ):
        raise ValidationError(
            "The first planned date cannot be after "
            "the recurrence end date."
        )

    _validate_initial_occurrence(
        assign_to=assign_to,
        planned_date=planned_date,
    )

    existing_live_series = (
        ChecklistRecurringSeries.objects
        .select_for_update()
        .filter(
            assign_to=assign_to,
            task_name=task_name,
            mode=normalized_mode,
            frequency=normalized_frequency,
            group_name=group_name,
            is_active=True,
            is_deleted=False,
        )
        .first()
    )

    if existing_live_series is not None:
        raise ValidationError(
            "An active recurring checklist series with the same "
            "assignee, task name, mode, frequency and group already exists."
        )

    series = ChecklistRecurringSeries.objects.create(
        assign_by=assign_by,
        assign_to=assign_to,
        task_name=task_name,
        message=message or "",
        mode=normalized_mode,
        frequency=normalized_frequency,
        group_name=group_name,
        first_planned_date=planned_date,
        next_run_at=None,
        recurrence_end_date=recurrence_end_date,
        priority=priority or "Low",
        attachment_mandatory=bool(
            attachment_mandatory
        ),
        time_per_task_minutes=(
            time_per_task_minutes or 0
        ),
        remind_before_days=(
            remind_before_days or 0
        ),
        assign_pc=assign_pc,
        notify_to=notify_to,
        auditor=auditor,
        set_reminder=bool(set_reminder),
        reminder_mode=reminder_mode,
        reminder_frequency=reminder_frequency,
        reminder_starting_time=(
            reminder_starting_time
        ),
        checklist_auto_close=bool(
            checklist_auto_close
        ),
        checklist_auto_close_days=(
            checklist_auto_close_days or 0
        ),
        is_active=True,
        is_deleted=False,
        delete_reason="",
    )

    first_occurrence = create_occurrence_from_series(
        series,
        planned_date,
    )

    next_run = calculate_next_run(
        series,
        first_occurrence.planned_date,
    )

    series.next_run_at = next_run

    if next_run is None:
        series.is_active = False

    series.save(
        update_fields=[
            "next_run_at",
            "is_active",
            "updated_at",
        ]
    )

    return series, first_occurrence