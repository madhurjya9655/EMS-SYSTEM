# apps/tasks/materializer.py
from __future__ import annotations

"""
Today-only ChecklistRecurringSeries materializer.

At or before the 10:00 IST notification run, this module creates a missing
Checklist occurrence whose calculated due date is today at 19:00 IST.

It uses ChecklistRecurringSeries as the only recurrence source of truth.
It never infers a series from Checklist task-name/frequency fields.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta
from typing import Any

import pytz
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, ChecklistRecurringSeries
from apps.tasks.recurrence_utils import is_working_day
from apps.tasks.services.blocking import guard_assign
from apps.tasks.services.recurring_series import (
    calculate_next_run,
    create_occurrence_from_series,
)
from apps.tasks.utils import _safe_console_text


logger = logging.getLogger(__name__)
IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))


def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


def _ttl_until_next_3am_ist(now_ist: datetime | None = None) -> int:
    current = now_ist or _now_ist()
    next_3am = (current + timedelta(days=1)).replace(
        hour=3,
        minute=0,
        second=0,
        microsecond=0,
    )
    return max(int((next_3am - current).total_seconds()), 60)


def _marker_key(day_iso: str, series_id: int) -> str:
    return f"mat:checklist_series:{series_id}:{day_iso}"


def _acquire_marker(
    day_iso: str,
    series_id: int,
    *,
    now_ist: datetime,
) -> str | None:
    key = _marker_key(day_iso, series_id)
    ttl = max(_ttl_until_next_3am_ist(now_ist), 6 * 60 * 60)

    try:
        return key if cache.add(key, True, ttl) else None
    except Exception:
        # Continue best-effort when cache is unavailable. Database locking and
        # duplicate checks still protect the create.
        return "NOLOCK"


def _release_marker(key: str | None) -> None:
    if not key or key == "NOLOCK":
        return

    try:
        cache.delete(key)
    except Exception:
        pass


def _anchor_10am_ist(day) -> datetime:
    return IST.localize(datetime.combine(day, dt_time(10, 0)))


def _to_ist(value: datetime) -> datetime:
    if timezone.is_naive(value):
        value = timezone.make_aware(
            value,
            timezone.get_current_timezone(),
        )
    return value.astimezone(IST)


@dataclass
class MaterializeResult:
    created: int = 0
    skipped_non_working_day: int = 0
    skipped_inactive: int = 0
    skipped_pending_exists: int = 0
    skipped_no_completed: int = 0
    skipped_not_today: int = 0
    skipped_leave: int = 0
    skipped_duplicate: int = 0
    skipped_marker_exists: int = 0
    failed: int = 0
    per_user: dict[int, int] = field(default_factory=dict)
    details: list[dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        *,
        series: ChecklistRecurringSeries,
        note: str,
        occurrence_id: int | None = None,
    ) -> None:
        if occurrence_id:
            self.per_user[series.assign_to_id] = (
                self.per_user.get(series.assign_to_id, 0) + 1
            )

        if len(self.details) < 100:
            self.details.append(
                {
                    "series_id": series.id,
                    "assign_to_id": series.assign_to_id,
                    "task_name": series.task_name,
                    "occurrence_id": occurrence_id,
                    "note": note,
                }
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "created": self.created,
            "skipped_non_working_day": self.skipped_non_working_day,
            "skipped_inactive": self.skipped_inactive,
            "skipped_pending_exists": self.skipped_pending_exists,
            "skipped_no_completed": self.skipped_no_completed,
            "skipped_not_today": self.skipped_not_today,
            "skipped_leave": self.skipped_leave,
            "skipped_duplicate": self.skipped_duplicate,
            "skipped_marker_exists": self.skipped_marker_exists,
            "failed": self.failed,
            "per_user": self.per_user,
            "details": self.details,
        }


def _active_pending_exists(series: ChecklistRecurringSeries) -> bool:
    return Checklist.objects.filter(
        recurring_series=series,
        status="Pending",
        is_deleted=False,
        is_active=True,
        is_skipped_due_to_leave=False,
    ).exists()


def _latest_completed(
    series: ChecklistRecurringSeries,
) -> Checklist | None:
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


def _candidate_for_today(
    series: ChecklistRecurringSeries,
    completed: Checklist,
) -> datetime | None:
    return calculate_next_run(series, completed.planned_date)


@transaction.atomic
def _materialize_one(
    *,
    series_id: int,
    today_ist,
    anchor_10am: datetime,
    dry_run: bool,
) -> tuple[str, int | None]:
    # Lock only the recurring-series row.
    #
    # Do not combine select_for_update() with select_related() here because
    # several related user fields are nullable. PostgreSQL rejects FOR UPDATE
    # when the query contains nullable outer joins.
    series = (
        ChecklistRecurringSeries.objects
        .select_for_update()
        .get(pk=series_id)
    )

    if series.is_deleted or not series.is_active:
        return "inactive_or_deleted", None

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
        return "inactive_assignee", None

    if _active_pending_exists(series):
        return "pending_exists", None

    completed = _latest_completed(series)
    if completed is None or completed.planned_date is None:
        return "no_completed_source", None

    candidate = _candidate_for_today(series, completed)
    if candidate is None:
        series.is_active = False
        series.next_run_at = None
        if not dry_run:
            series.save(
                update_fields=[
                    "is_active",
                    "next_run_at",
                    "updated_at",
                ]
            )
        return "recurrence_finished", None

    if _to_ist(candidate).date() != today_ist:
        return "not_today", None

    try:
        allowed = bool(guard_assign(series.assign_to, anchor_10am))
    except Exception:
        logger.exception(
            "Materializer leave guard failed for series_id=%s",
            series.id,
        )
        allowed = False

    if not allowed:
        if not dry_run:
            series.next_run_at = calculate_next_run(series, candidate)
            if series.next_run_at is None:
                series.is_active = False
            series.save(
                update_fields=[
                    "next_run_at",
                    "is_active",
                    "updated_at",
                ]
            )
        return "leave_blocked", None

    if Checklist.objects.filter(
        recurring_series=series,
        planned_date=candidate,
    ).exists():
        if not dry_run:
            series.next_run_at = calculate_next_run(series, candidate)
            if series.next_run_at is None:
                series.is_active = False
            series.save(
                update_fields=[
                    "next_run_at",
                    "is_active",
                    "updated_at",
                ]
            )
        return "duplicate", None

    if dry_run:
        return "dry_run_would_create", None

    occurrence = create_occurrence_from_series(series, candidate)

    series.next_run_at = calculate_next_run(series, candidate)
    if series.next_run_at is None:
        series.is_active = False
    series.save(
        update_fields=[
            "next_run_at",
            "is_active",
            "updated_at",
        ]
    )

    return "created", occurrence.id


def materialize_today_for_all(
    *,
    user_id: int | None = None,
    dry_run: bool = False,
    limit: int = 1000,
) -> MaterializeResult:
    result = MaterializeResult()
    now_ist = _now_ist()
    today_ist = now_ist.date()
    day_iso = today_ist.isoformat()

    if not is_working_day(today_ist):
        result.skipped_non_working_day = 1
        logger.info(
            _safe_console_text(
                f"[TODAY MAT] Skipped {day_iso}: Sunday or configured holiday"
            )
        )
        return result

    anchor_10am = _anchor_10am_ist(today_ist)

    qs = ChecklistRecurringSeries.objects.filter(
        is_active=True,
        is_deleted=False,
        assign_to__is_active=True,
    )

    if user_id:
        qs = qs.filter(assign_to_id=user_id)

    series_ids = list(
        qs.order_by("next_run_at", "id")
        .values_list("id", flat=True)[:limit]
    )

    for series_id in series_ids:
        try:
            series = ChecklistRecurringSeries.objects.get(pk=series_id)
        except ChecklistRecurringSeries.DoesNotExist:
            continue

        marker = None

        if not dry_run:
            marker = _acquire_marker(
                day_iso,
                series_id,
                now_ist=now_ist,
            )
            if marker is None:
                result.skipped_marker_exists += 1
                result.add(series=series, note="marker_exists")
                continue

        try:
            reason, occurrence_id = _materialize_one(
                series_id=series_id,
                today_ist=today_ist,
                anchor_10am=anchor_10am,
                dry_run=dry_run,
            )

            if reason == "created":
                result.created += 1
                result.add(
                    series=series,
                    note=reason,
                    occurrence_id=occurrence_id,
                )
                # Keep marker after successful creation.
                continue

            if reason == "dry_run_would_create":
                result.created += 1
                result.add(series=series, note=reason)
                continue

            if reason in {"inactive_or_deleted", "inactive_assignee"}:
                result.skipped_inactive += 1
            elif reason == "pending_exists":
                result.skipped_pending_exists += 1
            elif reason == "no_completed_source":
                result.skipped_no_completed += 1
            elif reason in {"not_today", "recurrence_finished"}:
                result.skipped_not_today += 1
            elif reason == "leave_blocked":
                result.skipped_leave += 1
            elif reason == "duplicate":
                result.skipped_duplicate += 1
            else:
                result.failed += 1

            result.add(series=series, note=reason)

            # Release marker for non-created states so a later legitimate retry
            # remains possible. Deleted masters are still protected by DB state.
            if marker:
                _release_marker(marker)

        except Exception as exc:
            logger.exception(
                "Today materializer failed for series_id=%s: %s",
                series_id,
                exc,
            )
            result.failed += 1
            result.add(series=series, note=f"failed:{type(exc).__name__}")
            if marker:
                _release_marker(marker)

    return result