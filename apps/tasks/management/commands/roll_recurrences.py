#apps\tasks\management\commands\roll_recurrences.py
from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.settings.models import Holiday
from apps.tasks.models import Checklist
from apps.tasks.recurrence_utils import RECURRING_MODES, get_next_planned_date, normalize_mode
from apps.tasks.services.checklist_lifecycle import (
    active_occurrences,
    recurring_series_q,
    series_is_permanently_deleted,
)
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
ASSIGN_ANCHOR_T = dt_time(10, 0)
DUE_T = dt_time(19, 0)


def _safe_console_text(value: object) -> str:
    try:
        return ("" if value is None else str(value)).encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        return repr(value)


def _to_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt.astimezone(IST)


def _is_holiday_or_sunday(day: date) -> bool:
    if day.weekday() == 6:
        return True
    try:
        return bool(Holiday.is_holiday(day))
    except Exception:
        return Holiday.objects.filter(date=day).exists()


def _get_user(user_id: int):
    from django.contrib.auth import get_user_model
    return get_user_model().objects.filter(id=user_id, is_active=True).first()


def _is_user_blocked_on_date_at_10am(user_id: int, day: date) -> bool:
    user = _get_user(user_id)
    if not user:
        return False
    return bool(is_user_blocked_at(user, datetime.combine(day, ASSIGN_ANCHOR_T, tzinfo=IST)))


def _push_to_next_allowed_date(user_id: int, day: date) -> date:
    current = day
    for _ in range(120):
        if not _is_holiday_or_sunday(current) and not _is_user_blocked_on_date_at_10am(user_id, current):
            return current
        current += timedelta(days=1)
    return current


class Command(BaseCommand):
    help = (
        "Backfill missed checklist recurrences using the legacy date-shift rule. "
        "Permanently deleted/inactive series are always excluded."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--user-id", type=int)

    def handle(self, *args, **opts):
        dry_run = bool(opts.get("dry_run"))
        user_id = opts.get("user_id")
        now_ist = timezone.now().astimezone(IST)

        base_qs = Checklist.objects.filter(mode__in=RECURRING_MODES)
        if user_id:
            base_qs = base_qs.filter(assign_to_id=user_id)

        seeds = (
            active_occurrences(base_qs)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        processed = 0
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN - no tasks will be created."))

        for series in seeds:
            processed += 1
            mode = normalize_mode(series["mode"])
            if mode not in RECURRING_MODES:
                continue

            frequency = max(int(series.get("frequency") or 1), 1)
            series_q = recurring_series_q(
                assign_to_id=series["assign_to_id"],
                task_name=series["task_name"],
                mode=mode,
                frequency=frequency,
                group_name=series.get("group_name"),
            )

            if series_is_permanently_deleted(series_q):
                continue

            if active_occurrences(Checklist.objects.filter(series_q, status="Pending")).exists():
                continue

            source = (
                active_occurrences(Checklist.objects.filter(series_q, status="Completed"))
                .order_by("-planned_date", "-id")
                .first()
            )
            if not source or not source.planned_date:
                continue

            next_planned = get_next_planned_date(source.planned_date, mode, frequency)
            if not next_planned:
                continue

            next_date = _to_ist(next_planned).date()
            safe_date = _push_to_next_allowed_date(series["assign_to_id"], next_date)
            if safe_date != next_date:
                next_planned = datetime.combine(safe_date, DUE_T, tzinfo=IST).astimezone(
                    timezone.get_current_timezone()
                )

            if _to_ist(next_planned).date() == now_ist.date():
                continue

            if active_occurrences(
                Checklist.objects.filter(
                    series_q,
                    status="Pending",
                    planned_date__gte=next_planned - timedelta(minutes=1),
                    planned_date__lt=next_planned + timedelta(minutes=1),
                )
            ).exists():
                continue

            if dry_run:
                created += 1
                self.stdout.write(
                    f"[DRY RUN] Would create: {series['task_name']} -> {_to_ist(next_planned):%Y-%m-%d %H:%M IST}"
                )
                continue

            try:
                with transaction.atomic():
                    if series_is_permanently_deleted(series_q):
                        continue
                    obj = Checklist.objects.create(
                        assign_by=source.assign_by,
                        task_name=source.task_name,
                        message=getattr(source, "message", "") or "",
                        assign_to=source.assign_to,
                        planned_date=next_planned,
                        priority=source.priority,
                        attachment_mandatory=source.attachment_mandatory,
                        mode=source.mode,
                        frequency=frequency,
                        time_per_task_minutes=source.time_per_task_minutes or 0,
                        remind_before_days=source.remind_before_days or 0,
                        assign_pc=source.assign_pc,
                        notify_to=source.notify_to,
                        auditor=getattr(source, "auditor", None),
                        set_reminder=source.set_reminder,
                        reminder_mode=source.reminder_mode,
                        reminder_frequency=source.reminder_frequency,
                        reminder_starting_time=source.reminder_starting_time,
                        checklist_auto_close=source.checklist_auto_close,
                        checklist_auto_close_days=source.checklist_auto_close_days or 0,
                        group_name=source.group_name,
                        actual_duration_minutes=0,
                        status="Pending",
                        is_skipped_due_to_leave=False,
                        is_deleted=False,
                        is_active=True,
                        delete_reason="",
                        skip_reason="",
                    )
                created += 1
                self.stdout.write(self.style.SUCCESS(f"Created CL-{obj.id}: {obj.task_name}"))
            except Exception as exc:
                logger.exception("Failed to roll recurrence for %s: %s", series, exc)
                self.stdout.write(self.style.ERROR(f"Failed: {series['task_name']} - {exc}"))

        message = f"Would create {created}" if dry_run else f"Created {created}"
        self.stdout.write(f"{message} task(s) from {processed} series")