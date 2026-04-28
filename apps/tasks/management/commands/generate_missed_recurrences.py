#E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\generate_missed_recurrences.py
from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from apps.settings.models import Holiday
from apps.tasks.models import Checklist
from apps.tasks.recurrence_utils import (
    RECURRING_MODES,
    get_next_planned_date,
    normalize_mode,
)
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
ASSIGN_ANCHOR_T = dt_time(10, 0)


def _safe_console_text(s: object) -> str:
    try:
        return ("" if s is None else str(s)).encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        try:
            return repr(s)
        except Exception:
            return ""


def _to_ist(dt: datetime) -> datetime:
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, tz)
    return dt.astimezone(IST)


def _is_holiday_or_sunday(d: date) -> bool:
    """
    Return True if date is Sunday or configured admin holiday.
    """
    try:
        if d.weekday() == 6:
            return True
    except Exception:
        pass

    try:
        try:
            return bool(Holiday.is_holiday(d))
        except Exception:
            return Holiday.objects.filter(date=d).exists()
    except Exception:
        logger.exception("Holiday check failed for date=%s", d)
        return False


def _get_user(user_id: int):
    from django.contrib.auth import get_user_model

    User = get_user_model()
    return User.objects.filter(id=user_id, is_active=True).first()


def _is_user_blocked_on_date_at_10am(user_id: int, d: date) -> bool:
    """
    Leave anchor for missed recurrence generation: 10:00 IST.

    New production rule:
    - PENDING leave blocks immediately after apply.
    - APPROVED leave blocks.
    - Half-day leave blocks only if it overlaps 10:00 IST.
    - Full-day leave blocks full date.
    """
    user = _get_user(user_id)
    if not user:
        return False

    anchor_ist = datetime.combine(d, ASSIGN_ANCHOR_T, tzinfo=IST)
    return bool(is_user_blocked_at(user, anchor_ist))


def _should_skip_recurring_date(user_id: int, d: date) -> tuple[bool, str | None]:
    """
    Hard missed-recurrence generation gate.

    Recurring tasks must NOT generate on:
    - Sunday
    - Admin holiday
    - Pending leave overlapping 10:00 IST
    - Approved leave overlapping 10:00 IST

    Important:
    We SKIP the occurrence. We do NOT push it to the next working day.
    """
    if _is_holiday_or_sunday(d):
        return True, "holiday_or_sunday"

    if _is_user_blocked_on_date_at_10am(user_id, d):
        return True, "leave"

    return False, None

def _series_q(
    *,
    assign_to_id: int,
    task_name: str,
    mode: str,
    frequency: int | None,
    group_name: str | None,
) -> tuple[Q, int]:
    """
    Legacy-tolerant grouping:
    - Treat NULL frequency as 1.
    - Exclude tombstoned/skipped rows so they do not participate in series logic.
    """
    freq = max(int(frequency or 1), 1)

    q = Q(
        assign_to_id=assign_to_id,
        task_name=task_name,
        mode=mode,
        is_skipped_due_to_leave=False,
    )

    if group_name:
        q &= Q(group_name=group_name)

    q &= Q(frequency__in=[freq, None])

    return q, freq


class Command(BaseCommand):
    help = (
        "Backfill missed recurrences (Checklist) WITHOUT violating strict task-blocking rules.\n"
        "For each series, if there is NO Pending item and there IS a Completed item, create the next occurrence.\n"
        "STRICT RULES:\n"
        "• Recurring task occurrence is SKIPPED if planned date is Sunday, admin holiday, or approved leave.\n"
        "• This command DOES NOT push invalid occurrences to the next allowed date.\n"
        "• This command does NOT send checklist emails. Consolidated 10:00 IST digest handles notifications."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show actions without writing to DB.")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee user id.")

    def handle(self, *args, **opts):
        dry_run = bool(opts.get("dry_run", False))
        user_id = opts.get("user_id")

        now = timezone.now()
        now_ist = now.astimezone(IST)

        filters = {
            "mode__in": RECURRING_MODES,
            "is_skipped_due_to_leave": False,
        }

        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        processed = 0

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created."))

        for s in seeds:
            processed += 1

            mode_norm = normalize_mode(s["mode"])

            if mode_norm not in RECURRING_MODES:
                continue

            q_series, freq_norm = _series_q(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=mode_norm,
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            # Completion-gated rule:
            # If any pending occurrence exists in this series, do not generate another.
            if Checklist.objects.filter(status="Pending").filter(q_series).exists():
                continue

            base = (
                Checklist.objects.filter(status="Completed")
                .filter(q_series)
                .order_by("-planned_date", "-id")
                .first()
            )

            if not base or not base.planned_date:
                continue

            next_planned = get_next_planned_date(base.planned_date, mode_norm, freq_norm)

            if not next_planned:
                continue

            try:
                next_date_ist = _to_ist(next_planned).date()
            except Exception:
                logger.exception(
                    "Could not convert next_planned to IST for series=%s next_planned=%s",
                    s,
                    next_planned,
                )
                continue

            # HARD BUSINESS RULE:
            # Do not generate missed recurring tasks on Sunday/holiday/approved leave.
            # Do not push the occurrence to another day.
            should_skip, skip_reason = _should_skip_recurring_date(
                user_id=s["assign_to_id"],
                d=next_date_ist,
            )

            if should_skip:
                logger.info(
                    _safe_console_text(
                        f"[MISSED] Skipped '{s['task_name']}' for user_id={s['assign_to_id']} "
                        f"on {next_date_ist}: {skip_reason}"
                    )
                )
                continue

            # Existing safety:
            # Suppress today creation because today reminders/visibility are handled separately.
            try:
                if _to_ist(next_planned).date() == now_ist.date():
                    logger.info(
                        _safe_console_text(
                            f"[MISSED] Suppressed TODAY creation for series '{s['task_name']}' "
                            f"user_id={s['assign_to_id']} @ {_to_ist(next_planned):%Y-%m-%d %H:%M IST}"
                        )
                    )
                    continue
            except Exception:
                logger.exception("Today suppression check failed for series=%s", s)
                continue

            dupe = (
                Checklist.objects.filter(status="Pending")
                .filter(q_series)
                .filter(
                    planned_date__gte=next_planned - timedelta(minutes=1),
                    planned_date__lt=next_planned + timedelta(minutes=1),
                )
                .exists()
            )

            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(
                    f"[DRY RUN] Would create: {s['task_name']} → {_to_ist(next_planned):%Y-%m-%d %H:%M IST}"
                )
                continue

            try:
                with transaction.atomic():
                    obj = Checklist.objects.create(
                        assign_by=base.assign_by,
                        task_name=base.task_name,
                        message=getattr(base, "message", "") or "",
                        assign_to=base.assign_to,
                        planned_date=next_planned,
                        priority=getattr(base, "priority", None),
                        attachment_mandatory=getattr(base, "attachment_mandatory", False),
                        mode=base.mode,
                        frequency=freq_norm,
                        time_per_task_minutes=getattr(base, "time_per_task_minutes", 0) or 0,
                        remind_before_days=getattr(base, "remind_before_days", 0) or 0,
                        assign_pc=getattr(base, "assign_pc", None),
                        notify_to=getattr(base, "notify_to", None),
                        auditor=getattr(base, "auditor", None),
                        set_reminder=getattr(base, "set_reminder", False),
                        reminder_mode=getattr(base, "reminder_mode", None),
                        reminder_frequency=getattr(base, "reminder_frequency", None),
                        reminder_starting_time=getattr(base, "reminder_starting_time", None),
                        checklist_auto_close=getattr(base, "checklist_auto_close", False),
                        checklist_auto_close_days=getattr(base, "checklist_auto_close_days", 0) or 0,
                        group_name=getattr(base, "group_name", None),
                        actual_duration_minutes=0,
                        status="Pending",
                        is_skipped_due_to_leave=False,
                    )

                created += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ Created: CL-{obj.id} '{obj.task_name}' @ {_to_ist(obj.planned_date):%Y-%m-%d %H:%M IST}"
                    )
                )

            except Exception as e:
                logger.exception("Failed to create missed recurrence for %s: %s", s, e)
                self.stdout.write(self.style.ERROR(f"❌ Failed: {s['task_name']} - {e}"))

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"[DRY RUN] Would create {created} task(s) from {processed} series"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Created {created} task(s) from {processed} series"
                )
            )

        if created == 0:
            self.stdout.write("No missed recurrences needed to be created.")