# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\generate_missed_recurrences.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date
from typing import Optional

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from apps.settings.models import Holiday
from apps.tasks.models import Checklist

# ✅ FINAL recurrence math: step date by mode/frequency and PIN to 19:00 IST.
#    (No holiday shift inside.)
from apps.tasks.recurrence_utils import (
    normalize_mode,
    RECURRING_MODES,
    get_next_planned_date,
)

# ✅ Unified leave source-of-truth (time-aware)
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# NOTE: We intentionally DO NOT send checklist emails from this command.
# Consolidated checklist emails are sent at 10:00 IST by apps/tasks/tasks.py::send_due_today_assignments
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)

ASSIGN_ANCHOR_T = dt_time(10, 0)
DUE_T = dt_time(19, 0)


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
    try:
        if d.weekday() == 6:
            return True
    except Exception:
        pass
    try:
        return Holiday.objects.filter(date=d).exists()
    except Exception:
        return False


def _get_user(user_id: int):
    from django.contrib.auth import get_user_model
    User = get_user_model()
    return User.objects.filter(id=user_id, is_active=True).first()


def _is_user_blocked_on_date_at_10am(user_id: int, d: date) -> bool:
    """
    Leave anchor for "day-of visibility" decisions: 10:00 IST.
    Full-day leave blocks the day, half-day blocks only if overlapping 10:00 IST.
    """
    user = _get_user(user_id)
    if not user:
        return False
    anchor_ist = IST.localize(datetime.combine(d, ASSIGN_ANCHOR_T))
    return bool(is_user_blocked_at(user, anchor_ist))


def _push_to_next_allowed_date(user_id: int, d: date) -> date:
    """
    Advance until:
      - not Sunday/holiday
      - not blocked by leave at 10:00 IST
    """
    cur = d
    for _ in range(0, 120):
        if (not _is_holiday_or_sunday(cur)) and (not _is_user_blocked_on_date_at_10am(user_id, cur)):
            return cur
        cur += timedelta(days=1)
    return cur


def _series_q(
    *,
    assign_to_id: int,
    task_name: str,
    mode: str,
    frequency: int | None,
    group_name: str | None,
) -> tuple[Q, int]:
    """
    Legacy-tolerant grouping: treat NULL frequency as 1.
    Excludes tombstoned rows (is_skipped_due_to_leave=True) so they don't participate in series logic.
    """
    freq = max(int(frequency or 1), 1)
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode, is_skipped_due_to_leave=False)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq, None])
    return q, freq


class Command(BaseCommand):
    help = (
        "Backfill missed recurrences (Checklist) WITHOUT violating the rule: next spawns ONLY after completion.\n"
        "For each series, if there is NO Pending item and there IS a Completed item, create the next at 19:00 IST.\n"
        "This command shifts the next date off Sunday/holidays and off assignee leave (leave check @ 10:00 IST).\n"
        "IMPORTANT: This command does NOT send checklist emails. Consolidated 10:00 IST digest handles notifications."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show actions without writing to DB")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id)")

    def handle(self, *args, **opts):
        dry_run = bool(opts.get("dry_run", False))
        user_id = opts.get("user_id")

        now = timezone.now()
        now_ist = now.astimezone(IST)

        filters = {"mode__in": RECURRING_MODES, "is_skipped_due_to_leave": False}
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

            # Golden rule #1: If ANY Pending exists → do not create future
            if Checklist.objects.filter(status="Pending").filter(q_series).exists():
                continue

            # Golden rule #2: Step only from the latest COMPLETED
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

            # Shift off Sunday/holiday + leave (10AM anchor)
            next_date_ist = _to_ist(next_planned).date()
            safe_date = _push_to_next_allowed_date(s["assign_to_id"], next_date_ist)
            if safe_date != next_date_ist:
                next_planned = IST.localize(datetime.combine(safe_date, DUE_T)).astimezone(
                    timezone.get_current_timezone()
                )

            # IMPORTANT: Don't create a "today" occurrence here.
            # Today's due items are produced by completion-driven signals/pre-10 flow;
            # re-creating today here can resurrect deleted tasks.
            try:
                if _to_ist(next_planned).date() == now_ist.date():
                    logger.info(_safe_console_text(
                        f"[MISSED] Suppressed TODAY creation for series '{s['task_name']}' "
                        f"(user_id={s['assign_to_id']}) @ {_to_ist(next_planned):%Y-%m-%d %H:%M IST}"
                    ))
                    continue
            except Exception:
                continue

            # Dupe guard within ±1 minute inside tolerant series
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
                        frequency=freq_norm,  # normalize going forward
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
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} task(s) from {processed} series"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} task(s) from {processed} series"))
        if created == 0:
            self.stdout.write("No missed recurrences needed to be created.")
