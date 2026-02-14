from __future__ import annotations

"""
Today-only materializer for recurring tasks.

Why this file exists
--------------------
Your generator in `apps.tasks.tasks.generate_recurring_checklists` *intentionally*
does NOT create a DUE-TODAY occurrence (it only pushes strictly-future items).
That is correct for recurrence semantics, but it leaves a gap on mornings where
a user has a daily/weekly series and no row yet exists for “today”, so neither
the dashboard nor the 10:00 AM mailer finds anything to show/send.

This module fills ONLY that gap:

  • At (or just before) 10:00 IST, create the *missing* “today” row
    for each recurring series that *should* have one today.
  • It never creates future rows (generator keeps doing that).
  • It never emails (the 10:00 mailer will pick up after creation).
  • It respects leave/anchor rules (skips users blocked at 10:00 IST).
  • It is idempotent and extremely defensive against duplicates.

Safe to call multiple times per day.
"""

from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List, Any

import pytz
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.conf import settings

from .models import Checklist
from .recurrence_utils import (
    RECURRING_MODES,
    normalize_mode,
    get_next_planned_date,  # pins to 19:00 IST for stepped date
)
from .utils import (
    _safe_console_text,
    send_checklist_assignment_to_user,  # used nowhere here (no emails), kept for parity
)
from apps.tasks.services.blocking import guard_assign  # single source of truth

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))


def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


def _assignment_anchor_for_today_10am_ist(now_ist: datetime | None = None) -> datetime:
    n = (now_ist or _now_ist())
    return IST.localize(datetime.combine(n.date(), dt_time(10, 0)))


def _series_q_tolerant(assign_to_id: int, task_name: str, mode: str, freq_norm: int, group_name: str | None):
    """
    Tolerate legacy rows where `frequency` was NULL by treating NULL as 1.
    """
    freq_set = [freq_norm, None]
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=freq_set)
    return q


def _today_row_exists(q_series) -> bool:
    """
    Does a *today-due* item already exist in this tolerant series?

    IMPORTANT:
      - We consider *any* row (Pending/Completed, skipped/not skipped) as "exists"
        for idempotency. If today's row was voided (is_skipped_due_to_leave=True),
        we MUST NOT recreate it.
    """
    today_ist = _now_ist().date()
    # We check both exact “today by date()” and a loose ±1 minute around the 19:00 pin.
    pinned_19 = IST.localize(datetime.combine(today_ist, dt_time(19, 0)))

    try:
        exists_by_date = Checklist.objects.filter(q_series).filter(planned_date__date=today_ist).exists()
    except Exception:
        exists_by_date = False

    try:
        exists_by_pin = Checklist.objects.filter(q_series).filter(
            planned_date__gte=pinned_19 - timedelta(minutes=1),
            planned_date__lt=pinned_19 + timedelta(minutes=1),
        ).exists()
    except Exception:
        exists_by_pin = False

    return bool(exists_by_date or exists_by_pin)


def _future_pending_exists(q_series) -> bool:
    """
    Defensive: if a future Pending exists in the series (including skipped ones),
    do not create another "today" row. This also prevents resurrection loops.
    """
    try:
        return Checklist.objects.filter(status="Pending").filter(q_series).filter(planned_date__gt=timezone.now()).exists()
    except Exception:
        return False


def _resolve_next_after_completed(latest_completed_dt: datetime | None, mode: str, freq: int) -> datetime | None:
    """
    Step using the canonical recurrence helper until >= now, then return the first next.
    For 'today' materialization we will later compare the .date() with today (IST).
    """
    if not latest_completed_dt:
        return None
    nxt = get_next_planned_date(latest_completed_dt, mode, freq)
    safety = 0
    now = timezone.now()
    while nxt and nxt < now and safety < 730:
        nxt = get_next_planned_date(nxt, mode, freq)
        safety += 1
    return nxt


def _create_today_from_completed(completed_obj: Checklist, next_dt: datetime, freq_norm: int) -> Checklist:
    """
    Create a “today” row by cloning fields from the latest completed occurrence.
    NOTE: Emails are NOT sent here. The 10:00 AM mailer will handle notifications.
    """
    with transaction.atomic():
        obj = Checklist.objects.create(
            assign_by=completed_obj.assign_by,
            task_name=completed_obj.task_name,
            message=completed_obj.message,
            assign_to=completed_obj.assign_to,
            planned_date=next_dt,  # already pinned to 19:00 IST by recurrence_utils
            priority=completed_obj.priority,
            attachment_mandatory=completed_obj.attachment_mandatory,
            mode=completed_obj.mode,
            frequency=freq_norm,  # normalized forward
            time_per_task_minutes=completed_obj.time_per_task_minutes,
            remind_before_days=completed_obj.remind_before_days,
            assign_pc=completed_obj.assign_pc,
            notify_to=completed_obj.notify_to,
            auditor=getattr(completed_obj, "auditor", None),
            set_reminder=completed_obj.set_reminder,
            reminder_mode=completed_obj.reminder_mode,
            reminder_frequency=completed_obj.reminder_frequency,
            reminder_starting_time=completed_obj.reminder_starting_time,
            checklist_auto_close=completed_obj.checklist_auto_close,
            checklist_auto_close_days=completed_obj.checklist_auto_close_days,
            group_name=getattr(completed_obj, "group_name", None),
            actual_duration_minutes=0,
            status="Pending",
        )
    return obj


class MaterializeResult:
    def __init__(self):
        self.created: int = 0
        self.skipped_leave: int = 0
        self.skipped_exists: int = 0
        self.skipped_no_completed: int = 0
        self.skipped_not_today: int = 0
        self.skipped_future_pending: int = 0
        self.per_user: Dict[int, int] = {}
        self.details: List[Dict[str, Any]] = []

    def add(self, user_id: int, obj_id: int | None, note: str):
        if obj_id:
            self.per_user[user_id] = self.per_user.get(user_id, 0) + 1
        self.details.append({"user_id": user_id, "created_id": obj_id, "note": note})

    def as_dict(self) -> Dict[str, Any]:
        return {
            "created": self.created,
            "skipped_leave": self.skipped_leave,
            "skipped_exists": self.skipped_exists,
            "skipped_no_completed": self.skipped_no_completed,
            "skipped_not_today": self.skipped_not_today,
            "skipped_future_pending": self.skipped_future_pending,
            "per_user": self.per_user,
            "details": self.details[:100],  # keep payload compact
        }

    # helpful when returning from views
    def __dict__(self):
        return self.as_dict()


def materialize_today_for_all(*, user_id: int | None = None, dry_run: bool = False) -> MaterializeResult:
    """
    Create missing “today” rows (19:00 IST due) for recurring series that have
    their next occurrence scheduled for *today*.

    Guards:
      • Skip if a today-row already exists (strong idempotency).
      • Skip if the assignee is blocked at 10:00 IST (full-day leave).
      • Requires at least one COMPLETED occurrence to compute the next step.
      • No emails are sent here.

    IMPORTANT:
      • If a today's row exists but is voided (is_skipped_due_to_leave=True),
        it still counts as "exists" and WILL NOT be recreated.

    Args:
        user_id: limit to a single user (optional).
        dry_run: when True, do not insert rows; only report what would happen.
    """
    res = MaterializeResult()
    now_ist = _now_ist()
    today_ist = now_ist.date()
    anchor10 = _assignment_anchor_for_today_10am_ist(now_ist)

    # Seed series from *existing* recurring checklists (any status); distinct by identity fields
    # IMPORTANT: ignore voided rows as seeds so they don’t drive series reconstruction.
    filters = {"mode__in": RECURRING_MODES}
    if hasattr(Checklist, "is_skipped_due_to_leave"):
        filters["is_skipped_due_to_leave"] = False
    if user_id:
        filters["assign_to_id"] = user_id

    seeds = (
        Checklist.objects.filter(**filters)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )

    for s in seeds:
        uid = int(s["assign_to_id"])
        mode_raw = s["mode"]
        mode = normalize_mode(mode_raw)
        if mode not in RECURRING_MODES:
            continue

        freq_norm = max(int(s.get("frequency") or 1), 1)
        q_series = _series_q_tolerant(
            assign_to_id=uid,
            task_name=s["task_name"],
            mode=mode,
            freq_norm=freq_norm,
            group_name=s.get("group_name"),
        )

        # Idempotency: already have a “today” row in this tolerant series?
        # (Counts voided rows too — do NOT recreate.)
        if _today_row_exists(q_series):
            res.skipped_exists += 1
            res.add(uid, None, f"exists:{s['task_name']}")
            continue

        # Defensive: if a future pending exists already, do not create another
        if _future_pending_exists(q_series):
            res.skipped_future_pending += 1
            res.add(uid, None, f"future_pending:{s['task_name']}")
            continue

        # Need a completed occurrence to step from (strict rule preserved)
        completed_qs = Checklist.objects.filter(status="Completed").filter(q_series)
        if hasattr(Checklist, "is_skipped_due_to_leave"):
            completed_qs = completed_qs.filter(is_skipped_due_to_leave=False)

        completed = completed_qs.order_by("-planned_date", "-id").first()
        if not completed:
            res.skipped_no_completed += 1
            res.add(uid, None, f"no_completed:{s['task_name']}")
            continue

        next_dt = _resolve_next_after_completed(getattr(completed, "planned_date", None), mode, freq_norm)
        if not next_dt or next_dt.astimezone(IST).date() != today_ist:
            res.skipped_not_today += 1
            res.add(uid, None, f"not_today:{s['task_name']}")
            continue

        # Leave-aware: if the user is blocked for assignment at 10:00 IST, skip creating today’s row
        try:
            user = completed.assign_to  # same assignee for the series
        except Exception:
            user = None

        try:
            if user and not guard_assign(user, anchor10):
                res.skipped_leave += 1
                res.add(uid, None, f"leave_blocked:{s['task_name']}")
                continue
        except Exception:
            # On guard failure, be safe and do not create to avoid polluting backlogs
            res.skipped_leave += 1
            res.add(uid, None, f"leave_guard_error:{s['task_name']}")
            continue

        # Finally, create the “today” row
        if dry_run:
            res.created += 1
            res.add(uid, 0, f"DRY:created:{s['task_name']}")
            continue

        try:
            obj = _create_today_from_completed(completed, next_dt, freq_norm)
            res.created += 1
            res.add(uid, obj.id, f"created:{s['task_name']}")
        except Exception as e:
            # Do not raise; the caller is a cron hook. Errors will be visible in logs.
            from logging import getLogger
            getLogger(__name__).error(_safe_console_text(f"[TODAY MAT] create failed for user_id={uid}: {e}"))
            # treat as skipped-not-today to stay non-fatal
            res.skipped_not_today += 1
            res.add(uid, None, f"create_failed:{s['task_name']}")

    return res
