# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Canonical Leave Blocking (IST)

Rule:
- If a leave exists with status PENDING or APPROVED, the user is BLOCKED inside its window.
- FULL DAY leave: blocks the entire IST calendar day(s) it covers.
- HALF DAY leave: blocks ONLY the exact [start_at, end_at) interval in IST.
- Rejected: never blocks.

Public APIs:
    is_user_blocked_at(user, when_dt)                 -> bool   (preferred)
    is_user_blocked(user, ist_date)                   -> bool   (legacy @ 10:00 IST anchor)
    is_user_blocked_for_task_time(user, ist_date, t)  -> bool
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Optional

from django.apps import apps
from django.db.models import Q
from django.utils import timezone

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    try:
        import pytz
        IST = pytz.timezone("Asia/Kolkata")  # type: ignore[assignment]
    except Exception:  # pragma: no cover
        IST = timezone.get_current_timezone()

logger = logging.getLogger("apps.tasks.blocking")

ASSIGN_ANCHOR_IST = time(10, 0)


def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST)
    except Exception:
        if timezone.is_naive(dt):
            try:
                return timezone.make_aware(dt, IST)
            except Exception:
                try:
                    return dt.replace(tzinfo=IST)
                except Exception:
                    return dt
        return dt


def _ensure_aware_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        try:
            return timezone.make_aware(dt, IST)
        except Exception:
            return dt.replace(tzinfo=IST)
    try:
        return dt.astimezone(IST)
    except Exception:
        return dt


def _ist_day_bounds(d: date) -> tuple[datetime, datetime]:
    day_start = datetime.combine(d, time.min)
    next_day_start = datetime.combine(d + timedelta(days=1), time.min)
    return _ensure_aware_ist(day_start), _ensure_aware_ist(next_day_start)


def is_user_blocked_at(user, when_dt) -> bool:
    """
    Return True if `user` is blocked for an assignment exactly at `when_dt` (any tz/naive allowed).

    IMPORTANT:
    - We DO NOT gate on applied_at. If leave exists, it blocks inside its window immediately.
    """
    user_id = getattr(user, "id", None)
    if not user_id:
        return False

    try:
        if isinstance(when_dt, date) and not isinstance(when_dt, datetime):
            when_dt = datetime.combine(when_dt, ASSIGN_ANCHOR_IST)

        when_ist = _ensure_aware_ist(when_dt)

        LeaveRequest = apps.get_model("leave", "LeaveRequest")
        if LeaveRequest is None:
            return False

        target_day = when_ist.date()
        day_start, next_day_start = _ist_day_bounds(target_day)

        qs = (
            LeaveRequest.objects.filter(employee_id=user_id)
            .filter(Q(start_at__lt=next_day_start) & Q(end_at__gt=day_start))
            .only("start_at", "end_at", "status", "is_half_day")
        )

        for lr in qs:
            status = str(getattr(lr, "status", "")).upper()
            if status not in ("PENDING", "APPROVED"):
                continue

            s = _to_ist(getattr(lr, "start_at", None)) or getattr(lr, "start_at", None)
            e = _to_ist(getattr(lr, "end_at", None)) or getattr(lr, "end_at", None)
            if not (s and e):
                continue

            if e < s:
                s, e = e, s

            if not bool(getattr(lr, "is_half_day", False)):
                s, e = day_start, next_day_start

            if s <= when_ist < e:
                return True

        return False

    except Exception:
        try:
            when_dbg = str(when_dt)
        except Exception:
            when_dbg = "<unprintable>"
        logger.exception(
            "is_user_blocked_at failed for user_id=%s, when=%s",
            user_id, when_dbg
        )
        return False


def is_user_blocked(user, ist_date: date) -> bool:
    anchor_dt = _ensure_aware_ist(datetime.combine(ist_date, ASSIGN_ANCHOR_IST))
    return is_user_blocked_at(user, anchor_dt)


def is_user_blocked_for_task_time(user, ist_date: date, at_time_ist: time) -> bool:
    when = _ensure_aware_ist(datetime.combine(ist_date, at_time_ist))
    return is_user_blocked_at(user, when)


__all__ = [
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
    "ASSIGN_ANCHOR_IST",
]
