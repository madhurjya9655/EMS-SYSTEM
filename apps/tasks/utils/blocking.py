# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Canonical Leave Blocking (IST)

Business rule:
- PENDING leave blocks immediately after apply.
- APPROVED leave blocks.
- REJECTED / CANCELLED leave does not block.
- FULL DAY leave blocks the entire IST calendar day(s).
- HALF DAY leave blocks ONLY the exact [start_at, end_at) interval in IST.

Public APIs:
    is_user_blocked_at(user, when_dt)                 -> bool
    is_user_blocked(user, ist_date)                   -> bool
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

TASK_BLOCKING_STATUSES = {"PENDING", "APPROVED"}


def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Convert datetime to IST.

    Naive datetime is treated as IST because leave forms store/operate
    on IST business time.
    """
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
    """
    Return an IST-aware datetime.

    If naive, assume it is already IST wall-clock time.
    """
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
    """
    Return [day_start, next_day_start) in IST.
    """
    day_start = datetime.combine(d, time.min)
    next_day_start = datetime.combine(d + timedelta(days=1), time.min)

    return _ensure_aware_ist(day_start), _ensure_aware_ist(next_day_start)


def is_user_blocked_at(user, when_dt) -> bool:
    """
    Return True if `user` is blocked for assignment/email at exact datetime.

    Production rule:
    - PENDING leave blocks immediately after apply.
    - APPROVED leave blocks.
    - REJECTED / CANCELLED leave does not block.
    - Full-day leave blocks the whole IST calendar day.
    - Half-day leave blocks only exact [start_at, end_at) window.
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
            status = str(getattr(lr, "status", "") or "").upper().strip()

            if status not in TASK_BLOCKING_STATUSES:
                continue

            s = _to_ist(getattr(lr, "start_at", None)) or getattr(lr, "start_at", None)
            e = _to_ist(getattr(lr, "end_at", None)) or getattr(lr, "end_at", None)

            if not (s and e):
                continue

            if e < s:
                s, e = e, s

            is_half_day = bool(getattr(lr, "is_half_day", False))

            if not is_half_day:
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
            user_id,
            when_dbg,
        )
        return False


def is_user_blocked(user, ist_date: date) -> bool:
    """
    Legacy date-level check using 10:00 AM IST anchor.
    """
    anchor_dt = _ensure_aware_ist(datetime.combine(ist_date, ASSIGN_ANCHOR_IST))
    return is_user_blocked_at(user, anchor_dt)


def is_user_blocked_for_task_time(user, ist_date: date, at_time_ist: time) -> bool:
    """
    Check if user is blocked on date at exact IST time.
    """
    when = _ensure_aware_ist(datetime.combine(ist_date, at_time_ist))
    return is_user_blocked_at(user, when)


__all__ = [
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
    "ASSIGN_ANCHOR_IST",
    "TASK_BLOCKING_STATUSES",
]