# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Single source of truth to decide if a user is BLOCKED by leave.

Rules summary (IST-based):
- APPROVED leave → blocks when the checked instant falls inside the leave window.
- PENDING leave → blocks when the checked instant falls inside the leave window AND:
    • the leave was applied before that calendar day, OR
    • it was applied on the same day at/before 09:30 IST.
- REJECTED never blocks.

APIs:
    is_user_blocked_at(user, when_dt_ist)       # time-aware (preferred for exact checks)
    is_user_blocked(user, ist_date)             # date-level; checks the anchor time (10:00 IST)
    is_user_blocked_for_task_time(user, ist_date, at_time_ist)  # date + custom time

Notes:
- For **full-day** leave the entire date will be blocked at any time checked.
- For **half-day** leave only the portion that overlaps the *checked time* will block.
  E.g., a 13:00–18:00 leave won't block a 10:00 recurring assignment.
"""

from datetime import date, datetime, time, timedelta
from typing import Optional
import logging

from django.apps import apps
from django.utils import timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

IST = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.get_current_timezone()
logger = logging.getLogger("apps.tasks.blocking")

# Default “assignment anchor” used by is_user_blocked(…date…)
ASSIGN_ANCHOR_IST = time(10, 0)        # 10:00 IST
SAME_DAY_PENDING_GATE_IST = time(9, 30)  # 09:30 IST


# ------------------------- IST helpers -------------------------
def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST)
    except Exception:
        # best-effort fallback
        if timezone.is_naive(dt):
            try:
                return dt.replace(tzinfo=IST)
            except Exception:
                return dt
        return dt


def _ist_date_from_dt(dt: datetime) -> date:
    local = _to_ist(dt) or dt
    if timezone.is_aware(local):
        return local.date()
    return local.replace(tzinfo=IST).date()


def _inside_inclusive_window(when_ist: datetime, start_at: datetime, end_at: datetime) -> bool:
    """
    Check if 'when_ist' lies inside [start_at, end_at) in IST (end exclusive).
    """
    s = _to_ist(start_at) or start_at
    e = _to_ist(end_at) or end_at
    if e < s:
        s, e = e, s
    return s <= when_ist < e


# ------------------------- rule helpers -------------------------
def _pending_counts_for_instant(applied_at: Optional[datetime], target_instant_ist: datetime) -> bool:
    """
    Pending leaves block the instant if:
      • it lies in the leave window, AND
      • either the apply date is before that day, OR
      • the apply date is the SAME day and apply time was <= 09:30 IST.
    """
    if not applied_at:
        return False
    a = _to_ist(applied_at)
    if not a:
        return False

    target_day = target_instant_ist.date()

    if a.date() < target_day:
        return True  # applied earlier → counts
    if a.date() > target_day:
        return False  # applied after the day → does not count

    # same-day gate: must be <= 09:30 IST
    return a.time() <= SAME_DAY_PENDING_GATE_IST


# ----------------------------- public -----------------------------
def is_user_blocked_at(user, when_dt_ist: datetime) -> bool:
    """
    Return True if `user` is blocked for an assignment that would happen exactly at `when_dt_ist` (IST).
    This is **time-aware**: half-day leaves only block when the instant falls inside the leave window.
    """
    if not getattr(user, "id", None):
        return False

    # Ensure 'when_dt_ist' is timezone-aware in IST
    if timezone.is_naive(when_dt_ist):
        when_dt_ist = when_dt_ist.replace(tzinfo=IST)
    else:
        when_dt_ist = when_dt_ist.astimezone(IST)

    try:
        LeaveRequest = apps.get_model("leave", "LeaveRequest")

        # Narrow the DB query to leaves overlapping the checked *day*
        target_day = when_dt_ist.date()
        start_floor = datetime.combine(target_day, time.min).replace(tzinfo=IST)
        end_ceiling = datetime.combine(target_day + timedelta(days=1), time.min).replace(tzinfo=IST)

        qs = (
            LeaveRequest.objects.filter(employee=user)
            .filter(start_at__lt=end_ceiling, end_at__gt=start_floor)
            .only("start_at", "end_at", "status", "applied_at")
        )

        for lr in qs:
            # Instant must lie inside the leave window to even consider blocking
            if not _inside_inclusive_window(when_dt_ist, lr.start_at, lr.end_at):
                continue

            status = str(getattr(lr, "status", "")).upper()

            if status == "APPROVED":
                return True

            if status == "PENDING":
                if _pending_counts_for_instant(getattr(lr, "applied_at", None), when_dt_ist):
                    return True

        return False

    except Exception:
        # Never break assignment flows on unexpected issues.
        logger.exception(
            "is_user_blocked_at failed for user_id=%s, when_dt_ist=%s",
            getattr(user, "id", None), when_dt_ist
        )
        return False


def is_user_blocked(user, ist_date: date) -> bool:
    """
    Backwards-compatible, date-level API.

    We check blocking at the **assignment anchor** time (default 10:00 IST) of the given IST calendar date.
    - Full-day leaves will block.
    - Half-day leaves will block **only if** their window overlaps the anchor time.

    If you assign tasks at other times, prefer `is_user_blocked_at`.
    """
    anchor_dt = datetime.combine(ist_date, ASSIGN_ANCHOR_IST).replace(tzinfo=IST)
    return is_user_blocked_at(user, anchor_dt)


def is_user_blocked_for_task_time(user, ist_date: date, at_time_ist: time) -> bool:
    """
    Convenience function: date + custom time (IST).
    """
    when = datetime.combine(ist_date, at_time_ist).replace(tzinfo=IST)
    return is_user_blocked_at(user, when)


__all__ = [
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
    "ASSIGN_ANCHOR_IST",
    "SAME_DAY_PENDING_GATE_IST",
]
