# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Single source of truth to decide if a user is BLOCKED by leave.

✅ Non-negotiable rule (IST-based):
    From the moment a leave exists in PENDING or APPROVED, NO task of ANY type
    must be assigned/emailed/shown to that user during the leave window.

Key details:
- Timezone: Asia/Kolkata (IST). All checks are done in IST.
- Full day: blocks whole 00:00:00–23:59:59 of that date.
- Half day: blocks only the exact [start_at, end_at) time window.
- Pending vs Approved: both block.
- Rejected: never blocks.
- IMPORTANT: We **do not** gate on `applied_at`. As soon as a leave is recorded,
  any check for a time inside its window is considered blocked.

APIs:
    is_user_blocked_at(user, when_dt_ist)        # time-aware (preferred)
    is_user_blocked(user, ist_date)              # date-level @ 10:00 IST (keeps recurring rules intact)
    is_user_blocked_for_task_time(user, ist_date, at_time_ist)
"""

from datetime import date, datetime, time, timedelta
import logging
from typing import Optional

from django.apps import apps
from django.utils import timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

IST = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.get_current_timezone()
logger = logging.getLogger("apps.tasks.blocking")

# Assignment anchor for date-level checks (preserves existing 10:00 AM behavior)
ASSIGN_ANCHOR_IST = time(10, 0)


# ------------------------- IST helpers -------------------------
def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST)
    except Exception:
        if timezone.is_naive(dt):
            try:
                return dt.replace(tzinfo=IST)
            except Exception:
                return dt
        return dt


def _inside_inclusive_window(when_ist: datetime, start_at: datetime, end_at: datetime) -> bool:
    """
    True if 'when_ist' lies inside [start_at, end_at) evaluated in IST.
    """
    s = _to_ist(start_at) or start_at
    e = _to_ist(end_at) or end_at
    if e < s:
        s, e = e, s
    return s <= when_ist < e


# ----------------------------- public -----------------------------
def is_user_blocked_at(user, when_dt_ist: datetime) -> bool:
    """
    Return True if `user` is blocked for an assignment exactly at `when_dt_ist` (IST).

    Blocking logic:
      - consider only leaves overlapping the checked day
      - status in {PENDING, APPROVED}
      - 'when' must be inside the leave window
      - NOTE: we intentionally ignore `applied_at` to enforce retroactive blocking
        for any checks within the window as soon as the leave exists.
    """
    if not getattr(user, "id", None):
        return False

    # Ensure aware IST
    if timezone.is_naive(when_dt_ist):
        when_dt_ist = when_dt_ist.replace(tzinfo=IST)
    else:
        when_dt_ist = when_dt_ist.astimezone(IST)

    try:
        LeaveRequest = apps.get_model("leave", "LeaveRequest")
        if LeaveRequest is None:
            return False

        # Window for the calendar day of 'when'
        target_day = when_dt_ist.date()
        start_floor = datetime.combine(target_day, time.min).replace(tzinfo=IST)
        end_ceiling = datetime.combine(target_day + timedelta(days=1), time.min).replace(tzinfo=IST)

        qs = (
            LeaveRequest.objects.filter(employee=user)
            .filter(start_at__lt=end_ceiling, end_at__gt=start_floor)
            .only("start_at", "end_at", "status")
        )

        for lr in qs:
            status = str(getattr(lr, "status", "")).upper()
            if status not in ("PENDING", "APPROVED"):
                continue
            if _inside_inclusive_window(when_dt_ist, lr.start_at, lr.end_at):
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
    Date-level API used by existing code (recurring visibility/email at 10:00 IST).
    - Full-day leaves block.
    - Half-day leaves block only if they overlap the anchor (10:00 IST).
    """
    anchor_dt = datetime.combine(ist_date, ASSIGN_ANCHOR_IST).replace(tzinfo=IST)
    return is_user_blocked_at(user, anchor_dt)


def is_user_blocked_for_task_time(user, ist_date: date, at_time_ist: time) -> bool:
    """
    Convenience: check at a specific IST time on a given date.
    """
    when = datetime.combine(ist_date, at_time_ist).replace(tzinfo=IST)
    return is_user_blocked_at(user, when)


__all__ = [
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
    "ASSIGN_ANCHOR_IST",
]
