from __future__ import annotations

"""
Single source of truth to decide if a user is BLOCKED by leave.

✅ Non-negotiable rule (IST-based):
    From the moment a leave exists in PENDING or APPROVED, NO task of ANY type
    must be assigned/emailed/shown to that user during the leave window.

Key details:
- Timezone: Asia/Kolkata (IST). All checks are done in IST.
- Full day: blocks whole 00:00:00–23:59:59 of that date.  ⟵ enforced here
- Half day: blocks only the exact [start_at, end_at) time window.
- Pending vs Approved: both block.
- Rejected: never blocks.
- IMPORTANT: We **do not** gate on `applied_at`. As soon as a leave is recorded,
  any check for a time inside its window is considered blocked.

APIs:
    is_user_blocked_at(user, when_dt_ist)        # time-aware (preferred)
    is_user_blocked(user, ist_date)              # date-level @ 10:00 IST (legacy anchor)
    is_user_blocked_for_task_time(user, ist_date, at_time_ist)
"""

from datetime import date, datetime, time, timedelta
import logging
from typing import Optional

from django.apps import apps
from django.utils import timezone

# Robust IST tzinfo with graceful fallback
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    try:
        import pytz
        IST = pytz.timezone("Asia/Kolkata")  # type: ignore[assignment]
    except Exception:  # pragma: no cover
        IST = timezone.get_current_timezone()

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
      - HALF DAY: use the exact [start_at, end_at)
      - FULL DAY: expand to the entire calendar day 00:00 → next 00:00 (IST)
      - NOTE: we intentionally ignore `applied_at` to enforce retroactive blocking
        for any checks within the window as soon as the leave exists.
    """
    if not getattr(user, "id", None):
        return False

    # Ensure aware IST
    if timezone.is_naive(when_dt_ist):
        when_ist = when_dt_ist.replace(tzinfo=IST)
    else:
        when_ist = when_dt_ist.astimezone(IST)

    try:
        LeaveRequest = apps.get_model("leave", "LeaveRequest")
        if LeaveRequest is None:
            return False

        # Window for the calendar day of 'when'
        target_day = when_ist.date()
        day_start = datetime.combine(target_day, time.min).replace(tzinfo=IST)
        next_day_start = datetime.combine(target_day + timedelta(days=1), time.min).replace(tzinfo=IST)

        # Fetch only leaves overlapping the target day; include is_half_day for logic
        qs = (
            LeaveRequest.objects.filter(employee=user)
            .filter(start_at__lt=next_day_start, end_at__gt=day_start)
            .only("start_at", "end_at", "status", "is_half_day")
        )

        for lr in qs:
            status = str(getattr(lr, "status", "")).upper()
            if status not in ("PENDING", "APPROVED"):
                continue

            s = _to_ist(lr.start_at) or lr.start_at
            e = _to_ist(lr.end_at) or lr.end_at
            if e < s:
                s, e = e, s

            # Expand FULL DAY leaves to whole calendar day of the check in IST
            if not getattr(lr, "is_half_day", False):
                s = day_start
                e = next_day_start

            if s <= when_ist < e:
                return True

        return False

    except Exception:
        # Never break assignment flows on unexpected issues.
        logger.exception(
            "is_user_blocked_at failed for user_id=%s, when_dt_ist=%s",
            getattr(user, "id", None), when_ist
        )
        return False


def is_user_blocked(user, ist_date: date) -> bool:
    """
    Date-level API used by existing code (recurring visibility/email at 10:00 IST).
    - Full-day leaves block the entire day.
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
