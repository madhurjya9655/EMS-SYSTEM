# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Single source of truth to decide if a user is BLOCKED for a given IST calendar date.

Keep the Example Flow in mind:
- As soon as an employee applies, the covered date is locked for that user.
- If the request is later REJECTED, the day unlocks.
- APPROVED keeps it locked.
- Same-day pending requests only count if they were applied before 09:30 IST.
- All comparisons are done on IST calendar dates.

Usage:
    from apps.tasks.utils.blocking import is_user_blocked
    blocked = is_user_blocked(user, ist_date)  # ist_date is a datetime.date (IST)

This function is intentionally side-effect free and fast; call it anywhere
before assigning work (recurring generators at 10:00 IST, delegations, tickets).
"""

from datetime import date, datetime, time, timedelta
from typing import Iterable, Optional
import logging

from django.utils import timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

IST = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.get_current_timezone()
logger = logging.getLogger("apps.tasks.blocking")

# Late import to avoid circular deps at import time
# (Django app registry must be ready when this runs).
def _leave_models():
    from apps.leave.models import LeaveRequest, LeaveStatus  # noqa: WPS433
    return LeaveRequest, LeaveStatus


# ------------- helpers (IST conversions) -------------------------------------
def _to_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST)
    except Exception:
        # best-effort fallback
        return dt

def _ist_date_from_dt(dt: datetime) -> date:
    local = _to_ist(dt) or dt
    return (local if timezone.is_aware(local) else local.replace(tzinfo=IST)).date()  # type: ignore[return-value]

def _datespan_inclusive_ist(start_dt: datetime, end_dt: datetime) -> list[date]:
    s = _ist_date_from_dt(start_dt)
    # treat end as inclusive by subtracting a tiny delta
    e = _ist_date_from_dt(end_dt - timedelta(microseconds=1))
    if e < s:
        s, e = e, s
    out: list[date] = []
    cur = s
    while cur <= e:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


# ------------- core rule -----------------------------------------------------
def _pending_counts_for_day(applied_at: Optional[datetime], target_day: date) -> bool:
    """
    Pending leaves block the day if:
      • the leave covers `target_day`, AND
      • either the apply date is before that day, OR
      • the apply date is the same day and the apply time was <= 09:30 IST.
    """
    if not applied_at:
        return False
    a = _to_ist(applied_at)
    if not a:
        return False

    if a.date() < target_day:
        return True  # applied earlier → counts
    if a.date() > target_day:
        return False  # applied after the day → does not count

    # same-day gate: must be <= 09:30 IST
    anchor_930 = a.replace(hour=9, minute=30, second=0, microsecond=0)
    return a <= anchor_930


def is_user_blocked(user, ist_date: date) -> bool:
    """
    Return True if `user` must be considered unavailable (blocked) for assignments
    on the given IST calendar date.

    Rules:
      - APPROVED leave covering the date → True
      - PENDING leave covering the date → True if _pending_counts_for_day(...) is True
      - REJECTED → never blocks
    """
    if not getattr(user, "id", None):
        return False

    try:
        LeaveRequest, LeaveStatus = _leave_models()

        # Fast path: we only need rows that could possibly hit the date.
        # We do not filter by status here because PENDING/APPROVED both matter.
        start_floor = datetime.combine(ist_date, time.min).replace(tzinfo=IST)
        end_ceiling = datetime.combine(ist_date + timedelta(days=1), time.min).replace(tzinfo=IST)

        qs = (
            LeaveRequest.objects.filter(employee=user)
            .filter(start_at__lt=end_ceiling, end_at__gt=start_floor)
            .only("start_at", "end_at", "status", "applied_at")
        )

        for lr in qs:
            # Quick skip if the IST day is not in the leave span
            span = _datespan_inclusive_ist(lr.start_at, lr.end_at)
            if ist_date not in span:
                continue

            if lr.status == LeaveStatus.APPROVED:
                return True

            if lr.status == LeaveStatus.PENDING:
                if _pending_counts_for_day(getattr(lr, "applied_at", None), ist_date):
                    return True

        return False

    except Exception:
        # Never break assignment flows on unexpected issues.
        logger.exception("is_user_blocked failed for user_id=%s, ist_date=%s", getattr(user, "id", None), ist_date)
        return False


__all__ = ["is_user_blocked"]
