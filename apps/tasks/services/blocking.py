from __future__ import annotations

"""
Assignment blocking helpers.

Single source of truth: LeaveRequest rows that are PENDING/APPROVED and
cover the IST calendar date. This module exposes small helpers that
other task generators/routers can call.
"""

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from django.utils import timezone

from apps.leave.models import LeaveRequest  # canonical check lives here

logger = logging.getLogger("apps.tasks.blocking")
IST = ZoneInfo("Asia/Kolkata")


# -----------------------------------------------------------------------------
# Optional hook (observability only)
# -----------------------------------------------------------------------------
def block_employee_dates(*, user_id: int, dates: list[date], source: str = "") -> None:
    """
    Called after a LeaveRequest is created. We don't persist anything here:
    assignment-time checks query LeaveRequest live.
    """
    try:
        if not user_id or not dates:
            return
        logger.info(
            "Blocking notice: user_id=%s dates=%s source=%s",
            user_id,
            ",".join(str(d) for d in dates),
            source or "-",
        )
    except Exception:  # pragma: no cover
        logger.exception("block_employee_dates logging failed")


# -----------------------------------------------------------------------------
# Core guards
# -----------------------------------------------------------------------------
def _ist_date_from_any(dt_or_date) -> date | None:
    """Accept a date or datetime and return the IST calendar date."""
    if dt_or_date is None:
        return None
    if isinstance(dt_or_date, date) and not isinstance(dt_or_date, datetime):
        return dt_or_date
    try:
        aware = timezone.localtime(dt_or_date, IST) if timezone.is_aware(dt_or_date) else dt_or_date.replace(tzinfo=IST)
        return aware.date()
    except Exception:
        try:
            return dt_or_date.date()  # type: ignore[attr-defined]
        except Exception:
            return None


def is_user_blocked_for_datetime(user, planned_dt) -> bool:
    """
    True -> skip assignment (blocked)
    False -> safe to consider assigning
    """
    d = _ist_date_from_any(planned_dt)
    if not d:
        return False
    return LeaveRequest.is_user_blocked_on(user, d)


def should_skip_assignment(user, planned_dt) -> bool:
    """Alias used in generators. If True, you must NOT assign."""
    try:
        return is_user_blocked_for_datetime(user, planned_dt)
    except Exception:  # pragma: no cover
        logger.exception("should_skip_assignment failed (user_id=%s, dt=%r)", getattr(user, "id", None), planned_dt)
        return False


def guard_assign(user, planned_dt) -> bool:
    """Return True if it's OK to assign (i.e., NOT blocked)."""
    return not should_skip_assignment(user, planned_dt)


__all__ = [
    "block_employee_dates",
    "is_user_blocked_for_datetime",
    "should_skip_assignment",
    "guard_assign",
]
