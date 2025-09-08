# apps/tasks/services/blocking.py
from __future__ import annotations

"""
Assignment blocking helpers used across:
- Recurring generators (10:00 IST checklist generation)
- Delegation auto-assignment
- Help-ticket auto-routing

Keep the Example Flow in mind:
• As soon as an employee applies, the day is "locked" for that user.
• Rejected requests *unlock* the day.
• Approved keeps it locked.
• All checks are done on IST calendar dates.

This module is intentionally thin and defers the day-level decision to
`apps.tasks.utils.blocking.is_user_blocked(user, ist_date)` so there is
exactly one source of truth.
"""

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from django.utils import timezone

from apps.tasks.utils.blocking import is_user_blocked

logger = logging.getLogger("apps.tasks.blocking")
IST = ZoneInfo("Asia/Kolkata")


# ---------------------------------------------------------------------------
# Optional hook called by Leave app on apply (best-effort)
# See: apps.leave.signals._call_optional_blocker
# ---------------------------------------------------------------------------
def block_employee_dates(*, user_id: int, dates: list[date], source: str = "") -> None:
    """
    This function is called immediately after a LeaveRequest is created.

    We do not need to persist anything because assignment-time checks call
    `is_user_blocked(...)` which evaluates the Leave table live.
    We keep this hook for observability and future optimization (e.g., caching).

    Args:
        user_id: Django auth user id
        dates: list of IST dates that the leave covers (inclusive)
        source: free-form context like "leave:<id>"
    """
    try:
        if not user_id or not dates:
            return
        logger.info(
            "Blocking notice: user_id=%s dates=%s source=%s (live checks via is_user_blocked)",
            user_id, ",".join(str(d) for d in dates), source or "-"
        )
    except Exception:  # pragma: no cover
        logger.exception("block_employee_dates logging failed")


# ---------------------------------------------------------------------------
# Assignment guards (use these in generators/routers before assigning)
# ---------------------------------------------------------------------------
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
    Convenience wrapper when you have a datetime:
      True -> skip assignment (blocked)
      False -> safe to consider assigning
    """
    d = _ist_date_from_any(planned_dt)
    if not d:
        return False
    return is_user_blocked(user, d)


def should_skip_assignment(user, planned_dt) -> bool:
    """
    Alias used by existing assignment code. If True, you must NOT assign.
    """
    try:
        return is_user_blocked_for_datetime(user, planned_dt)
    except Exception:  # pragma: no cover
        logger.exception("should_skip_assignment failed (user_id=%s, dt=%r)", getattr(user, "id", None), planned_dt)
        return False


def guard_assign(user, planned_dt) -> bool:
    """
    Return True if it is OK to assign work to `user` at `planned_dt`.

    Typical usage in generators:
        if not guard_assign(user, planned_dt):
            continue  # skip this user

    Equivalent to: not should_skip_assignment(...)
    """
    return not should_skip_assignment(user, planned_dt)


__all__ = [
    "block_employee_dates",
    "is_user_blocked_for_datetime",
    "should_skip_assignment",
    "guard_assign",
]
