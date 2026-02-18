# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\services\blocking.py
from __future__ import annotations

"""
Assignment blocking helpers.

✅ Single source of truth:
    apps.tasks.utils.blocking   (time-aware + date-aware, IST rules)

This module provides small helpers for generators/routers.
Do NOT re-implement leave window logic here.
"""

import logging
from datetime import date, datetime

from django.utils import timezone

from apps.tasks.utils.blocking import (
    is_user_blocked_at,  # time-aware
    is_user_blocked,     # date-level (10:00 IST anchor)
)

logger = logging.getLogger("apps.tasks.blocking")


def block_employee_dates(*, user_id: int, dates: list[date], source: str = "") -> None:
    """Observability hook only (no persistence)."""
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


def is_user_blocked_for_datetime(user, planned_dt) -> bool:
    """
    True  -> skip assignment (blocked)
    False -> safe to consider assigning

    Accepts:
      - datetime (naive or aware): checked at that exact instant (IST rules internally)
      - date: checked using date-level legacy rule (10:00 IST anchor)
    """
    if not getattr(user, "id", None) or planned_dt is None:
        return False

    # date-only: treat as whole-day check with 10:00 IST anchor
    if isinstance(planned_dt, date) and not isinstance(planned_dt, datetime):
        try:
            return bool(is_user_blocked(user, planned_dt))
        except Exception:
            logger.exception(
                "is_user_blocked_for_datetime(date) failed (user_id=%s, planned_dt=%r)",
                getattr(user, "id", None),
                planned_dt,
            )
            return False

    # datetime: time-aware exact check
    try:
        # If naive, interpret in project tz before blocking util converts to IST
        if timezone.is_naive(planned_dt):
            planned_dt = timezone.make_aware(planned_dt, timezone.get_current_timezone())
        return bool(is_user_blocked_at(user, planned_dt))
    except Exception:
        logger.exception(
            "is_user_blocked_for_datetime(datetime) failed (user_id=%s, planned_dt=%r)",
            getattr(user, "id", None),
            planned_dt,
        )
        return False


def should_skip_assignment(user, planned_dt) -> bool:
    """Alias used in generators. If True, you must NOT assign."""
    return is_user_blocked_for_datetime(user, planned_dt)


def guard_assign(user, planned_dt) -> bool:
    """Return True if it's OK to assign (i.e., NOT blocked)."""
    return not should_skip_assignment(user, planned_dt)


__all__ = [
    "block_employee_dates",
    "is_user_blocked_for_datetime",
    "should_skip_assignment",
    "guard_assign",
]
