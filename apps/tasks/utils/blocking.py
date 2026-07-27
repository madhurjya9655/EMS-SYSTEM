# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Canonical employee blocking utilities for task assignment and task email checks.

Production rules
----------------
1. PENDING leave blocks immediately after the employee applies.
2. APPROVED leave blocks.
3. REJECTED and CANCELLED leave do not block.
4. Full-day leave blocks every instant of every covered IST calendar date.
5. Half-day leave blocks only the exact [start_at, end_at) interval.
6. All comparisons are performed in Asia/Kolkata time.
7. Failures are logged and return False so task processing does not crash.

Public API
----------
is_user_blocked_at(user, when_dt) -> bool
is_user_blocked(user, ist_date) -> bool
is_user_blocked_for_task_time(user, ist_date, at_time_ist) -> bool
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Optional

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

logger = logging.getLogger(__name__)

# Date-only checks use the office-start anchor.
ASSIGN_ANCHOR_IST = time(10, 0)

# Leave blocks tasks as soon as it is submitted.
TASK_BLOCKING_STATUSES = frozenset({"PENDING", "APPROVED"})


def _normalize_status(value: Any) -> str:
    """Return a normalized uppercase leave status."""
    return str(value or "").strip().upper()


def _get_user_id(user: Any) -> Optional[int]:
    """
    Return a valid user ID.

    Accepts either:
    - a Django user/model instance with .pk or .id
    - a positive integer user ID
    """
    if isinstance(user, bool):
        return None

    if isinstance(user, int):
        return user if user > 0 else None

    value = getattr(user, "pk", None)
    if value is None:
        value = getattr(user, "id", None)

    try:
        value = int(value)
    except (TypeError, ValueError):
        return None

    return value if value > 0 else None


def _ensure_aware_ist(value: datetime) -> datetime:
    """
    Return an IST-aware datetime.

    A naive datetime is treated as an IST wall-clock datetime.
    """
    if not isinstance(value, datetime):
        raise TypeError("Expected datetime value.")

    if timezone.is_naive(value):
        try:
            return timezone.make_aware(value, IST)
        except Exception:
            return value.replace(tzinfo=IST)

    try:
        return value.astimezone(IST)
    except Exception:
        return value


def _to_ist(value: Optional[datetime]) -> Optional[datetime]:
    """Convert a datetime to IST, returning None for an empty value."""
    if value is None:
        return None

    try:
        return _ensure_aware_ist(value)
    except Exception:
        return None


def _coerce_check_datetime(value: date | datetime) -> datetime:
    """
    Convert a date/datetime input into an IST-aware datetime.

    A plain date is checked at ASSIGN_ANCHOR_IST.
    """
    if isinstance(value, datetime):
        return _ensure_aware_ist(value)

    if isinstance(value, date):
        return _ensure_aware_ist(
            datetime.combine(value, ASSIGN_ANCHOR_IST)
        )

    raise TypeError("when_dt must be a date or datetime instance.")


def _ist_day_bounds(value: date) -> tuple[datetime, datetime]:
    """Return the half-open IST range [day_start, next_day_start)."""
    day_start = _ensure_aware_ist(datetime.combine(value, time.min))
    next_day_start = _ensure_aware_ist(
        datetime.combine(value + timedelta(days=1), time.min)
    )
    return day_start, next_day_start


def _normalized_leave_window(leave) -> Optional[tuple[datetime, datetime]]:
    """
    Return the normalized IST leave window.

    Invalid or incomplete windows return None.
    """
    start_at = _to_ist(getattr(leave, "start_at", None))
    end_at = _to_ist(getattr(leave, "end_at", None))

    if start_at is None or end_at is None:
        return None

    if end_at < start_at:
        start_at, end_at = end_at, start_at

    if end_at == start_at:
        return None

    return start_at, end_at


def _full_day_covers_target_date(leave, target_date: date) -> bool:
    """
    Return True when a full-day leave covers the target IST calendar date.

    Prefer the persisted start_date/end_date fields. Fall back to start_at/end_at.
    """
    leave_start_date = getattr(leave, "start_date", None)
    leave_end_date = getattr(leave, "end_date", None)

    if not leave_start_date or not leave_end_date:
        window = _normalized_leave_window(leave)
        if window is None:
            return False

        leave_start_date = window[0].date()
        leave_end_date = window[1].date()

    if leave_end_date < leave_start_date:
        leave_start_date, leave_end_date = leave_end_date, leave_start_date

    return leave_start_date <= target_date <= leave_end_date


def is_user_blocked_at(user, when_dt: date | datetime) -> bool:
    """
    Return True when the user is blocked by an active leave at an exact IST time.

    Full-day behavior
    -----------------
    The complete IST calendar date is blocked, even though LeaveRequest.start_at
    and end_at may be stored using office-hour values.

    Half-day behavior
    -----------------
    Only the exact [start_at, end_at) interval is blocked. The end instant is not
    blocked, which prevents two adjacent time windows from overlapping.
    """
    user_id = _get_user_id(user)
    if user_id is None:
        return False

    try:
        check_at_ist = _coerce_check_datetime(when_dt)
        target_date = check_at_ist.date()
        day_start, next_day_start = _ist_day_bounds(target_date)

        LeaveRequest = apps.get_model("leave", "LeaveRequest")
        if LeaveRequest is None:
            logger.error("LeaveRequest model could not be loaded.")
            return False

        candidates = (
            LeaveRequest.objects
            .filter(
                employee_id=user_id,
                status__in=TASK_BLOCKING_STATUSES,
            )
            .filter(
                Q(start_date__lte=target_date, end_date__gte=target_date)
                |
                Q(start_at__lt=next_day_start, end_at__gt=day_start)
            )
            .only(
                "id",
                "status",
                "start_at",
                "end_at",
                "start_date",
                "end_date",
                "is_half_day",
            )
            .order_by("start_at", "id")
        )

        for leave in candidates:
            if _normalize_status(getattr(leave, "status", None)) not in TASK_BLOCKING_STATUSES:
                continue

            if bool(getattr(leave, "is_half_day", False)):
                window = _normalized_leave_window(leave)
                if window is None:
                    continue

                leave_start_ist, leave_end_ist = window

                if leave_start_ist <= check_at_ist < leave_end_ist:
                    return True

                continue

            if _full_day_covers_target_date(leave, target_date):
                return True

        return False

    except (TypeError, ValueError):
        logger.exception(
            "Invalid blocking check input: user_id=%s when=%r",
            user_id,
            when_dt,
        )
        return False

    except Exception:
        logger.exception(
            "is_user_blocked_at failed: user_id=%s when=%r",
            user_id,
            when_dt,
        )
        return False


def is_user_blocked(user, ist_date: date) -> bool:
    """
    Perform the legacy date-level leave check at 10:00 AM IST.

    This is appropriate for:
    - daily task generation
    - date-only screens
    - date-only assignment checks

    For exact half-day behavior, call is_user_blocked_at() instead.
    """
    if not isinstance(ist_date, date) or isinstance(ist_date, datetime):
        return False

    check_at = _ensure_aware_ist(
        datetime.combine(ist_date, ASSIGN_ANCHOR_IST)
    )
    return is_user_blocked_at(user, check_at)


def is_user_blocked_for_task_time(
    user,
    ist_date: date,
    at_time_ist: time,
) -> bool:
    """
    Check whether the user is blocked at an exact IST date and time.
    """
    if not isinstance(ist_date, date) or isinstance(ist_date, datetime):
        return False

    if not isinstance(at_time_ist, time):
        return False

    check_at = _ensure_aware_ist(
        datetime.combine(ist_date, at_time_ist)
    )
    return is_user_blocked_at(user, check_at)


__all__ = [
    "ASSIGN_ANCHOR_IST",
    "TASK_BLOCKING_STATUSES",
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
]