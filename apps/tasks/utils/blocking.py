# apps/tasks/utils/blocking.py
from __future__ import annotations

"""
Canonical employee leave-blocking utilities.

Production rules
----------------
1. PENDING leave blocks immediately after submission.
2. APPROVED leave continues to block.
3. REJECTED and CANCELLED leave do not block.
4. Full-day leave blocks the complete covered IST calendar date.
5. Half-day leave blocks only the exact selected IST time range.
6. All comparisons use Asia/Kolkata time.
7. Errors are logged and return False so task processing does not crash.

Public API
----------
is_user_blocked_at(user, when_dt) -> bool
is_user_blocked(user, ist_date) -> bool
is_user_blocked_for_task_time(user, ist_date, at_time_ist) -> bool
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from django.apps import apps
from django.db.models import Q
from django.utils import timezone

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# Date-only checks use the normal office starting time.
ASSIGN_ANCHOR_IST = time(10, 0)

# Leave blocks tasks immediately after submission.
TASK_BLOCKING_STATUSES = frozenset(
    {
        "PENDING",
        "APPROVED",
    }
)


def _normalize_status(value: Any) -> str:
    """Return leave status as a clean uppercase string."""
    return str(value or "").strip().upper()


def _get_user_id(user: Any) -> Optional[int]:
    """
    Return a valid user ID.

    Supports:
    - Django User object
    - Any model object having pk/id
    - Positive integer user ID
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

    A naive datetime is treated as an IST local time.
    """
    if not isinstance(value, datetime):
        raise TypeError("Expected a datetime value.")

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
    """Convert a datetime to IST."""
    if value is None:
        return None

    try:
        return _ensure_aware_ist(value)
    except Exception:
        return None


def _coerce_check_datetime(value: date | datetime) -> datetime:
    """
    Convert date or datetime to an IST-aware datetime.

    A plain date is checked at 10:00 AM IST.
    """
    if isinstance(value, datetime):
        return _ensure_aware_ist(value)

    if isinstance(value, date):
        return _ensure_aware_ist(
            datetime.combine(
                value,
                ASSIGN_ANCHOR_IST,
            )
        )

    raise TypeError("Value must be a date or datetime.")


def _ist_day_bounds(target_date: date) -> tuple[datetime, datetime]:
    """
    Return the IST datetime range for one date.

    Result:
        [00:00 today, 00:00 next day)
    """
    day_start = _ensure_aware_ist(
        datetime.combine(
            target_date,
            time.min,
        )
    )

    next_day_start = _ensure_aware_ist(
        datetime.combine(
            target_date + timedelta(days=1),
            time.min,
        )
    )

    return day_start, next_day_start


def _normalized_leave_window(
    leave,
) -> Optional[tuple[datetime, datetime]]:
    """
    Return leave start and end in IST.

    Returns None for invalid or empty leave windows.
    """
    start_at = _to_ist(
        getattr(
            leave,
            "start_at",
            None,
        )
    )

    end_at = _to_ist(
        getattr(
            leave,
            "end_at",
            None,
        )
    )

    if start_at is None or end_at is None:
        return None

    if end_at < start_at:
        start_at, end_at = end_at, start_at

    if end_at == start_at:
        return None

    return start_at, end_at


def _leave_dates_from_window(
    leave,
) -> Optional[tuple[date, date]]:
    """
    Return the leave start and end dates in IST.

    Stored start_date/end_date are preferred.

    If they are missing, dates are calculated from start_at/end_at.

    One microsecond is removed from the end time so a leave ending exactly at
    midnight does not incorrectly block the next date.
    """
    start_date = getattr(
        leave,
        "start_date",
        None,
    )

    end_date = getattr(
        leave,
        "end_date",
        None,
    )

    if start_date and end_date:
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        return start_date, end_date

    window = _normalized_leave_window(leave)

    if window is None:
        return None

    start_at, end_at = window

    calculated_start = start_at.date()
    calculated_end = (
        end_at - timedelta(microseconds=1)
    ).date()

    if calculated_end < calculated_start:
        calculated_start, calculated_end = (
            calculated_end,
            calculated_start,
        )

    return calculated_start, calculated_end


def _full_day_covers_target_date(
    leave,
    target_date: date,
) -> bool:
    """
    Return True when a full-day leave covers the given IST date.

    A full-day leave blocks the complete date even when start_at/end_at use
    office-hour values such as 10:00 AM to 06:30 PM.
    """
    dates = _leave_dates_from_window(leave)

    if dates is None:
        return False

    leave_start_date, leave_end_date = dates

    return leave_start_date <= target_date <= leave_end_date


def _half_day_blocks_time(
    leave,
    check_at_ist: datetime,
) -> bool:
    """
    Return True when check_at_ist is inside the half-day leave window.

    The range is half-open:

        start_at <= check time < end_at

    Therefore, the exact end time is not blocked.
    """
    window = _normalized_leave_window(leave)

    if window is None:
        return False

    leave_start_ist, leave_end_ist = window

    return leave_start_ist <= check_at_ist < leave_end_ist


def is_user_blocked_at(
    user,
    when_dt: date | datetime,
) -> bool:
    """
    Return True when the user is blocked by leave at the given IST time.

    Full-day leave:
        Blocks every time during every covered calendar date.

    Half-day leave:
        Blocks only the exact selected time window.

    Counted statuses:
        PENDING
        APPROVED

    Ignored statuses:
        REJECTED
        CANCELLED
    """
    user_id = _get_user_id(user)

    if user_id is None:
        return False

    try:
        check_at_ist = _coerce_check_datetime(when_dt)
        target_date = check_at_ist.date()

        day_start, next_day_start = _ist_day_bounds(
            target_date
        )

        LeaveRequest = apps.get_model(
            "leave",
            "LeaveRequest",
        )

        if LeaveRequest is None:
            logger.error(
                "LeaveRequest model could not be loaded."
            )
            return False

        candidates = (
            LeaveRequest.objects
            .filter(
                employee_id=user_id,
                status__in=TASK_BLOCKING_STATUSES,
            )
            .filter(
                Q(
                    start_date__lte=target_date,
                    end_date__gte=target_date,
                )
                |
                Q(
                    start_at__lt=next_day_start,
                    end_at__gt=day_start,
                )
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
            .order_by(
                "start_at",
                "id",
            )
        )

        for leave in candidates:
            status = _normalize_status(
                getattr(
                    leave,
                    "status",
                    None,
                )
            )

            if status not in TASK_BLOCKING_STATUSES:
                continue

            is_half_day = bool(
                getattr(
                    leave,
                    "is_half_day",
                    False,
                )
            )

            if is_half_day:
                if _half_day_blocks_time(
                    leave,
                    check_at_ist,
                ):
                    return True

                continue

            if _full_day_covers_target_date(
                leave,
                target_date,
            ):
                return True

        return False

    except (TypeError, ValueError):
        logger.warning(
            "Invalid blocking check input: user_id=%s when=%r",
            user_id,
            when_dt,
            exc_info=True,
        )
        return False

    except Exception:
        logger.exception(
            "Leave blocking check failed: user_id=%s when=%r",
            user_id,
            when_dt,
        )
        return False


def is_user_blocked(
    user,
    ist_date: date,
) -> bool:
    """
    Perform a date-level leave check at 10:00 AM IST.

    Use for:
    - Daily task generation
    - Date-only task screens
    - Date-only assignment validation

    Important:
    An afternoon half-day may return False here because this function checks
    specifically at 10:00 AM.

    For exact half-day checking, use:

        is_user_blocked_at()

    or:

        is_user_blocked_for_task_time()
    """
    if not isinstance(ist_date, date):
        return False

    if isinstance(ist_date, datetime):
        return False

    check_at = _ensure_aware_ist(
        datetime.combine(
            ist_date,
            ASSIGN_ANCHOR_IST,
        )
    )

    return is_user_blocked_at(
        user,
        check_at,
    )


def is_user_blocked_for_task_time(
    user,
    ist_date: date,
    at_time_ist: time,
) -> bool:
    """
    Check whether a user is blocked at an exact IST date and time.

    Example:

        is_user_blocked_for_task_time(
            employee,
            date(2026, 7, 29),
            time(15, 0),
        )
    """
    if not isinstance(ist_date, date):
        return False

    if isinstance(ist_date, datetime):
        return False

    if not isinstance(at_time_ist, time):
        return False

    check_at = _ensure_aware_ist(
        datetime.combine(
            ist_date,
            at_time_ist,
        )
    )

    return is_user_blocked_at(
        user,
        check_at,
    )


__all__ = [
    "ASSIGN_ANCHOR_IST",
    "TASK_BLOCKING_STATUSES",
    "is_user_blocked",
    "is_user_blocked_at",
    "is_user_blocked_for_task_time",
]