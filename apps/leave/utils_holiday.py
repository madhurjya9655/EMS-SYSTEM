"""
Holiday, Sunday, and applied-leave utility functions for task blocking.

Production rule:
- Tasks must NOT generate / show / email during:
  1. Admin holidays
  2. Sundays
  3. Employee leave period

Important:
- PENDING leave blocks immediately after apply.
- APPROVED leave blocks.
- REJECTED / CANCELLED leave does not block.
- Full-day leave blocks full IST date.
- Half-day leave blocks exact selected time period.
"""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from django.utils import timezone

IST = ZoneInfo("Asia/Kolkata")


def is_sunday(check_date: date) -> bool:
    """Return True if the date is a Sunday."""
    return check_date.weekday() == 6


def is_holiday_date(check_date: date) -> bool:
    """Return True if the date is a configured admin holiday."""
    try:
        from apps.settings.models import Holiday

        try:
            return bool(Holiday.is_holiday(check_date))
        except Exception:
            return Holiday.objects.filter(date=check_date).exists()

    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    """Return True if the date is holiday or Sunday."""
    return is_holiday_date(check_date) or is_sunday(check_date)


def _ensure_aware_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def is_employee_on_leave_at(employee, check_dt: datetime) -> bool:
    """
    Exact datetime leave check.

    Use this for:
    - manual assignment
    - email sending
    - half-day leave checks
    """
    if not getattr(employee, "id", None):
        return False

    try:
        from apps.tasks.utils.blocking import is_user_blocked_at

        return bool(is_user_blocked_at(employee, _ensure_aware_ist(check_dt)))

    except Exception:
        return False


def is_employee_on_leave_for_date(employee, check_date: date) -> bool:
    """
    Date-level leave check using 10:00 AM IST anchor.

    Good for:
    - full-day recurrence check
    - daily digest check
    - date-only screens
    """
    if not getattr(employee, "id", None):
        return False

    anchor_ist = datetime.combine(check_date, time(10, 0)).replace(tzinfo=IST)
    return is_employee_on_leave_at(employee, anchor_ist)


def get_skip_reason_at(check_dt: datetime, employee=None) -> str | None:
    """
    Exact datetime skip reason.

    Priority:
    1. Holiday
    2. Sunday
    3. Employee on pending/approved leave at exact datetime
    """
    check_dt_ist = _ensure_aware_ist(check_dt)
    check_date = check_dt_ist.date()

    if is_holiday_date(check_date):
        return "holiday"

    if is_sunday(check_date):
        return "sunday"

    if employee and is_employee_on_leave_at(employee, check_dt_ist):
        return "leave"

    return None


def get_skip_reason(check_date: date, employee=None) -> str | None:
    """
    Date-level skip reason using 10:00 AM IST anchor.

    Priority:
    1. Holiday
    2. Sunday
    3. Employee on pending/approved leave at 10:00 AM IST
    """
    if is_holiday_date(check_date):
        return "holiday"

    if is_sunday(check_date):
        return "sunday"

    if employee and is_employee_on_leave_for_date(employee, check_date):
        return "leave"

    return None