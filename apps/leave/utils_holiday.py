# apps/leave/utils_holiday.py
"""
Holiday and Sunday utility functions for task blocking.
Import these in your recurring task engine.
"""
from __future__ import annotations

from datetime import date


def is_sunday(check_date: date) -> bool:
    """Return True if the date is a Sunday."""
    return check_date.weekday() == 6


def is_holiday_date(check_date: date) -> bool:
    """Return True if the date is a configured holiday."""
    try:
        from apps.settings.models import Holiday
        return Holiday.objects.filter(date=check_date).exists()
    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    """
    Return True if the date is non-working (holiday OR Sunday).
    Priority: Holiday > Sunday
    """
    return is_holiday_date(check_date) or is_sunday(check_date)


def get_skip_reason(check_date: date, employee=None) -> str | None:
    """
    Returns the reason to skip, or None if task should be generated.

    Priority:
    1. Holiday
    2. Sunday
    3. Employee on leave
    """
    if is_holiday_date(check_date):
        return "holiday"
    if is_sunday(check_date):
        return "sunday"
    if employee:
        from apps.leave.utils import is_employee_on_leave
        if is_employee_on_leave(employee, check_date):
            return "leave"
    return None