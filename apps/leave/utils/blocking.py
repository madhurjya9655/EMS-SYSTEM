"""Holiday, Sunday, and applied-leave utility functions for task blocking.

Production rules:
- PENDING and APPROVED leave block immediately.
- REJECTED and CANCELLED leave do not block.
- Full-day leave blocks the covered IST date range.
- Half-day leave blocks only its exact stored IST datetime interval.
"""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from django.utils import timezone

IST = ZoneInfo("Asia/Kolkata")
ACTIVE_LEAVE_STATUSES = ("PENDING", "APPROVED")


def is_sunday(check_date: date) -> bool:
    return check_date.weekday() == 6


def is_holiday_date(check_date: date) -> bool:
    try:
        from apps.settings.models import Holiday
        try:
            return bool(Holiday.is_holiday(check_date))
        except Exception:
            return Holiday.objects.filter(date=check_date).exists()
    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    return is_holiday_date(check_date) or is_sunday(check_date)


def _ensure_aware_ist(value: datetime) -> datetime:
    if timezone.is_naive(value):
        return timezone.make_aware(value, IST)
    return value.astimezone(IST)


def is_user_blocked_at(employee, check_dt: datetime) -> bool:
    if not getattr(employee, "id", None):
        return False

    from apps.leave.models import LeaveRequest

    check_ist = _ensure_aware_ist(check_dt)
    check_date = check_ist.date()

    candidates = LeaveRequest.objects.filter(
        employee_id=employee.id,
        status__in=ACTIVE_LEAVE_STATUSES,
        start_date__lte=check_date,
        end_date__gte=check_date,
    ).only("start_at", "end_at", "is_half_day")

    for leave in candidates:
        start_ist = _ensure_aware_ist(leave.start_at)
        end_ist = _ensure_aware_ist(leave.end_at)
        if end_ist < start_ist:
            start_ist, end_ist = end_ist, start_ist

        if leave.is_half_day:
            if start_ist <= check_ist < end_ist:
                return True
        elif start_ist.date() <= check_date <= end_ist.date():
            return True

    return False


def is_employee_on_leave_at(employee, check_dt: datetime) -> bool:
    try:
        return is_user_blocked_at(employee, check_dt)
    except Exception:
        return False


def is_employee_on_leave_for_date(employee, check_date: date) -> bool:
    anchor = datetime.combine(check_date, time(10, 0)).replace(tzinfo=IST)
    return is_employee_on_leave_at(employee, anchor)


def get_skip_reason_at(check_dt: datetime, employee=None) -> str | None:
    check_ist = _ensure_aware_ist(check_dt)
    check_date = check_ist.date()
    if is_holiday_date(check_date):
        return "holiday"
    if is_sunday(check_date):
        return "sunday"
    if employee and is_employee_on_leave_at(employee, check_ist):
        return "leave"
    return None


def get_skip_reason(check_date: date, employee=None) -> str | None:
    if is_holiday_date(check_date):
        return "holiday"
    if is_sunday(check_date):
        return "sunday"
    if employee and is_employee_on_leave_for_date(employee, check_date):
        return "leave"
    return None


__all__ = [
    "get_skip_reason",
    "get_skip_reason_at",
    "is_employee_on_leave_at",
    "is_employee_on_leave_for_date",
    "is_holiday_date",
    "is_non_working_day",
    "is_sunday",
    "is_user_blocked_at",
]