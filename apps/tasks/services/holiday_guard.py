#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\services\holiday_guard.py
from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time
from typing import Optional

import pytz
from django.db.utils import OperationalError, ProgrammingError
from django.utils import timezone

from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
ASSIGN_ANCHOR_T = dt_time(10, 0)


def ensure_project_aware(value: datetime) -> datetime:
    """
    Make datetime timezone-aware in Django project timezone.

    If datetime is already aware, return it.
    If datetime is naive, assume project timezone.
    """
    if timezone.is_aware(value):
        return value

    return timezone.make_aware(value, timezone.get_current_timezone())


def to_ist_datetime(value: Optional[datetime | date] = None) -> datetime:
    """
    Convert None/date/datetime into IST-aware datetime.

    None:
        current time in IST

    date:
        same date at 10:00 AM IST

    naive datetime:
        assume Django project timezone, then convert to IST

    aware datetime:
        convert to IST
    """
    if value is None:
        return timezone.now().astimezone(IST)

    if isinstance(value, date) and not isinstance(value, datetime):
        return IST.localize(datetime.combine(value, ASSIGN_ANCHOR_T))

    if isinstance(value, datetime):
        value = ensure_project_aware(value)
        return value.astimezone(IST)

    return timezone.now().astimezone(IST)


def to_ist_date(value: Optional[datetime | date] = None) -> date:
    """
    Return IST date from None/date/datetime.
    """
    return to_ist_datetime(value).date()


def is_sunday(d: date) -> bool:
    """
    Python weekday:
    Monday = 0
    Sunday = 6
    """
    return d.weekday() == 6


def is_holiday_master_date(d: date) -> bool:
    """
    Check Holiday Master safely.
    """
    try:
        if hasattr(Holiday, "is_holiday"):
            return bool(Holiday.is_holiday(d))

        return Holiday.objects.filter(date=d).exists()

    except (OperationalError, ProgrammingError):
        logger.exception("Holiday table not ready while checking date=%s", d)
        return False

    except Exception:
        logger.exception("Holiday check failed for date=%s", d)
        return False


def is_off_day_date(d: date) -> bool:
    """
    BOS Lakshya OFF DAY rule.

    True means:
    - Sunday
    - Holiday Master date
    """
    return is_sunday(d) or is_holiday_master_date(d)


def is_holiday_for_user(user=None, value: Optional[datetime | date] = None) -> bool:
    """
    Main common guard.

    user is accepted because future architecture may support:
    - branch-wise holiday
    - location-wise holiday
    - department-wise holiday

    Current rule:
    - Sunday is global off day
    - Holiday Master is global off day
    """
    d = to_ist_date(value)
    return is_off_day_date(d)


def holiday_skip_reason(value: Optional[datetime | date] = None) -> str:
    """
    Return simple reason for logs.
    """
    d = to_ist_date(value)

    if is_sunday(d):
        return "sunday"

    if is_holiday_master_date(d):
        return "holiday"

    return ""


def ist_day_project_bounds(d: date) -> tuple[datetime, datetime]:
    """
    Return IST day start/end converted to Django project timezone.

    Useful for dashboard queryset.
    """
    start_ist = IST.localize(datetime.combine(d, dt_time.min))
    end_ist = IST.localize(datetime.combine(d, dt_time.max))

    project_tz = timezone.get_current_timezone()

    return (
        start_ist.astimezone(project_tz),
        end_ist.astimezone(project_tz),
    )