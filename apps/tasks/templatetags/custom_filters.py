# apps/tasks/templatetags/custom_filters.py
from __future__ import annotations

from datetime import datetime, date as _date
import decimal
import pytz

from django import template
from django.utils import timezone

register = template.Library()

# ---------------------------
# Numeric helpers (safe ints)
# ---------------------------
def _to_int(v, default=0) -> int:
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int,)):
            return v
        if isinstance(v, (float, decimal.Decimal)):
            return int(v)
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


@register.filter
def subtract(value, arg):
    """Subtract arg from value. Returns 0 on any conversion error."""
    return _to_int(value) - _to_int(arg)


@register.filter
def multiply(value, arg):
    """Multiply value by arg. Returns 0 on any conversion error."""
    return _to_int(value) * _to_int(arg)


@register.filter
def divide(value, arg):
    """
    Integer divide value by arg.
    Returns 0 if arg is 0 or on conversion error.
    """
    a = _to_int(arg)
    if a == 0:
        return 0
    return _to_int(value) // a


# ---------------------------
# Datetime helpers / IST
# ---------------------------
IST = pytz.timezone("Asia/Kolkata")


@register.filter
def to_ist(value):
    """
    Convert a date/datetime to an aware datetime in IST.
    - If value is an aware datetime: convert tz to IST
    - If naive datetime: assume current project tz, make aware, then convert to IST
    - If date: return midnight in IST for that date
    - Otherwise: return empty string
    """
    if not value:
        return ""
    try:
        if isinstance(value, _date) and not isinstance(value, datetime):
            # Treat pure date as midnight in IST
            naive = datetime(value.year, value.month, value.day)
            return IST.localize(naive)
        if isinstance(value, datetime):
            dt = value
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, timezone.get_current_timezone())
            return dt.astimezone(IST)
    except Exception:
        return ""
    return ""


@register.filter
def format_ist(value, fmt="%Y-%m-%d %H:%M"):
    """
    Convert a datetime to IST and format as string with the given strftime format.
    Falls back to empty string on error.
    """
    if not value:
        return ""
    try:
        dt_ist = to_ist(value)
        if not isinstance(dt_ist, datetime):
            return ""
        return dt_ist.strftime(fmt)
    except Exception:
        return ""


@register.filter
def minutes_to_hours(minutes):
    """
    Convert an integer number of minutes to HH:MM (zero-padded).
    Returns "00:00" on error.
    """
    try:
        total = _to_int(minutes)
        hours = total // 60
        mins = total % 60
        return f"{hours:02d}:{mins:02d}"
    except Exception:
        return "00:00"


# Friendly alias
@register.filter(name="hhmm")
def minutes_to_hhmm(minutes):
    """Alias for minutes_to_hours."""
    return minutes_to_hours(minutes)
