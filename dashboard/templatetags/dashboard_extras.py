# E:\CLIENT PROJECT\employee management system bos\employee_management_system\dashboard\templatetags\dashboard_extras.py
from __future__ import annotations

from datetime import datetime
from django import template
from django.utils import timezone
from django.utils.timesince import timesince as dj_timesince

register = template.Library()


@register.filter(name="delay_since")
def delay_since(value, now=None) -> str:
    """
    Humanized time since 'value' (e.g., '2d 3h', '45m').
    - Future -> '0m'
    - Accepts naive/aware datetimes
    Returns empty string on bad input.
    """
    if not value:
        return ""

    tz = timezone.get_current_timezone()
    try:
        dt = value
        if isinstance(dt, datetime):
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, tz)
        else:
            return ""

        now_ = now or timezone.now()
        if timezone.is_naive(now_):
            now_ = timezone.make_aware(now_, tz)

        if dt > now_:
            return "0m"

        raw = dj_timesince(dt, now_)

        # Convert "2 days, 3 hours" -> "2d 3h" etc.
        parts = []
        for chunk in raw.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            pieces = chunk.split()
            if not pieces:
                continue
            num = pieces[0]
            unit = pieces[1].lower() if len(pieces) > 1 else ""
            if unit.startswith("year"):
                suf = "y"
            elif unit.startswith("month"):
                suf = "mo"
            elif unit.startswith("week"):
                suf = "w"
            elif unit.startswith("day"):
                suf = "d"
            elif unit.startswith("hour"):
                suf = "h"
            elif unit.startswith("minute"):
                suf = "m"
            else:
                suf = ""
            parts.append(f"{num}{suf}" if suf else chunk)

        return " ".join(parts) or "0m"
    except Exception:
        return ""
