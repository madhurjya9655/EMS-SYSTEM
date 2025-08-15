from __future__ import annotations

from django import template
from django.utils import timezone
from datetime import datetime

register = template.Library()


def _to_aware(dt: datetime | None) -> datetime | None:
    """Return an aware datetime in the current TZ."""
    if dt is None:
        return None
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        # treat naive datetimes as current TZ
        return timezone.make_aware(dt, tz)
    return timezone.localtime(dt, tz)


@register.filter(name="delay_since")
def delay_since(planned_dt: datetime | None) -> str:
    """
    Human-friendly delay elapsed from planned_dt until now.
    Examples:
      - "—" (no date)
      - "on time" (planned is in the future)
      - "15 minutes"
      - "2 hours"
      - "1 day"
    """
    planned = _to_aware(planned_dt)
    if not planned:
        return "—"

    now = timezone.now()
    if planned > now:
        return "on time"

    delta = now - planned
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins} minute" + ("" if mins == 1 else "s")

    hours = mins // 60
    if hours < 24:
        return f"{hours} hour" + ("" if hours == 1 else "s")

    days = hours // 24
    return f"{days} day" + ("" if days == 1 else "s")


@register.filter(name="minutes_to_hhmm")
def minutes_to_hhmm(total_minutes) -> str:
    """
    Convert raw minutes into HH:MM (e.g., 10 -> 00:10, 130 -> 02:10).
    Graceful on bad input.
    """
    try:
        m = int(total_minutes or 0)
    except Exception:
        m = 0
    h = m // 60
    mm = m % 60
    return f"{h:02d}:{mm:02d}"