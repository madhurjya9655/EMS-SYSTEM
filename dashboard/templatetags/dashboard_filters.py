from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
import math

import pytz
from django import template
from django.utils import timezone

register = template.Library()

IST = pytz.timezone("Asia/Kolkata")


def _to_aware(dt: datetime) -> datetime | None:
    if not dt:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


def _to_ist(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    dt = _to_aware(dt)
    return dt.astimezone(IST) if dt else None


def _humanize_minutes(total_minutes: int, signed: bool = False) -> str:
    """
    Convert minutes to a compact human string.
    Examples:
      0  -> "0m"
      5  -> "5m"
      61 -> "1h 1m"
      180 -> "3h"
      -15 -> "-15m"   (if signed=True)
    """
    sign = ""
    mins = total_minutes

    if signed and mins < 0:
        sign = "-"
        mins = abs(mins)

    if mins == 0:
        return f"{sign}0m"

    h = mins // 60
    m = mins % 60
    if h and m:
        return f"{sign}{h}h {m}m"
    if h:
        return f"{sign}{h}h"
    return f"{sign}{m}m"


@register.filter(name="delay_since")
def delay_since(planned_dt: datetime, mode: str = "") -> str:
    """
    Return the time delta between *now* and `planned_dt`, humanized.

    - If `mode == "signed"`, show negative values for future items (e.g., "-2h").
    - Otherwise (default), never show negatives; clamp to "0m" for future.

    Always computed in IST to match dashboard rules.
    """
    if not planned_dt:
        return "—"

    p = _to_ist(planned_dt)
    if p is None:
        return "—"

    now = _now_ist()
    diff = now - p
    minutes = int(math.floor(diff.total_seconds() / 60.0))

    if mode == "signed":
        return _humanize_minutes(minutes, signed=True)

    # Unsigned mode (default): clamp future to 0
    return _humanize_minutes(max(0, minutes), signed=False)


@register.filter(name="ist_datetime")
def ist_datetime(dt: datetime, fmt: str = "%d %b, %Y %H:%M") -> str:
    """
    Convenience: format any datetime in IST using the given format.
    """
    if not dt:
        return "—"
    ist = _to_ist(dt)
    if not ist:
        return "—"
    try:
        return ist.strftime(fmt)
    except Exception:
        return ist.isoformat()


@register.filter(name="bool_badge")
def bool_badge(val: bool) -> str:
    """
    Render a simple Yes/No badge text (used in some tables).
    """
    return "Yes" if bool(val) else "No"
