from __future__ import annotations

from datetime import timedelta
from django import template
from django.utils import timezone

register = template.Library()

@register.filter(name="modulo")
def modulo(value, arg):
    """
    Usage: {{ forloop.counter0|modulo:2 }}  ->  0/1 alternating
    Returns 0 on any error instead of crashing the template.
    """
    try:
        return int(value) % int(arg)
    except Exception:
        return 0

@register.filter(name="hhmm")
def hhmm(minutes):
    """Render minutes as HH:MM (e.g. 135 -> '02:15')."""
    try:
        m = int(float(minutes))
        return f"{m // 60:02d}:{m % 60:02d}"
    except Exception:
        return "00:00"

@register.filter(name="percent")
def percent(value, total):
    """
    Safe percentage helper: (value / total) * 100, rounded to 2 decimals.
    """
    try:
        total = float(total)
        value = float(value)
        if total == 0:
            return 0.0
        return round((value / total) * 100.0, 2)
    except Exception:
        return 0.0

@register.filter(name="delay_since")
def delay_since(dt, now=None):
    """
    Humanized delay since `dt`, similar to '2h 14m' / '3d 1h'.
    Kept here too so reports templates can use it when needed.
    """
    if not dt:
        return ""
    if now is None:
        now = timezone.now()
    try:
        delta: timedelta = now - dt
        secs = abs(int(delta.total_seconds()))
        days, rem = divmod(secs, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"
    except Exception:
        return ""
