from __future__ import annotations

from datetime import datetime
from typing import Optional, Any

from django import template
from django.utils import timezone

# Prefer stdlib zoneinfo; fall back to pytz if needed
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

# Import canonical filters to avoid duplicate implementations
# This module simply proxies to dashboard_filters to prevent conflicts/recursion.
from . import dashboard_filters as _df  # type: ignore

register = template.Library()


def _ensure_aware(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _to_ist(dt: datetime) -> datetime:
    return _ensure_aware(dt).astimezone(IST)


@register.filter(name="delay_since")
def delay_since(value: Any, mode: str = "") -> str:
    """
    Proxy to the canonical implementation in dashboard_filters.delay_since.
    Keeps the same public name so templates that load this library continue to work.
    """
    try:
        return _df.delay_since(value, mode)
    except Exception:
        return "00:00"


@register.filter(name="priority_badge_class")
def priority_badge_class(priority: Optional[str]) -> str:
    return _df.priority_badge_class(priority)


@register.filter(name="status_badge_class")
def status_badge_class(status: Optional[str]) -> str:
    return _df.status_badge_class(status)


@register.filter(name="hhmm")
def hhmm(total_minutes: Optional[int]) -> str:
    return _df.hhmm(total_minutes)


@register.filter(name="istfmt")
def istdatetime_format(dt: Optional[datetime], fmt: str = "%d %b, %Y %H:%M") -> str:
    try:
        return _to_ist(dt).strftime(fmt) if dt else "â€”"
    except Exception:
        try:
            return _to_ist(dt).isoformat()
        except Exception:
            return "â€”"


@register.simple_tag
def now_ist(fmt: str = "%d %b, %Y %H:%M") -> str:
    try:
        return timezone.now().astimezone(IST).strftime(fmt)
    except Exception:
        return ""


@register.filter(name="coalesce")
def coalesce(value: Any, default: str = "â€”"):
    return value if value not in (None, "", [], {}) else default

