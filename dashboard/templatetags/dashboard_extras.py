# E:\CLIENT PROJECT\employee management system bos\employee_management_system\dashboard\templatetags\dashboard_extras.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from django import template
from django.utils import timezone

# Prefer stdlib zoneinfo; fall back to pytz if needed
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

register = template.Library()


def _ensure_aware(dt: datetime) -> datetime:
    """Return an aware datetime in the project TZ if input is naive."""
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _to_ist(dt: datetime) -> datetime:
    """Coerce datetime (naive/aware) to aware IST."""
    return _ensure_aware(dt).astimezone(IST)


def _parse_any_datetime(val) -> Optional[datetime]:
    """
    Accept a datetime or (rarely) a string and return a datetime.
    Returns None if it can't be parsed.
    """
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        s = val.strip()
        # Try ISO first
        try:
            return datetime.fromisoformat(s)
        except Exception:
            pass
        # Common fallbacks
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                    "%d %b, %Y %H:%M", "%d %b %Y %H:%M"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None


def _fmt_hhmm(total_minutes: int) -> str:
    h = max(total_minutes, 0) // 60
    m = max(total_minutes, 0) % 60
    return f"{h:02d}:{m:02d}"


@register.filter(name="delay_since")
def delay_since(value, now=None) -> str:
    """
    Delay (display only) = max(0, now_IST - planned_IST), formatted HH:MM.

    Rules:
      - If now < planned -> "00:00"
      - Robust to naive/aware datetimes and even strings.
      - On any parsing/conversion issue, return "00:00" (never blank).

    NOTE: Visibility gating ("Today Only" vs "Show All") is handled in views;
          this filter strictly formats the delay for the cell.
    """
    # Parse planned datetime
    planned_raw = _parse_any_datetime(value)
    if not planned_raw:
        return "00:00"  # safer than empty

    try:
        planned_ist = _to_ist(planned_raw)

        # 'now' override for tests, else current time
        if isinstance(now, datetime):
            now_dt = now
        else:
            now_dt = timezone.now()
        now_ist = _to_ist(now_dt)

        # Before or exactly at planned time => no delay
        if now_ist <= planned_ist:
            return "00:00"

        # Floor minutes
        minutes = int((now_ist - planned_ist).total_seconds() // 60)
        return _fmt_hhmm(minutes)
    except Exception:
        # Never show a blank cell; be safe and show 00:00
        return "00:00"
