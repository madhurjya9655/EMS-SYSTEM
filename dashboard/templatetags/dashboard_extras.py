from __future__ import annotations

from datetime import datetime, time as dt_time
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

register = template.Library()


# -----------------------------
# Datetime helpers
# -----------------------------
def _ensure_aware(dt: datetime) -> datetime:
    """Return an aware datetime in the project TZ if input is naive."""
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _to_ist(dt: datetime) -> datetime:
    """Coerce datetime (naive/aware) to aware IST."""
    return _ensure_aware(dt).astimezone(IST)


def _parse_any_datetime(val: Any) -> Optional[datetime]:
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
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d %b, %Y %H:%M",
            "%d %b %Y %H:%M",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None


def _fmt_hhmm(total_minutes: int) -> str:
    mins = max(int(total_minutes or 0), 0)
    h = mins // 60
    m = mins % 60
    return f"{h:02d}:{m:02d}"


# -----------------------------
# Public filters/tags
# -----------------------------
@register.filter(name="delay_since")
def delay_since(value: Any, now: Any = None) -> str:
    """
    Delay (display only) = max(0, now_IST - planned_IST), formatted HH:MM.

    Rules:
      - If now < planned -> "00:00"
      - Robust to naive/aware datetimes and even strings.
      - On any parsing/conversion issue, return "00:00" (never blank).

    NOTE: Visibility gating is handled in views; this filter strictly formats
          the delay for the cell.
    """
    planned_raw = _parse_any_datetime(value)
    if not planned_raw:
        return "00:00"

    try:
        planned_ist = _to_ist(planned_raw)

        # 'now' override for tests; else current time
        if isinstance(now, datetime):
            now_dt = now
        else:
            now_dt = timezone.now()
        now_ist = _to_ist(now_dt)

        if now_ist <= planned_ist:
            return "00:00"

        minutes = int((now_ist - planned_ist).total_seconds() // 60)
        return _fmt_hhmm(minutes)
    except Exception:
        return "00:00"


@register.filter(name="priority_badge_class")
def priority_badge_class(priority: Optional[str]) -> str:
    """
    Map 'High'/'Medium'/'Low' -> bootstrap badge classes.
    """
    p = (priority or "").strip().lower()
    if p == "high":
        return "danger"
    if p == "medium":
        return "warning text-dark"
    if p == "low":
        return "success"
    return "secondary"


@register.filter(name="status_badge_class")
def status_badge_class(status: Optional[str]) -> str:
    """
    Map common task statuses to bootstrap badge classes.
    """
    s = (status or "").strip().lower()
    if s in {"pending", "open", "in progress"}:
        return "warning text-dark"
    if s in {"completed", "closed", "done"}:
        return "success"
    if s in {"blocked", "on hold"}:
        return "secondary"
    return "secondary"


@register.filter(name="hhmm")
def hhmm(total_minutes: Optional[int]) -> str:
    """Render minutes as HH:MM (zero-padded)."""
    try:
        return _fmt_hhmm(int(total_minutes or 0))
    except (TypeError, ValueError):
        return "00:00"


@register.filter(name="istfmt")
def istdatetime_format(dt: Optional[datetime], fmt: str = "%d %b, %Y %H:%M") -> str:
    """Format any datetime in IST using the given format string."""
    if not dt:
        return "—"
    try:
        return _to_ist(dt).strftime(fmt)
    except Exception:
        try:
            return _to_ist(dt).isoformat()
        except Exception:
            return "—"


@register.simple_tag
def now_ist(fmt: str = "%d %b, %Y %H:%M") -> str:
    """Current time in IST (handy for headers/debug)."""
    try:
        return timezone.now().astimezone(IST).strftime(fmt)
    except Exception:
        return ""


@register.filter(name="coalesce")
def coalesce(value: Any, default: str = "—"):
    """Return value if truthy, else the default glyph."""
    return value if value not in (None, "", [], {}) else default
