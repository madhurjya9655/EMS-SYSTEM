from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta, tzinfo
from typing import Optional, Tuple

import pytz
from dateutil.relativedelta import relativedelta
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

# ------------------------------
# Constants / Settings
# ------------------------------
IST = pytz.timezone("Asia/Kolkata")

# Valid recurring modes (canonical)
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Dashboard visibility gate
VISIBILITY_HOUR = 10  # 10:00 AM IST
VISIBILITY_MINUTE = 0

# Handy default evening time for recurring (if you choose to use it)
DEFAULT_EVENING_HOUR = 19  # 7:00 PM IST
DEFAULT_EVENING_MINUTE = 0


# ------------------------------
# Low-level datetime helpers
# ------------------------------
def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Return an aware datetime in the *project* timezone if possible; treat naive input as IST first."""
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    # Interpret naive input as IST wall-clock
    return IST.localize(dt)


def _to_ist(dt: datetime) -> datetime:
    """Convert any aware datetime to IST. If naive, first treat it as IST."""
    return _ensure_aware(dt).astimezone(IST)  # type: ignore[union-attr]


def _from_ist(dt: datetime) -> datetime:
    """Convert an IST aware datetime to the current project timezone (TIME_ZONE)."""
    return dt.astimezone(timezone.get_current_timezone())


def _lazy_holiday_model():
    """Import Holiday lazily to avoid app registry timing issues during startup/migrations."""
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday
    except Exception:
        return None


# ------------------------------
# Working day / mode utilities
# ------------------------------
def is_working_day(d: date) -> bool:
    """Working days are Mon–Sat, excluding configured holidays (Sunday = 6)."""
    if d.weekday() == 6:  # Sunday
        return False
    Holiday = _lazy_holiday_model()
    if Holiday is None:
        # If holiday model isn't available yet, treat only Sunday as non-working.
        return True
    return not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    """Move forward to the next working day (Mon–Sat and not a holiday)."""
    for _ in range(0, 90):  # safety cap
        if is_working_day(d):
            return d
        d += timedelta(days=1)
    return d


def normalize_mode(mode: Optional[str]) -> str:
    """
    Accepts flexible inputs (day/daily, week/weekly, month/monthly, year/yearly/annual).
    Returns one of: Daily, Weekly, Monthly, Yearly — or '' if invalid.
    """
    if not mode:
        return ""
    m = mode.strip().lower()
    if m in ("day", "daily"):
        return "Daily"
    if m in ("week", "weekly"):
        return "Weekly"
    if m in ("month", "monthly"):
        return "Monthly"
    if m in ("year", "yearly", "annual", "annually"):
        return "Yearly"
    return ""


# ------------------------------
# First occurrence handling
# ------------------------------
def preserve_first_occurrence_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    FIRST occurrence:
      • Keep EXACT date+time user set.
      • Naive input is treated as IST.
      • Do NOT shift off Sundays/holidays.
      • Return aware datetime in project timezone.
    """
    if planned_dt is None:
        return None
    ist_dt = _to_ist(planned_dt)
    return _from_ist(ist_dt)


# ------------------------------
# Recurrence computation
# ------------------------------
def _advance_date(ist_dt: datetime, mode: str, frequency: int) -> date:
    """Advance the IST datetime by mode/frequency and return the resulting *date* (drop time)."""
    step = max(int(frequency or 1), 1)
    if mode == "Daily":
        return (ist_dt + relativedelta(days=step)).date()
    if mode == "Weekly":
        return (ist_dt + relativedelta(weeks=step)).date()
    if mode == "Monthly":
        return (ist_dt + relativedelta(months=step)).date()
    # Yearly
    return (ist_dt + relativedelta(years=step)).date()


def get_next_same_time(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Compute next occurrence **preserving the SAME wall-clock time** as prev_planned.

    Steps:
      1) Advance by mode/frequency in IST.
      2) Keep the original time-of-day from prev_planned (IST).
      3) If Sunday/holiday → push forward to next working day (keeping the same time).
      4) If end_date provided and next date is AFTER it → return None (stop).
      5) Return aware datetime in project timezone.
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    prev_ist = _to_ist(prev_planned)
    wall_time = dt_time(prev_ist.hour, prev_ist.minute, prev_ist.second, prev_ist.microsecond)

    nxt_date = _advance_date(prev_ist, m, frequency)
    if not is_working_day(nxt_date):
        nxt_date = next_working_day(nxt_date)

    if end_date and nxt_date > end_date:
        return None

    nxt_ist = IST.localize(datetime.combine(nxt_date, wall_time))
    return _from_ist(nxt_ist)


def get_next_fixed_7pm(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Alternate helper: compute next occurrence at **19:00 IST** regardless of previous time.
    Useful if you decide all recurrences must be at 7 PM.
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    prev_ist = _to_ist(prev_planned)
    nxt_date = _advance_date(prev_ist, m, frequency)

    if not is_working_day(nxt_date):
        nxt_date = next_working_day(nxt_date)

    if end_date and nxt_date > end_date:
        return None

    nxt_ist = IST.localize(datetime.combine(nxt_date, dt_time(DEFAULT_EVENING_HOUR, DEFAULT_EVENING_MINUTE)))
    return _from_ist(nxt_ist)


# Backward/compat name (external callers)
def compute_next_planned_datetime(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
    force_7pm: bool = False,
) -> Optional[datetime]:
    """
    Public entrypoint for next-occurrence generation.
    • By default, preserves the same wall-clock time.
    • Set force_7pm=True to always schedule at 19:00 IST.
    """
    if force_7pm:
        return get_next_fixed_7pm(prev_planned, mode, frequency, end_date=end_date)
    return get_next_same_time(prev_planned, mode, frequency, end_date=end_date)


# ------------------------------
# Visibility (10:00 IST gating)
# ------------------------------
def visibility_anchor_ist(due_planned: datetime) -> datetime:
    """
    Return 10:00 IST of the due day (aware IST).
    This is the dashboard gate for checklists on the due day.
    """
    due_ist = _to_ist(due_planned)
    anchor = IST.localize(datetime.combine(due_ist.date(), dt_time(VISIBILITY_HOUR, VISIBILITY_MINUTE)))
    return anchor


def is_recurring_visible_now(due_planned: datetime, now: Optional[datetime] = None) -> bool:
    """
    Historical helper retained for compatibility:
      • Due day < today IST  → visible
      • Same day             → visible from 10:00 IST
      • Future day           → not visible
    """
    if now is None:
        now = timezone.now()
    now_ist = _to_ist(now)
    due_ist = _to_ist(due_planned)

    if due_ist.date() < now_ist.date():
        return True
    if due_ist.date() > now_ist.date():
        return False
    return now_ist >= visibility_anchor_ist(due_planned)


def is_checklist_visible_now(due_planned: datetime, now: Optional[datetime] = None) -> bool:
    """
    FINAL RULE (Checklist – recurring or one-time):
      • If due day < today IST      → visible (past-due remains until completed)
      • If due day > today IST      → not visible
      • If due day == today IST     → visible if (now >= planned_datetime) OR (now >= 10:00 IST)
        → i.e., always visible from 10:00 IST onwards on the due day, even if planned time is later (e.g., 19:00).
    """
    if now is None:
        now = timezone.now()
    now_ist = _to_ist(now)
    due_ist = _to_ist(due_planned)

    if due_ist.date() < now_ist.date():
        return True
    if due_ist.date() > now_ist.date():
        return False

    # Same day: show if planned time already reached, OR it's 10:00 IST or later.
    if _ensure_aware(due_planned).astimezone(IST) <= now_ist:  # type: ignore[union-attr]
        return True
    return now_ist >= visibility_anchor_ist(due_planned)


# ------------------------------
# Dashboard cutoffs (for views)
# ------------------------------
@dataclass(frozen=True)
class DashboardCutoff:
    """
    Encapsulates dynamic dashboard gating:
      • Checklist: use is_checklist_visible_now()
      • Delegation/HelpTicket: visible at/after planned datetime; past-due remains shown.
      • "today_only" narrows by IST date.
    """
    now_ist: datetime
    project_tz: tzinfo

    @classmethod
    def build(cls, now: Optional[datetime] = None) -> "DashboardCutoff":
        if now is None:
            now = timezone.now()
        return cls(now_ist=_to_ist(now), project_tz=timezone.get_current_timezone())

    def _same_ist_date(self, planned_date: datetime) -> bool:
        return _to_ist(planned_date).date() == self.now_ist.date()

    def should_show_checklist(self, *, planned_date: datetime, is_recurring: bool, today_only: bool) -> bool:
        if today_only and not self._same_ist_date(planned_date):
            return False
        return is_checklist_visible_now(planned_date, now=self.now_ist)

    def should_show_delegation(self, *, planned_date: datetime, today_only: bool) -> bool:
        due_ist = _to_ist(planned_date)
        if today_only and due_ist.date() != self.now_ist.date():
            return False
        # Visible when its planned time has arrived (or past)
        if _ensure_aware(planned_date).astimezone(IST) <= self.now_ist:  # type: ignore[union-attr]
            return True
        # Past days (shouldn't happen with filter, but safe):
        return due_ist.date() < self.now_ist.date()

    def should_show_help_ticket(self, *, planned_date: datetime, today_only: bool) -> bool:
        return self.should_show_delegation(planned_date=planned_date, today_only=today_only)


# ------------------------------
# Optional convenience exports
# ------------------------------
def extract_ist_wallclock(dt: datetime) -> Tuple[date, dt_time]:
    """Return (IST date, IST time) for a datetime."""
    dt_ist = _to_ist(dt)
    return dt_ist.date(), dt_time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    """Build an aware datetime in project tz from IST (date, time)."""
    ist_dt = IST.localize(datetime.combine(d, t))
    return _from_ist(ist_dt)


__all__ = [
    # Modes / working days
    "RECURRING_MODES",
    "normalize_mode",
    "is_working_day",
    "next_working_day",

    # First occurrence
    "preserve_first_occurrence_time",

    # Recurrence calculators
    "get_next_same_time",
    "get_next_fixed_7pm",
    "compute_next_planned_datetime",

    # Visibility gates
    "visibility_anchor_ist",
    "is_recurring_visible_now",
    "is_checklist_visible_now",
    "DashboardCutoff",

    # Utilities
    "extract_ist_wallclock",
    "ist_wallclock_to_project_tz",
]
