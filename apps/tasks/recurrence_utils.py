from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta, tzinfo
from typing import Optional, Tuple

import pytz
from dateutil.relativedelta import relativedelta

from django.utils import timezone

from apps.settings.models import Holiday

# ------------------------------
# Constants
# ------------------------------
IST = pytz.timezone("Asia/Kolkata")
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]
VISIBILITY_HOUR = 10  # 10:00 AM IST visibility gate
VISIBILITY_MINUTE = 0


# ------------------------------
# Date helpers
# ------------------------------
def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    # Treat naive datetimes as IST-local input
    return IST.localize(dt)


def _to_ist(dt: datetime) -> datetime:
    return _ensure_aware(dt).astimezone(IST)  # type: ignore[union-attr]


def _from_ist(dt: datetime) -> datetime:
    return dt.astimezone(timezone.get_current_timezone())


def is_working_day(d: date) -> bool:
    """
    Working days = Mon–Sat and not a configured holiday.
    Monday=0 ... Sunday=6
    """
    if d.weekday() == 6:
        return False
    return not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def normalize_mode(mode: Optional[str]) -> str:
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
    - Keep EXACT date+time user chose.
    - If naive, interpret as IST.
    - Do NOT shift off Sundays/holidays.
    - Return in project timezone.
    """
    if planned_dt is None:
        return None
    ist_dt = _to_ist(planned_dt)
    return _from_ist(ist_dt)


# ------------------------------
# Recurrence computation (keep same planned time)
# ------------------------------
def get_next_same_time(prev_planned: datetime, mode: str, frequency: int) -> Optional[datetime]:
    """
    Compute next occurrence preserving the SAME wall-clock time as prev_planned.

    Steps:
      1) Move by `frequency` in the requested unit (day/week/month/year) — in IST.
      2) Keep the original time-of-day (HH:MM:SS.microsecond) from prev_planned (IST).
      3) If resulting date is Sunday/holiday → push to next working day, keeping the SAME time.
      4) Return aware datetime in the project timezone.

    Example:
      prev=2025-08-21 19:00 IST, mode=Weekly, freq=2 → 2025-09-04 19:00 IST (or later working day at 19:00).
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    step = max(int(frequency or 1), 1)

    prev_ist = _to_ist(prev_planned)
    wall_time = dt_time(prev_ist.hour, prev_ist.minute, prev_ist.second, prev_ist.microsecond)

    if m == "Daily":
        nxt_date = (prev_ist + relativedelta(days=step)).date()
    elif m == "Weekly":
        nxt_date = (prev_ist + relativedelta(weeks=step)).date()
    elif m == "Monthly":
        nxt_date = (prev_ist + relativedelta(months=step)).date()
    else:  # "Yearly"
        nxt_date = (prev_ist + relativedelta(years=step)).date()

    if not is_working_day(nxt_date):
        nxt_date = next_working_day(nxt_date)

    nxt_ist = IST.localize(datetime.combine(nxt_date, wall_time))
    return _from_ist(nxt_ist)


# ------------------------------
# Visibility helpers (10:00 IST gate)
# ------------------------------
def visibility_anchor_ist(due_planned: datetime) -> datetime:
    """
    10:00 IST on the due day of the task.
    Returned as an AWARE datetime in IST.
    """
    ist_dt = _to_ist(due_planned)
    anchor = IST.localize(datetime.combine(ist_dt.date(), dt_time(VISIBILITY_HOUR, VISIBILITY_MINUTE)))
    return anchor


def is_recurring_visible_now(due_planned: datetime, now: Optional[datetime] = None) -> bool:
    """
    Historical helper retained for compatibility:
    Recurring task is visible when:
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
    FINAL RULE (applies to Checklist tasks — recurring OR one-time):
      • If due day < today IST      → visible (past-due remains until completed)
      • If due day > today IST      → not visible
      • If due day == today IST     → visible if (now >= planned_datetime) OR (now >= 10:00 IST)
    """
    if now is None:
        now = timezone.now()
    now_ist = _to_ist(now)
    due_ist = _to_ist(due_planned)

    if due_ist.date() < now_ist.date():
        return True
    if due_ist.date() > now_ist.date():
        return False

    # Same day: show if planned time already passed, OR it's 10:00 IST or later.
    if _ensure_aware(due_planned).astimezone(IST) <= now_ist:  # type: ignore[union-attr]
        return True
    return now_ist >= visibility_anchor_ist(due_planned)


# ------------------------------
# Dashboard gating helpers
# ------------------------------
@dataclass(frozen=True)
class DashboardCutoff:
    """
    Encapsulates dynamic cutoffs for the dashboard.

    Default mode:
      - Checklist (recurring or one-time): is_checklist_visible_now()
      - Past-due remain until completed.

    Today-only mode:
      - Restrict to tasks whose planned_date DATE == today (IST).
      - Apply the same time-gating rule.
    """
    now_ist: datetime
    project_tz: tzinfo

    @classmethod
    def build(cls, now: Optional[datetime] = None) -> "DashboardCutoff":
        if now is None:
            now = timezone.now()
        return cls(now_ist=_to_ist(now), project_tz=timezone.get_current_timezone())

    def should_show_checklist(self, *, planned_date: datetime, is_recurring: bool, today_only: bool) -> bool:
        due_ist = _to_ist(planned_date)
        if today_only and due_ist.date() != self.now_ist.date():
            return False
        # Unified rule for checklist visibility
        return is_checklist_visible_now(planned_date, now=self.now_ist)

    def should_show_delegation(self, *, planned_date: datetime, today_only: bool) -> bool:
        # Delegations: immediate visibility at/after planned datetime; past-due remains.
        due_ist = _to_ist(planned_date)
        if today_only and due_ist.date() != self.now_ist.date():
            return False
        if due_ist.date() < self.now_ist.date():
            return True
        return _ensure_aware(planned_date).astimezone(IST) <= self.now_ist  # type: ignore[union-attr]

    def should_show_help_ticket(self, *, planned_date: datetime, today_only: bool) -> bool:
        # Help tickets behave like one-time tasks: immediate at/after planned datetime; past-due remains.
        return self.should_show_delegation(planned_date=planned_date, today_only=today_only)


# ------------------------------
# Convenience / Re-exports
# ------------------------------
def compute_next_planned_datetime(prev_planned: datetime, mode: str, frequency: int) -> Optional[datetime]:
    """Alias for external callers — preserves wall-clock and working-day shift."""
    return get_next_same_time(prev_planned, mode, frequency)


__all__ = [
    "RECURRING_MODES",
    "preserve_first_occurrence_time",
    "compute_next_planned_datetime",
    "get_next_same_time",
    "is_working_day",
    "next_working_day",
    "visibility_anchor_ist",
    "is_recurring_visible_now",
    "is_checklist_visible_now",
    "DashboardCutoff",
]
