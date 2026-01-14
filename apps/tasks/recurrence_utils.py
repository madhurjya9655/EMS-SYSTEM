from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta, tzinfo
from typing import Optional, Tuple

import pytz
from dateutil.relativedelta import relativedelta
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Visibility anchors (IST)
VISIBILITY_HOUR = 10
VISIBILITY_MINUTE = 0
DEFAULT_EVENING_HOUR = 19
DEFAULT_EVENING_MINUTE = 0


# -----------------------------
# Timezone helpers
# -----------------------------
def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """
    If naive, make aware in the *project timezone* (not IST).
    """
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt
    return timezone.make_aware(dt, timezone.get_current_timezone())


def _to_ist(dt: datetime) -> datetime:
    """
    Convert any aware/naive datetime to IST (naive assumed project TZ).
    """
    aware = _ensure_aware(dt)  # type: ignore[arg-type]
    return aware.astimezone(IST)  # type: ignore[union-attr]


def _from_ist(dt: datetime) -> datetime:
    """
    Convert an IST-aware datetime back to the project timezone.
    """
    return dt.astimezone(timezone.get_current_timezone())


# -----------------------------
# Working-day helpers
# (kept public, but recurrence helpers below DO NOT use these
# to avoid double-shifting; shifting is done by callers, e.g. signals)
# -----------------------------
def _lazy_holiday_model():
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday
    except Exception:
        return None


def is_working_day(d: date) -> bool:
    # Sunday == 6
    if d.weekday() == 6:
        return False
    Holiday = _lazy_holiday_model()
    if Holiday is None:
        return True
    return not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    for _ in range(0, 90):
        if is_working_day(d):
            return d
        d += timedelta(days=1)
    return d


# -----------------------------
# Mode normalization
# -----------------------------
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


# -----------------------------
# Core recurrence helpers
# -----------------------------
def _advance_date(ist_dt: datetime, mode: str, frequency: int) -> date:
    """
    Step the IST datetime by mode/frequency and return the resulting DATE (IST).
    """
    step = max(int(frequency or 1), 1)
    if mode == "Daily":
        return (ist_dt + relativedelta(days=step)).date()
    if mode == "Weekly":
        return (ist_dt + relativedelta(weeks=step)).date()
    if mode == "Monthly":
        return (ist_dt + relativedelta(months=step)).date()
    # Yearly
    return (ist_dt + relativedelta(years=step)).date()


def preserve_first_occurrence_time(planned_dt: Optional[datetime]) -> Optional[datetime]:
    """
    For initial creation, pin the planned datetime to 19:00 IST on the same date.
    Mirrors signal pre_save behavior; safe utility for callers that need the pin.
    """
    if planned_dt is None:
        return None
    ist_base = _to_ist(planned_dt)
    d = ist_base.date()
    fixed_ist = IST.localize(
        datetime.combine(d, dt_time(DEFAULT_EVENING_HOUR, DEFAULT_EVENING_MINUTE))
    )
    return _from_ist(fixed_ist)


def get_next_same_time(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Step forward by mode/frequency while preserving the *same wall-clock time*.
    No 7 PM pin. No working-day shift (callers handle shift if desired).

    Returns TZ-aware datetime in the project timezone.
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    prev_ist = _to_ist(prev_planned)

    # Preserve wall-clock time by stepping in IST
    if m == "Daily":
        nxt_ist = prev_ist + relativedelta(days=max(int(frequency or 1), 1))
    elif m == "Weekly":
        nxt_ist = prev_ist + relativedelta(weeks=max(int(frequency or 1), 1))
    elif m == "Monthly":
        nxt_ist = prev_ist + relativedelta(months=max(int(frequency or 1), 1))
    else:
        nxt_ist = prev_ist + relativedelta(years=max(int(frequency or 1), 1))

    if end_date and nxt_ist.date() > end_date:
        return None

    return _from_ist(nxt_ist)


def get_next_fixed_7pm(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
) -> Optional[datetime]:
    """
    Step by mode/frequency (date math in IST), then PIN to 19:00 IST on that stepped date.
    No working-day shift here — callers (e.g. signals) perform the shift.

    Returns TZ-aware datetime in the project timezone.
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    prev_ist = _to_ist(prev_planned)
    nxt_date = _advance_date(prev_ist, m, frequency)

    if end_date and nxt_date > end_date:
        return None

    nxt_ist = IST.localize(
        datetime.combine(nxt_date, dt_time(DEFAULT_EVENING_HOUR, DEFAULT_EVENING_MINUTE))
    )
    return _from_ist(nxt_ist)


def get_next_planned_date(
    prev_planned: datetime,
    mode: str,
    frequency: int,
) -> Optional[datetime]:
    """
    Back-compat wrapper (used by legacy code): same as get_next_fixed_7pm (no shift).
    """
    return get_next_fixed_7pm(prev_planned, mode, frequency)


def compute_next_planned_datetime(
    prev_planned: datetime,
    mode: str,
    frequency: int,
    *,
    end_date: Optional[date] = None,
    force_7pm: bool = False,  # kept for signature compatibility
) -> Optional[datetime]:
    """
    Preferred single entry-point for next occurrence under current rules.
    (Pin to 7 PM; no shift. Callers should shift to next working day if needed.)
    """
    return get_next_fixed_7pm(prev_planned, mode, frequency, end_date=end_date)


# -----------------------------
# Visibility logic (IST anchored)
# -----------------------------
def visibility_anchor_ist(due_planned: datetime) -> datetime:
    due_ist = _to_ist(due_planned)
    anchor = IST.localize(
        datetime.combine(due_ist.date(), dt_time(VISIBILITY_HOUR, VISIBILITY_MINUTE))
    )
    return anchor


def is_recurring_visible_now(due_planned: datetime, now: Optional[datetime] = None) -> bool:
    """
    For recurring items: visible if past date, or same date after 10:00 IST.
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
    Checklists/Delegations:
      - Past dates: visible
      - Today: visible if (planned time has arrived) OR (after 10:00 IST)
      - Future: hidden
    """
    if now is None:
        now = timezone.now()
    now_ist = _to_ist(now)
    due_ist = _to_ist(due_planned)
    if due_ist.date() < now_ist.date():
        return True
    if due_ist.date() > now_ist.date():
        return False
    if _ensure_aware(due_planned).astimezone(IST) <= now_ist:  # type: ignore[union-attr]
        return True
    return now_ist >= visibility_anchor_ist(due_planned)


def is_delegation_visible_now(due_planned: datetime, now: Optional[datetime] = None) -> bool:
    return is_checklist_visible_now(due_planned, now=now)


# -----------------------------
# Dashboard filtering helpers
# -----------------------------
@dataclass(frozen=True)
class DashboardCutoff:
    now_ist: datetime
    project_tz: tzinfo

    @classmethod
    def build(cls, now: Optional[datetime] = None) -> "DashboardCutoff":
        if now is None:
            now = timezone.now()
        return cls(now_ist=_to_ist(now), project_tz=timezone.get_current_timezone())

    def _same_ist_date(self, planned_date: datetime) -> bool:
        return _to_ist(planned_date).date() == self.now_ist.date()

    def should_show_checklist(
        self, *, planned_date: datetime, is_recurring: bool, today_only: bool
    ) -> bool:
        if today_only and not self._same_ist_date(planned_date):
            return False
        return is_checklist_visible_now(planned_date, now=self.now_ist)

    def should_show_delegation(self, *, planned_date: datetime, today_only: bool) -> bool:
        if today_only and not self._same_ist_date(planned_date):
            return False
        return is_delegation_visible_now(planned_date, now=self.now_ist)

    def should_show_help_ticket(self, *, planned_date: datetime, today_only: bool) -> bool:
        """
        Help Tickets are IMMEDIATE:
        - Past dates: always visible
        - Today: visible regardless of time (no 10:00 gate)
        - Future: hidden
        """
        d = _to_ist(planned_date).date()
        t = self.now_ist.date()
        if today_only:
            return d == t
        return d <= t


# -----------------------------
# IST wall-clock conversions
# -----------------------------
def extract_ist_wallclock(dt: datetime) -> Tuple[date, dt_time]:
    dt_ist = _to_ist(dt)
    return dt_ist.date(), dt_time(
        dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond
    )


def ist_wallclock_to_project_tz(d: date, t: dt_time) -> datetime:
    ist_dt = IST.localize(datetime.combine(d, t))
    return _from_ist(ist_dt)


__all__ = [
    "RECURRING_MODES",
    "normalize_mode",
    "is_working_day",
    "next_working_day",
    "preserve_first_occurrence_time",
    "get_next_same_time",
    "get_next_fixed_7pm",
    "get_next_planned_date",
    "compute_next_planned_datetime",
    "visibility_anchor_ist",
    "is_recurring_visible_now",
    "is_checklist_visible_now",
    "is_delegation_visible_now",
    "DashboardCutoff",
    "extract_ist_wallclock",
    "ist_wallclock_to_project_tz",
]
