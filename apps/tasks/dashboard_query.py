from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta
from typing import Dict, List, Tuple

import pytz
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from .recurrence import RECURRING_MODES  # source of truth

# --------------------------
# Timezone helpers
# --------------------------
IST = pytz.timezone("Asia/Kolkata")


def _to_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt.astimezone(IST)


def _ist_day_bounds_in_project_tz(anchor: date) -> Tuple[datetime, datetime]:
    """
    Returns (start_of_day, end_of_day) for the IST date 'anchor', converted to project TZ.
    """
    tz = timezone.get_current_timezone()
    start_ist = timezone.make_aware(datetime.combine(anchor, dt_time.min), IST)
    end_ist = timezone.make_aware(datetime.combine(anchor, dt_time.max), IST)
    return start_ist.astimezone(tz), end_ist.astimezone(tz)


def _end_of_today_project_tz(now: datetime) -> datetime:
    """Return end-of-today (23:59:59.999) IST converted to project timezone."""
    now_ist = _to_ist(now)
    _, end_proj = _ist_day_bounds_in_project_tz(now_ist.date())
    return end_proj


# --------------------------
# Visibility gating helpers
# --------------------------
def is_checklist_visible_now(planned_dt: datetime, *, now: datetime | None = None) -> bool:
    """
    Unified visibility rule for CHECKLISTS — STRICT 10:00 AM IST gating:
      • If planned day < today (IST)  -> visible (past-due stays visible)
      • If planned day > today (IST)  -> not visible
      • If same day (IST)             -> visible IFF now_IST >= 10:00 (ignore planned time)
    """
    if not now:
        now = timezone.now()

    now_ist = _to_ist(now)
    due_ist = _to_ist(planned_dt)

    if due_ist.date() < now_ist.date():
        return True
    if due_ist.date() > now_ist.date():
        return False

    # Same IST day → strictly from 10:00 IST
    ten_am_ist = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return now_ist >= ten_am_ist


# --------------------------
# Gating per model
# --------------------------
def _is_checklist_recurring(obj: Checklist) -> bool:
    try:
        return (obj.mode or "") in RECURRING_MODES and int(obj.frequency or 0) > 0
    except Exception:
        return False


def _gate_checklist(obj: Checklist, now: datetime, today_only: bool) -> bool:
    """
    Returns True if the checklist should be shown on the dashboard at `now`
    using the unified visibility rule.
    """
    now_ist = _to_ist(now)
    due_ist = _to_ist(obj.planned_date)

    if today_only and due_ist.date() != now_ist.date():
        return False

    return is_checklist_visible_now(obj.planned_date, now=now)


def _gate_delegation(obj: Delegation, now: datetime, today_only: bool) -> bool:
    """
    Delegations: immediate visibility at/after planned datetime; past-due remains visible.
    """
    now_ist = _to_ist(now)
    due_ist = _to_ist(obj.planned_date)

    if today_only and due_ist.date() != now_ist.date():
        return False

    if due_ist.date() < now_ist.date():
        return True

    return obj.planned_date <= now


def _gate_help_ticket(obj: HelpTicket, now: datetime, today_only: bool) -> bool:
    """
    Help tickets: immediate visibility at/after planned datetime; past-due remains.
    """
    return _gate_delegation(obj, now, today_only)


# --------------------------
# Public API
# --------------------------
@dataclass(frozen=True)
class DashboardLists:
    checklists: List[Checklist]
    delegations: List[Delegation]
    help_tickets: List[HelpTicket]

    def selected(self, kind: str | None):
        if kind == "delegation":
            return self.delegations
        if kind == "help_ticket":
            return self.help_tickets
        return self.checklists


def fetch_dashboard_lists(
    *,
    user,
    today_only: bool = False,
    selected: str | None = None,
) -> DashboardLists:
    """
    Fetch dashboard lists for a specific user, enforcing visibility rules.

    Filtering strategy:
      • DB pre-filter to reduce volume (status and day bounds).
      • Python-level gating to enforce unified visibility (strict 10:00 IST for checklists).
    """
    now = timezone.now()
    now_ist = _to_ist(now)
    start_today_proj, end_today_proj = _ist_day_bounds_in_project_tz(now_ist.date())

    # Choose a DB cutoff:
    #  - For "today_only": we include today's whole IST day in DB filter,
    #    then Python gating will restrict visibility.
    #  - Otherwise: include everything up to end-of-today (IST) in project TZ.
    cutoff_all = end_today_proj

    # --- Base DB filters (do not create recurrences here) ---
    # Checklists: include up to end-of-day; if today_only, constrain to today's bounds.
    cl_base_qs = Checklist.objects.filter(assign_to=user, status="Pending", planned_date__lte=cutoff_all)
    if today_only:
        cl_base_qs = cl_base_qs.filter(planned_date__gte=start_today_proj, planned_date__lte=end_today_proj)
    cl_base = cl_base_qs.select_related("assign_by", "assign_to").order_by("planned_date", "id")

    # Delegations/HelpTickets: visible at/after planned time; for today_only,
    # DB-filter to today's items up to *now* for efficiency.
    if today_only:
        dl_base = (
            Delegation.objects.filter(assign_to=user, status="Pending",
                                      planned_date__gte=start_today_proj, planned_date__lte=now)
            .select_related("assign_by", "assign_to")
            .order_by("planned_date", "id")
        )
        ht_base = (
            HelpTicket.objects.filter(assign_to=user,
                                      planned_date__gte=start_today_proj, planned_date__lte=now)
            .exclude(status="Closed")
            .select_related("assign_by", "assign_to")
            .order_by("planned_date", "id")
        )
    else:
        dl_base = (
            Delegation.objects.filter(assign_to=user, status="Pending", planned_date__lte=cutoff_all)
            .select_related("assign_by", "assign_to")
            .order_by("planned_date", "id")
        )
        ht_base = (
            HelpTicket.objects.filter(assign_to=user, planned_date__lte=cutoff_all)
            .exclude(status="Closed")
            .select_related("assign_by", "assign_to")
            .order_by("planned_date", "id")
        )

    # --- Python-level gating ---
    checklists: List[Checklist] = [c for c in cl_base if _gate_checklist(c, now, today_only)]
    delegations: List[Delegation] = [d for d in dl_base if _gate_delegation(d, now, today_only)]
    help_tickets: List[HelpTicket] = [h for h in ht_base if _gate_help_ticket(h, now, today_only)]

    return DashboardLists(
        checklists=checklists,
        delegations=delegations,
        help_tickets=help_tickets,
    )


def compute_week_buckets(
    *,
    user,
    anchor_today_ist: date | None = None,
) -> Tuple[Tuple[datetime, datetime], Tuple[datetime, datetime]]:
    """
    Return ((prev_start, prev_end), (curr_start, curr_end)) as aware datetimes in project TZ,
    where weeks are Mon..Sun in IST and 'current' ends today (IST).
    """
    now_ist = _to_ist(timezone.now())
    if anchor_today_ist is None:
        anchor_today_ist = now_ist.date()

    # Week boundaries (Mon..Sun)
    start_current = anchor_today_ist - timedelta(days=anchor_today_ist.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    tz = timezone.get_current_timezone()

    def _bounds(d_from: date, d_to_incl: date) -> Tuple[datetime, datetime]:
        start = timezone.make_aware(datetime.combine(d_from, dt_time.min), IST).astimezone(tz)
        end = timezone.make_aware(datetime.combine(d_to_incl, dt_time.max), IST).astimezone(tz)
        return start, end

    prev = _bounds(start_prev, end_prev)
    curr = _bounds(start_current, anchor_today_ist)
    return prev, curr


def weekly_score_counts(*, user) -> Dict[str, Dict[str, int]]:
    """
    Mirror of dashboard "week score" counters using the planned_date field (sacred).
    Returns dict of:
    {
      'checklist':  {'previous': int, 'current': int},
      'delegation': {'previous': int, 'current': int},
      'help_ticket':{'previous': int, 'current': int},
    }
    """
    (prev_start, prev_end), (curr_start, curr_end) = compute_week_buckets(user=user)

    checklist_prev = Checklist.objects.filter(
        assign_to=user, planned_date__gte=prev_start, planned_date__lte=prev_end, status="Completed"
    ).count()
    checklist_curr = Checklist.objects.filter(
        assign_to=user, planned_date__gte=curr_start, planned_date__lte=curr_end, status="Completed"
    ).count()

    delegation_prev = Delegation.objects.filter(
        assign_to=user, planned_date__gte=prev_start, planned_date__lte=prev_end, status="Completed"
    ).count()
    delegation_curr = Delegation.objects.filter(
        assign_to=user, planned_date__gte=curr_start, planned_date__lte=curr_end, status="Completed"
    ).count()

    help_prev = HelpTicket.objects.filter(
        assign_to=user, planned_date__gte=prev_start, planned_date__lte=prev_end, status="Closed"
    ).count()
    help_curr = HelpTicket.objects.filter(
        assign_to=user, planned_date__gte=curr_start, planned_date__lte=curr_end, status="Closed"
    ).count()

    return {
        "checklist": {"previous": checklist_prev, "current": checklist_curr},
        "delegation": {"previous": delegation_prev, "current": delegation_curr},
        "help_ticket": {"previous": help_prev, "current": help_curr},
    }


def pending_counts(*, user) -> Dict[str, int]:
    """Pending totals (without visibility gating) for quick header badges."""
    return {
        "checklist": Checklist.objects.filter(assign_to=user, status="Pending").count(),
        "delegation": Delegation.objects.filter(assign_to=user, status="Pending").count(),
        "help_ticket": HelpTicket.objects.filter(assign_to=user).exclude(status="Closed").count(),
    }


__all__ = [
    "fetch_dashboard_lists",
    "DashboardLists",
    "weekly_score_counts",
    "pending_counts",
    "is_checklist_visible_now",
]
