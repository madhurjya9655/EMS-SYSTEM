# apps/tasks/dashboard_query.py
# Updated: 2026-02-14

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta
from typing import Dict, List, Tuple

import pytz
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket

# ✅ Source of truth (matches tasks.py/signals/materializer)
from .recurrence_utils import RECURRING_MODES

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
    """Return end-of-today (23:59:59.999999) IST converted to project timezone."""
    now_ist = _to_ist(now)
    _, end_proj = _ist_day_bounds_in_project_tz(now_ist.date())
    return end_proj


def _exclude_voided(qs):
    """
    Exclude voided/skipped rows if the model has the boolean field.
    This keeps the query safe even if migrations are not applied yet.
    """
    try:
        # Django model field existence check (safe)
        Checklist._meta.get_field("is_skipped_due_to_leave")  # type: ignore[attr-defined]
        return qs.filter(is_skipped_due_to_leave=False)
    except Exception:
        return qs


def _exclude_voided_any_model(qs, model):
    try:
        model._meta.get_field("is_skipped_due_to_leave")  # type: ignore[attr-defined]
        return qs.filter(is_skipped_due_to_leave=False)
    except Exception:
        return qs


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

    IMPORTANT:
      - Excludes voided/skipped rows (is_skipped_due_to_leave=True).
        This flag is also used as a generic “voided” tombstone to prevent
        recurring materializers from recreating deleted occurrences.
    """
    now = timezone.now()
    now_ist = _to_ist(now)
    start_today_proj, end_today_proj = _ist_day_bounds_in_project_tz(now_ist.date())

    cutoff_all = end_today_proj

    # --- Base DB filters (do not create recurrences here) ---
    cl_base_qs = Checklist.objects.filter(
        assign_to=user,
        status="Pending",
        planned_date__lte=cutoff_all,
    )
    cl_base_qs = _exclude_voided(cl_base_qs)

    if today_only:
        cl_base_qs = cl_base_qs.filter(planned_date__gte=start_today_proj, planned_date__lte=end_today_proj)

    cl_base = cl_base_qs.select_related("assign_by", "assign_to").order_by("planned_date", "id")

    if today_only:
        dl_base_qs = Delegation.objects.filter(
            assign_to=user,
            status="Pending",
            planned_date__gte=start_today_proj,
            planned_date__lte=now,
        )
        dl_base_qs = _exclude_voided_any_model(dl_base_qs, Delegation)

        ht_base_qs = HelpTicket.objects.filter(
            assign_to=user,
            planned_date__gte=start_today_proj,
            planned_date__lte=now,
        ).exclude(status="Closed")
        ht_base_qs = _exclude_voided_any_model(ht_base_qs, HelpTicket)
    else:
        dl_base_qs = Delegation.objects.filter(
            assign_to=user,
            status="Pending",
            planned_date__lte=cutoff_all,
        )
        dl_base_qs = _exclude_voided_any_model(dl_base_qs, Delegation)

        ht_base_qs = HelpTicket.objects.filter(
            assign_to=user,
            planned_date__lte=cutoff_all,
        ).exclude(status="Closed")
        ht_base_qs = _exclude_voided_any_model(ht_base_qs, HelpTicket)

    dl_base = dl_base_qs.select_related("assign_by", "assign_to").order_by("planned_date", "id")
    ht_base = ht_base_qs.select_related("assign_by", "assign_to").order_by("planned_date", "id")

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
    (prev_start, prev_end), (curr_start, curr_end) = compute_week_buckets(user=user)

    cl_prev_qs = Checklist.objects.filter(
        assign_to=user,
        planned_date__gte=prev_start,
        planned_date__lte=prev_end,
        status="Completed",
    )
    cl_prev_qs = _exclude_voided(cl_prev_qs)

    cl_curr_qs = Checklist.objects.filter(
        assign_to=user,
        planned_date__gte=curr_start,
        planned_date__lte=curr_end,
        status="Completed",
    )
    cl_curr_qs = _exclude_voided(cl_curr_qs)

    dl_prev_qs = Delegation.objects.filter(
        assign_to=user,
        planned_date__gte=prev_start,
        planned_date__lte=prev_end,
        status="Completed",
    )
    dl_prev_qs = _exclude_voided_any_model(dl_prev_qs, Delegation)

    dl_curr_qs = Delegation.objects.filter(
        assign_to=user,
        planned_date__gte=curr_start,
        planned_date__lte=curr_end,
        status="Completed",
    )
    dl_curr_qs = _exclude_voided_any_model(dl_curr_qs, Delegation)

    ht_prev_qs = HelpTicket.objects.filter(
        assign_to=user,
        planned_date__gte=prev_start,
        planned_date__lte=prev_end,
        status="Closed",
    )
    ht_prev_qs = _exclude_voided_any_model(ht_prev_qs, HelpTicket)

    ht_curr_qs = HelpTicket.objects.filter(
        assign_to=user,
        planned_date__gte=curr_start,
        planned_date__lte=curr_end,
        status="Closed",
    )
    ht_curr_qs = _exclude_voided_any_model(ht_curr_qs, HelpTicket)

    return {
        "checklist": {"previous": cl_prev_qs.count(), "current": cl_curr_qs.count()},
        "delegation": {"previous": dl_prev_qs.count(), "current": dl_curr_qs.count()},
        "help_ticket": {"previous": ht_prev_qs.count(), "current": ht_curr_qs.count()},
    }


def pending_counts(*, user) -> Dict[str, int]:
    cl_qs = Checklist.objects.filter(assign_to=user, status="Pending")
    cl_qs = _exclude_voided(cl_qs)

    dl_qs = Delegation.objects.filter(assign_to=user, status="Pending")
    dl_qs = _exclude_voided_any_model(dl_qs, Delegation)

    ht_qs = HelpTicket.objects.filter(assign_to=user).exclude(status="Closed")
    ht_qs = _exclude_voided_any_model(ht_qs, HelpTicket)

    return {
        "checklist": cl_qs.count(),
        "delegation": dl_qs.count(),
        "help_ticket": ht_qs.count(),
    }


__all__ = [
    "fetch_dashboard_lists",
    "DashboardLists",
    "weekly_score_counts",
    "pending_counts",
    "is_checklist_visible_now",
]
