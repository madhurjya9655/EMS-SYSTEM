# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\reports.py
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, date, time as dt_time, timedelta
from typing import Iterable, List, Optional, Tuple

from django.http import HttpResponse
from django.utils import timezone
from django.db.models import Q

from .models import Checklist
from .recurrence_utils import RECURRING_MODES, IST


# -----------------------------
# Query DTO
# -----------------------------
@dataclass
class RecurringReportQuery:
    """
    Parameters to shape the recurring report.

    status:
      - "completed" → Checklist.status = Completed
      - "missed"    → status != Completed AND planned_date < now
      - "pending"   → status != Completed AND planned_date >= now
      - "all"       → no status narrowing (still only recurring modes)
    """
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    assign_to_id: Optional[int] = None
    group_name: Optional[str] = None
    status: str = "all"  # completed | missed | pending | all


# -----------------------------
# Helpers
# -----------------------------
def _minutes_between(later: Optional[datetime], earlier: Optional[datetime]) -> int:
    """Return non-negative whole minutes between two datetimes."""
    if not later or not earlier:
        return 0
    try:
        delta = later - earlier
    except Exception:
        # Fallback across timezones if needed
        later = timezone.make_naive(later, timezone.utc)
        earlier = timezone.make_naive(earlier, timezone.utc)
        delta = later - earlier
    mins = int(delta.total_seconds() // 60)
    return max(mins, 0)


def _now() -> datetime:
    """Aware 'now' (UTC-aware). We compare aware datetimes safely in Django."""
    return timezone.now()


def _apply_common_filters(qs, query: RecurringReportQuery):
    """Apply filters common to all report categories."""
    qs = qs.filter(mode__in=RECURRING_MODES)

    # Date range over the planned_date DATE portion
    if query.date_from:
        qs = qs.filter(planned_date__date__gte=query.date_from)
    if query.date_to:
        qs = qs.filter(planned_date__date__lte=query.date_to)

    if query.assign_to_id:
        qs = qs.filter(assign_to_id=query.assign_to_id)

    if query.group_name:
        qs = qs.filter(group_name__icontains=query.group_name.strip())

    return qs.select_related("assign_by", "assign_to")


def _status_filtered_queryset(query: RecurringReportQuery):
    """
    Return queryset narrowed by the requested status category.
    No duplicate kwargs; each condition applied exactly once.
    """
    base = _apply_common_filters(Checklist.objects.all(), query)
    now = _now()

    status = (query.status or "all").lower()
    if status == "completed":
        return base.filter(status="Completed")

    if status == "missed":
        # Missed = not completed and planned date < now
        return base.filter(~Q(status="Completed"), planned_date__lt=now)

    if status == "pending":
        # Pending/upcoming = not completed and planned date >= now
        return base.filter(~Q(status="Completed"), planned_date__gte=now)

    # "all" → no status constraint (only recurring modes + other filters)
    return base


# -----------------------------
# Public API
# -----------------------------
def generate_recurring_report(query: RecurringReportQuery) -> List[dict]:
    """
    Build a list of rows for the recurring tasks report.

    Columns:
      id, task_name, assign_to, planned_date, planned_date_ist, status,
      completed_at, completed_at_ist, delay_minutes, category
        - category ∈ {"Completed", "Missed", "Pending"}

    Delay:
      - Completed → minutes between completed_at and planned_date (>= 0)
      - Missed    → minutes between now and planned_date
      - Pending   → 0 if planned_date > now, else minutes between now and planned_date
                    (but since pending = planned_date >= now, it will be 0)
    """
    qs = _status_filtered_queryset(query).order_by("planned_date", "id")

    rows: List[dict] = []
    now = _now()

    for obj in qs:
        planned_dt = obj.planned_date
        planned_ist = planned_dt.astimezone(IST) if planned_dt else None

        comp_dt = obj.completed_at
        comp_ist = comp_dt.astimezone(IST) if comp_dt else None

        if obj.status == "Completed":
            category = "Completed"
            delay_mins = _minutes_between(comp_dt, planned_dt)
        else:
            # Not completed:
            if planned_dt and planned_dt < now:
                category = "Missed"
                delay_mins = _minutes_between(now, planned_dt)
            else:
                category = "Pending"
                delay_mins = 0

        rows.append({
            "id": obj.id,
            "task_name": obj.task_name,
            "assign_to": (obj.assign_to.get_full_name() or obj.assign_to.username) if obj.assign_to_id else "",
            "group_name": obj.group_name or "",
            "planned_date": planned_dt,
            "planned_date_ist": planned_ist,
            "status": obj.status,
            "completed_at": comp_dt,
            "completed_at_ist": comp_ist,
            "delay_minutes": delay_mins,
            "category": category,
            "priority": obj.priority,
            "mode": obj.mode,
            "frequency": obj.frequency,
        })

    return rows


def export_recurring_report_csv(query: RecurringReportQuery) -> HttpResponse:
    """
    Return an HttpResponse with CSV attachment for the recurring report.
    """
    rows = generate_recurring_report(query)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow([
        "ID", "Task Name", "Assign To", "Group", "Priority",
        "Mode", "Frequency",
        "Planned Date (Project TZ)", "Planned Date (IST)",
        "Status", "Completed At (Project TZ)", "Completed At (IST)",
        "Category", "Delay (minutes)"
    ])

    def fmt(dt: Optional[datetime]) -> str:
        return dt.strftime("%Y-%m-%d %H:%M") if dt else ""

    for r in rows:
        w.writerow([
            r["id"],
            r["task_name"],
            r["assign_to"],
            r["group_name"],
            r["priority"],
            r["mode"] or "",
            r["frequency"] or "",
            fmt(r["planned_date"]),
            fmt(r["planned_date_ist"]),
            r["status"],
            fmt(r["completed_at"]),
            fmt(r["completed_at_ist"]),
            r["category"],
            r["delay_minutes"],
        ])

    data = out.getvalue()
    out.close()

    resp = HttpResponse(data, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = 'attachment; filename="recurring_tasks_report.csv"'
    return resp
