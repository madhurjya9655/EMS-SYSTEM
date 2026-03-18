# apps/tasks/views_reports.py
from __future__ import annotations

import csv
import logging
from typing import Optional

import pytz
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation
from .views import is_admin_user, clean_unicode_string

logger = logging.getLogger(__name__)
User = get_user_model()
IST = pytz.timezone("Asia/Kolkata")


# =============================================================================
# recurring_report — preserved from original
# Shows all recurring series with pending/completed breakdown.
# =============================================================================
@login_required
def recurring_report(request):
    if not is_admin_user(request.user):
        messages.error(request, "You do not have permission to view reports.")
        return redirect(reverse("tasks:list_checklist"))

    series_qs = (
        Checklist.objects
        .filter(mode__isnull=False, is_skipped_due_to_leave=False)
        .exclude(mode__exact="")
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .annotate(
            total=Count("id"),
            pending=Count("id", filter=Q(status="Pending")),
            completed=Count("id", filter=Q(status="Completed")),
        )
        .order_by("task_name")
    )

    user_ids = {s["assign_to_id"] for s in series_qs}
    users_map = {u.id: u for u in User.objects.filter(id__in=user_ids)}
    series = list(series_qs)
    for s in series:
        s["assign_to_user"] = users_map.get(s["assign_to_id"])

    return render(request, "tasks/recurring_report.html", {
        "series": series,
        "current_tab": "reports",
    })


# =============================================================================
# checklist_report — NEW employee-wise performance & delay report
#
# ACCESS:  Admin / Manager / EA / CEO / Superuser ONLY.
# PURPOSE: Track ALL tasks (pending + completed + deleted) per employee
#          for performance tracking and delay analysis.
#          This is NOT the checklist view. Checklist ≠ Reports.
#
# KEY DIFFERENCES from checklist view:
#   - Shows ALL statuses by default (not just Pending)
#   - Shows completed_at and delay calculations
#   - Shows soft-deleted rows (is_skipped_due_to_leave=True) labelled "Deleted"
#   - Employee-wise summary with avg delay, overdue count
#   - CSV export for HR/management use
# =============================================================================
@login_required
def checklist_report(request):
    if not is_admin_user(request.user):
        messages.error(request, "You do not have permission to view reports.")
        return redirect(reverse("tasks:list_checklist"))

    # ── Filter params ──────────────────────────────────────────────────────────
    employee_id  = request.GET.get("employee",  "").strip()
    start_date   = request.GET.get("start_date", "").strip()
    end_date     = request.GET.get("end_date",   "").strip()
    status_param = request.GET.get("status",     "all").strip()
    task_type    = request.GET.get("task_type",  "checklist").strip()

    # ── Helper: apply shared filters to any QS ────────────────────────────────
    def _apply_filters(qs):
        if employee_id:
            try:
                qs = qs.filter(assign_to_id=int(employee_id))
            except (ValueError, TypeError):
                pass
        if start_date:
            qs = qs.filter(planned_date__date__gte=start_date)
        if end_date:
            qs = qs.filter(planned_date__date__lte=end_date)
        if status_param and status_param != "all":
            qs = qs.filter(status=status_param)
        return qs

    items = []
    summary_by_employee: dict = {}
    now = timezone.now()

    # ── Checklist tasks ────────────────────────────────────────────────────────
    if task_type in ("checklist", "all"):
        qs = (
            Checklist.objects
            # Reports deliberately include ALL rows: active, completed, soft-deleted.
            # is_skipped_due_to_leave rows appear as "Deleted" — visible for audit.
            .select_related("assign_by", "assign_to")
            .defer("media_upload", "doer_file")
            .order_by("assign_to__first_name", "assign_to__username", "-planned_date")
        )
        qs = _apply_filters(qs)

        for obj in qs:
            delay_minutes = _calc_delay(obj.planned_date, obj.completed_at, obj.status)
            row = _build_row(
                task_id=f"CL-{obj.id}",
                task_type="Checklist",
                task_name=obj.task_name,
                assign_by=obj.assign_by,
                assign_to=obj.assign_to,
                planned_date=obj.planned_date,
                completed_at=obj.completed_at,
                status=obj.status,
                priority=obj.priority,
                is_deleted=obj.is_skipped_due_to_leave,
                delay_minutes=delay_minutes,
                now=now,
            )
            items.append(row)
            _accum_summary(summary_by_employee, obj.assign_to_id, obj.assign_to,
                           row, delay_minutes, now)

    # ── Delegation tasks ───────────────────────────────────────────────────────
    if task_type in ("delegation", "all"):
        qs = (
            Delegation.objects
            .select_related("assign_by", "assign_to")
            .defer("doer_file")
            .order_by("assign_to__first_name", "assign_to__username", "-planned_date")
        )
        qs = _apply_filters(qs)

        for obj in qs:
            delay_minutes = _calc_delay(obj.planned_date, obj.completed_at, obj.status)
            row = _build_row(
                task_id=f"DL-{obj.id}",
                task_type="Delegation",
                task_name=obj.task_name,
                assign_by=obj.assign_by,
                assign_to=obj.assign_to,
                planned_date=obj.planned_date,
                completed_at=obj.completed_at,
                status=obj.status,
                priority=obj.priority,
                is_deleted=obj.is_skipped_due_to_leave,
                delay_minutes=delay_minutes,
                now=now,
            )
            items.append(row)
            _accum_summary(summary_by_employee, obj.assign_to_id, obj.assign_to,
                           row, delay_minutes, now)

    # ── Finalise per-employee avg delay ───────────────────────────────────────
    for s in summary_by_employee.values():
        if s["_delay_count"] > 0:
            avg = s["_delay_sum"] // s["_delay_count"]
            s["avg_delay_minutes"] = avg
            s["avg_delay_display"] = _fmt_delay(avg)
        else:
            s["avg_delay_minutes"] = None
            s["avg_delay_display"] = "—"
        del s["_delay_sum"]
        del s["_delay_count"]

    summary_list = sorted(
        summary_by_employee.values(),
        key=lambda x: (
            getattr(x["employee"], "first_name", "") or
            getattr(x["employee"], "username", "") or ""
        ).lower(),
    )

    # ── CSV download ───────────────────────────────────────────────────────────
    if request.GET.get("download"):
        return _export_csv(items)

    return render(request, "tasks/checklist_report.html", {
        "items": items,
        "summary_list": summary_list,
        "total_tasks": len(items),
        "employees": User.objects.filter(is_active=True).order_by("first_name", "last_name", "username"),
        "selected_employee": employee_id,
        "start_date": start_date,
        "end_date": end_date,
        "status_param": status_param,
        "task_type": task_type,
        "current_tab": "reports",
        "is_admin": True,
    })


# =============================================================================
# Internal helpers
# =============================================================================

def _calc_delay(planned_date, completed_at, status) -> Optional[int]:
    """
    Returns delay in minutes (positive = late, negative = early).
    Only meaningful for Completed tasks.
    """
    if status != "Completed":
        return None
    if not completed_at or not planned_date:
        return None
    try:
        delta = completed_at - planned_date
        return int(delta.total_seconds() // 60)
    except Exception:
        return None


def _fmt_delay(minutes: Optional[int]) -> str:
    if minutes is None:
        return "—"
    if minutes < 0:
        return f"Early {abs(minutes)} min"
    if minutes == 0:
        return "On time"
    if minutes < 60:
        return f"+{minutes} min late"
    hours = minutes // 60
    mins = minutes % 60
    if hours < 24:
        return f"+{hours}h {mins}m late" if mins else f"+{hours}h late"
    days = hours // 24
    rem = hours % 24
    return f"+{days}d {rem}h late" if rem else f"+{days}d late"


def _build_row(*, task_id, task_type, task_name, assign_by, assign_to,
               planned_date, completed_at, status, priority,
               is_deleted, delay_minutes, now) -> dict:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "task_name": task_name,
        "assign_by": assign_by,
        "assign_to": assign_to,
        "planned_date": planned_date,
        "completed_at": completed_at,
        "status": status,
        "priority": priority or "Low",
        "is_deleted": is_deleted,
        "delay_minutes": delay_minutes,
        "delay_display": _fmt_delay(delay_minutes),
        "is_overdue": (
            status == "Pending" and
            not is_deleted and
            planned_date is not None and
            planned_date < now
        ),
    }


def _accum_summary(summary: dict, uid, employee, row: dict,
                   delay_minutes: Optional[int], now) -> None:
    if uid not in summary:
        summary[uid] = {
            "employee": employee,
            "total": 0,
            "pending": 0,
            "completed": 0,
            "overdue": 0,
            "deleted": 0,
            "_delay_sum": 0,
            "_delay_count": 0,
        }
    s = summary[uid]
    s["total"] += 1
    if row["is_deleted"]:
        s["deleted"] += 1
    elif row["status"] == "Pending":
        s["pending"] += 1
        if row["is_overdue"]:
            s["overdue"] += 1
    elif row["status"] == "Completed":
        s["completed"] += 1
        if delay_minutes is not None:
            s["_delay_sum"] += delay_minutes
            s["_delay_count"] += 1


def _export_csv(items: list) -> HttpResponse:
    resp = HttpResponse(content_type="text/csv")
    resp["Content-Disposition"] = 'attachment; filename="task_performance_report.csv"'
    w = csv.writer(resp)
    w.writerow([
        "Task ID", "Type", "Task Name",
        "Assigned By", "Assigned To",
        "Planned Date", "Completed Date",
        "Status", "Priority",
        "Delay", "Deleted/Skipped",
    ])
    for row in items:
        def _name(u):
            if not u:
                return ""
            return u.get_full_name() or u.username

        w.writerow([
            row["task_id"],
            row["task_type"],
            clean_unicode_string(row["task_name"]),
            _name(row["assign_by"]),
            _name(row["assign_to"]),
            row["planned_date"].strftime("%Y-%m-%d %H:%M") if row["planned_date"] else "",
            row["completed_at"].strftime("%Y-%m-%d %H:%M") if row["completed_at"] else "",
            row["status"],
            row["priority"],
            row["delay_display"],
            "Yes" if row["is_deleted"] else "No",
        ])
    return resp