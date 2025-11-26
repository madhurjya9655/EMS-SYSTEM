# apps/tasks/views_reports.py
from __future__ import annotations

from datetime import datetime, date
from typing import Optional, Tuple

from django.http import HttpResponse, HttpResponseBadRequest
from django.utils.html import escape
from django.contrib.auth.decorators import login_required, user_passes_test
from django.views.decorators.http import require_GET

from .reports import (
    RecurringReportQuery,
    generate_recurring_report,
    export_recurring_report_csv,
)

def _is_admin(user) -> bool:
    """Allow superusers, staff, or members of Admin/Manager/EA/CEO groups."""
    if not user.is_authenticated or not user.is_active:
        return False
    if user.is_superuser or user.is_staff:
        return True
    return user.groups.filter(name__in=["Admin", "Manager", "EA", "CEO"]).exists()

def _parse_int(s: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """
    Returns (value, error). Only basic validation; keeps logic unchanged elsewhere.
    """
    if s is None or s == "":
        return None, None
    try:
        return int(s), None
    except (TypeError, ValueError):
        return None, "assign_to_id must be an integer"

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

@require_GET
@login_required
@user_passes_test(_is_admin)
def recurring_report(request):
    """
    Admin-only recurring tasks report.

    Query params:
      - date_from, date_to: YYYY-MM-DD (also accepts DD-MM-YYYY / DD/MM/YYYY)
      - assign_to_id: int
      - group_name: str
      - status: completed | missed | pending | all   (default: all)
      - format: csv | html                           (default: html)
    """
    # Validate status early
    raw_status = (request.GET.get("status") or "all").lower().strip()
    valid_status = {"completed", "missed", "pending", "all"}
    if raw_status not in valid_status:
        return HttpResponseBadRequest(
            f"Invalid status '{escape(raw_status)}'. Allowed: completed | missed | pending | all"
        )

    # Validate assign_to_id if provided
    assign_to_id, err = _parse_int(request.GET.get("assign_to_id"))
    if err:
        return HttpResponseBadRequest(escape(err))

    q = RecurringReportQuery(
        date_from=_parse_date(request.GET.get("date_from") or request.GET.get("from")),
        date_to=_parse_date(request.GET.get("date_to") or request.GET.get("to")),
        assign_to_id=assign_to_id,
        group_name=(request.GET.get("group_name") or request.GET.get("group")),
        status=raw_status,
    )

    # CSV download
    if (request.GET.get("format") or "html").lower() == "csv":
        return export_recurring_report_csv(q)

    # Minimal HTML (no template dependency)
    rows = generate_recurring_report(q)
    html_parts = [
        "<html><head><meta charset='utf-8'><title>Recurring Report</title>",
        "<style>body{font-family:Arial,sans-serif}table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ddd;padding:6px}th{background:#f5f5f5;text-align:left}"
        ".muted{color:#666}</style></head><body>",
        "<h2>Recurring Tasks Report</h2>",
        "<p class='muted'>Use <code>?format=csv</code> to download as CSV.</p>",
        "<table><thead><tr>"
        "<th>ID</th><th>Task</th><th>Assignee</th><th>Group</th>"
        "<th>Priority</th><th>Mode</th><th>Freq</th>"
        "<th>Planned (TZ)</th><th>Planned (IST)</th>"
        "<th>Status</th><th>Completed (TZ)</th><th>Completed (IST)</th>"
        "<th>Category</th><th>Delay (min)</th>"
        "</tr></thead><tbody>",
    ]
    for r in rows:
        def fmt(x): return "" if x is None else escape(str(x))
        html_parts.append(
            "<tr>"
            f"<td>{fmt(r['id'])}</td>"
            f"<td>{fmt(r['task_name'])}</td>"
            f"<td>{fmt(r['assign_to'])}</td>"
            f"<td>{fmt(r['group_name'])}</td>"
            f"<td>{fmt(r['priority'])}</td>"
            f"<td>{fmt(r['mode'])}</td>"
            f"<td>{fmt(r['frequency'])}</td>"
            f"<td>{fmt(r['planned_date'])}</td>"
            f"<td>{fmt(r['planned_date_ist'])}</td>"
            f"<td>{fmt(r['status'])}</td>"
            f"<td>{fmt(r['completed_at'])}</td>"
            f"<td>{fmt(r['completed_at_ist'])}</td>"
            f"<td>{fmt(r['category'])}</td>"
            f"<td>{fmt(r['delay_minutes'])}</td>"
            "</tr>"
        )
    html_parts.append("</tbody></table></body></html>")
    return HttpResponse("".join(html_parts), content_type="text/html; charset=utf-8")
