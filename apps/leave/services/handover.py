from __future__ import annotations

import logging
from datetime import date
from typing import Iterable, List, Optional

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone
from django.utils.html import strip_tags
from zoneinfo import ZoneInfo

from apps.leave.models import LeaveHandover, LeaveRequest, LeaveStatus

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _now_date_ist() -> date:
    """Current date in IST (no time)."""
    return timezone.localtime(timezone.now(), IST).date()


def _handover_is_effective(h: LeaveHandover, *, today_ist: Optional[date] = None) -> bool:
    """Check if a handover row is active for *today* and leave is still valid."""
    if not h.is_active:
        return False
    if not h.leave_request or h.leave_request.status not in (LeaveStatus.PENDING, LeaveStatus.APPROVED):
        return False
    today = today_ist or _now_date_ist()
    if h.effective_start_date and today < h.effective_start_date:
        return False
    if h.effective_end_date and today > h.effective_end_date:
        return False
    return True


def _set_task_assignee(task_obj, new_user) -> bool:
    """
    Safely move `assign_to` on a task-like model if the attribute exists.
    Returns True if changed and saved.
    """
    try:
        if not hasattr(task_obj, "assign_to"):
            return False
        current = getattr(task_obj, "assign_to", None)
        if current == new_user:
            return False

        setattr(task_obj, "assign_to", new_user)

        update_fields = ["assign_to"]
        if hasattr(task_obj, "updated_at"):
            setattr(task_obj, "updated_at", timezone.now())
            update_fields.append("updated_at")

        task_obj.save(update_fields=update_fields)
        return True
    except Exception:
        logger.exception("Failed to update task assignee for %s", task_obj)
        return False


def apply_handover_for_leave(leave: LeaveRequest) -> int:
    """
    Apply active handovers for a given leave (idempotent).
    - Only moves tasks whose LeaveHandover is active *today* and the leave is PENDING/APPROVED.
    - Returns count of tasks whose assignee actually changed.
    """
    try:
        moved = 0
        today = _now_date_ist()
        handovers: Iterable[LeaveHandover] = (
            LeaveHandover.objects
            .select_related("leave_request", "original_assignee", "new_assignee")
            .filter(leave_request=leave, is_active=True)
        )
        for ho in handovers:
            if not _handover_is_effective(ho, today_ist=today):
                continue
            task = ho.get_task_object()
            if not task:
                continue
            if _set_task_assignee(task, ho.new_assignee):
                moved += 1
        if moved:
            logger.info("Handover applied for leave %s → moved %s task(s).", leave.id, moved)
        return moved
    except Exception:
        logger.exception("apply_handover_for_leave failed for leave %s", getattr(leave, "id", None))
        return 0


def _build_handover_email_context(leave: LeaveRequest, assignee, handovers: List[LeaveHandover]):
    rows = []
    for ho in handovers:
        rows.append({
            "type": ho.get_task_type_display(),
            "title": ho.get_task_title(),
            "url": ho.get_task_url(),
            "from_name": ho.original_assignee.get_full_name() or ho.original_assignee.username,
            "effective_end": ho.effective_end_date,
            "message": ho.message or "",
        })
    ctx = {
        "leave": leave,
        "assignee": assignee,
        "rows": rows,
        "from_ist": timezone.localtime(leave.start_at, IST),
        "to_ist": timezone.localtime(leave.end_at, IST),
        "employee_name": leave.employee.get_full_name() or leave.employee.username,
    }
    return ctx


def send_handover_email(leave: LeaveRequest, assignee, handovers: List[LeaveHandover]) -> bool:
    """
    Send a single summary email to the assignee listing all tasks handed over
    for this leave window. This signature matches usage from views and services.

    Returns True on success, False on failure. Failures are logged but never raised.
    """
    try:
        if not assignee or not getattr(assignee, "email", None):
            logger.warning("Handover email skipped: no assignee email.")
            return False

        ctx = _build_handover_email_context(leave, assignee, handovers)

        # Try to use a project template if present; otherwise render minimal inline HTML.
        try:
            html_body = render_to_string("leave/email_handover_summary.html", ctx)
        except Exception:
            # Fallback inline rendering
            lines = [
                f"<p>Hi {assignee.get_full_name() or assignee.username},</p>",
                f"<p>The following task(s) were handed over to you while <strong>{ctx['employee_name']}</strong> is on leave ",
                f"({ctx['from_ist']:%Y-%m-%d %H:%M} → {ctx['to_ist']:%Y-%m-%d %H:%M} IST):</p>",
                "<ul>",
            ]
            for r in ctx["rows"]:
                title = r["title"]
                link = r["url"] or "#"
                msg = f"<br><em>{r['message']}</em>" if r["message"] else ""
                end = f" (until {r['effective_end']})" if r["effective_end"] else ""
                lines.append(f"<li><strong>{r['type']}</strong>: <a href='{link}'>{title}</a>{end}{msg}</li>")
            lines.append("</ul>")
            lines.append("<p>Thanks.</p>")
            html_body = "".join(lines)

        text_body = strip_tags(html_body)
        subject = f"[Handover] Tasks assigned to you for {ctx['employee_name']}’s leave"

        conn = get_connection(
            username=getattr(settings, "EMAIL_HOST_USER", None),
            password=getattr(settings, "EMAIL_HOST_PASSWORD", None),
            fail_silently=True,
        )

        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None),
            to=[assignee.email],
            connection=conn,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send()

        logger.info("Handover email sent to %s for leave %s (%d items).", assignee.email, leave.id, len(handovers))
        return True

    except Exception:
        logger.exception("Failed sending handover email for leave %s to %s", getattr(leave, "id", None), getattr(assignee, "email", None))
        return False


__all__ = ["apply_handover_for_leave", "send_handover_email"]
