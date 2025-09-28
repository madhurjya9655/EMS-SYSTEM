# apps/leave/services/handover.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List
from datetime import datetime

import pytz
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone

from ..models import LeaveHandover

IST = pytz.timezone("Asia/Kolkata")


@dataclass
class HandoverItemDTO:
    task_name: str
    task_type: str
    task_id: int | None
    task_url: str | None
    message: str | None


def _format_ist(dt: datetime | None) -> str:
    if not dt:
        return ""
    if timezone.is_aware(dt):
        local = dt.astimezone(IST)
    else:
        local = IST.localize(dt)
    # readable but compact
    return local.strftime("%d %b %Y")


def _safe_full_name(user) -> str:
    try:
        return user.get_full_name() or user.username
    except Exception:
        return getattr(user, "username", "") or str(user)


def send_handover_email(handover: LeaveHandover, site_url: str) -> None:
    """
    Renders and sends the "leave_handover" email to the assignee.
    Depends on templates:
      templates/email/leave_handover.html
      templates/email/leave_handover.txt
    """
    # Pull leave window from leave_request if present (datetime) → IST dates
    lr = handover.leave_request
    # Try common attribute names; fall back to effective_* dates already stored
    start_dt = getattr(lr, "start_at", None) or getattr(lr, "start_date", None)
    end_dt = getattr(lr, "end_at", None) or getattr(lr, "end_date", None)

    start_at_ist = _format_ist(start_dt) if start_dt else handover.effective_start_date.strftime("%d %b %Y")
    end_at_ist = _format_ist(end_dt) if end_dt else handover.effective_end_date.strftime("%d %b %Y")

    # half-day flag is optional on your LeaveRequest
    is_half_day = bool(getattr(lr, "is_half_day", False))

    items: List[HandoverItemDTO] = []
    for it in handover.items.all():
        items.append(
            HandoverItemDTO(
                task_name=it.task_name or f"{it.get_task_type_display()} #{it.original_task_id}",
                task_type=it.task_type,
                task_id=it.original_task_id,
                task_url=it.task_url or None,
                message=it.message or None,
            )
        )

    ctx = {
        "assignee_name": _safe_full_name(handover.new_assignee),
        "employee_name": _safe_full_name(handover.employee),
        "employee_email": handover.employee_email,
        "leave_type": handover.leave_type,
        "start_at_ist": start_at_ist,
        "end_at_ist": end_at_ist,
        "duration_days": handover.duration_days(),
        "is_half_day": is_half_day,
        "handover_message": handover.handover_message or "",
        "handovers": [asdict(i) for i in items],
        "site_url": site_url.rstrip("/"),
    }

    subject = f"Task Handover: {ctx['employee_name']} → You ({ctx['start_at_ist']} to {ctx['end_at_ist']})"

    text_body = render_to_string("email/leave_handover.txt", ctx)
    html_body = render_to_string("email/leave_handover.html", ctx)

    to_email = getattr(handover.new_assignee, "email", None)
    if not to_email:
        return  # silently skip if assignee has no email

    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "SERVER_EMAIL", None) or "no-reply@example.com"

    msg = EmailMultiAlternatives(subject, text_body, from_email, [to_email])
    msg.attach_alternative(html_body, "text/html")
    msg.send(fail_silently=True)
