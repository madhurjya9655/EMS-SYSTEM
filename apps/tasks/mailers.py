# tasks/mailers.py
from __future__ import annotations

import logging
from typing import Optional, List

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.template import TemplateDoesNotExist

logger = logging.getLogger(__name__)


def _email_list(*vals: Optional[str]) -> List[str]:
    return [v for v in vals if v]


def _user_name(u) -> str:
    if not u:
        return "—"
    full = getattr(u, "get_full_name", lambda: "")() or ""
    return full or getattr(u, "username", "") or str(u)


def _render_fallback_text(prefix: str, task, is_recurring: bool) -> str:
    from django.utils import timezone
    lines = [
        f"{prefix} Assigned" + (" (Recurring)" if is_recurring else ""),
        "",
        f"Task: {getattr(task, 'task_name', 'Task')}",
        f"Assigned By: {_user_name(getattr(task, 'assign_by', None))}",
        f"Assigned To: {_user_name(getattr(task, 'assign_to', None))}",
    ]
    planned = getattr(task, "planned_date", None)
    if planned:
        lines.append(f"Planned Date: {timezone.localtime(planned).strftime('%d %b, %Y %H:%M')}")
    priority = getattr(task, "priority", None)
    if priority:
        disp = getattr(task, "get_priority_display", lambda: priority)()
        lines.append(f"Priority: {disp}")
    msg = getattr(task, "message", None) or getattr(task, "description", None)
    if msg:
        lines.extend(["", "Description:", str(msg)])
    return "\n".join(lines)


def _send_assignment_email(task, *, subject_prefix: str, template_base: str, is_recurring: bool) -> bool:
    try:
        assign_to = getattr(task, "assign_to", None)
        to_email = getattr(assign_to, "email", None)
        if not to_email:
            logger.info(
                "Assignment email skipped for task %s: no assignee email (assignee=%s)",
                getattr(task, "id", "?"),
                _user_name(assign_to),
            )
            return False

        notify_to = getattr(task, "notify_to", None)
        cc = []
        if notify_to and getattr(notify_to, "email", None):
            cc.append(notify_to.email)

        subject = f"{subject_prefix}{' • Recurring' if is_recurring else ''} {getattr(task, 'task_name', 'Task')}"

        context = {"task": task, "is_recurring": is_recurring}

        try:
            html_body = render_to_string(f"emails/{template_base}.html", context)
        except TemplateDoesNotExist:
            html_body = None

        try:
            text_body = render_to_string(f"emails/{template_base}.txt", context)
        except TemplateDoesNotExist:
            text_body = _render_fallback_text(subject_prefix.strip("[]"), task, is_recurring)

        from_email = getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(
            settings, "SERVER_EMAIL", "no-reply@example.com"
        )
        reply_to = None
        assign_by = getattr(task, "assign_by", None)
        if assign_by and getattr(assign_by, "email", None):
            reply_to = [assign_by.email]

        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=from_email,
            to=[to_email],
            cc=cc or None,
            reply_to=reply_to,
        )
        if html_body:
            msg.attach_alternative(html_body, "text/html")
        msg.send()

        logger.info(
            "Assignment email sent for task %s to %s (cc=%s, recurring=%s)",
            getattr(task, "id", "?"),
            to_email,
            ", ".join(cc) if cc else "—",
            is_recurring,
        )
        return True

    except Exception as e:
        logger.exception(
            "Failed to send assignment email for task %s (recurring=%s): %s",
            getattr(task, "id", "?"),
            is_recurring,
            e,
        )
        return False


def send_checklist_assignment_email(task, *, is_recurring: bool = False) -> bool:
    return _send_assignment_email(
        task,
        subject_prefix="[Checklist]",
        template_base="checklist_assigned",
        is_recurring=is_recurring,
    )


def send_delegation_assignment_email(task, *, is_recurring: bool = False) -> bool:
    return _send_assignment_email(
        task,
        subject_prefix="[Delegation]",
        template_base="delegation_assigned",
        is_recurring=is_recurring,
    )
