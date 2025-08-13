# apps/tasks/utils.py
"""
Backwards-compatible task utilities.

- Re-exports email helpers so existing imports from `.utils` keep working.
- Adds a helper to preserve the FIRST occurrence datetime exactly as entered
  (manual or bulk), interpreting naive datetimes as IST and returning an
  aware datetime in the project's timezone.

If you already import directly from `.email_utils`, that continues to work.
"""

from typing import Iterable, List, Sequence, Optional, Dict, Any
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone

User = get_user_model()


def _dedupe_emails(emails: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for e in emails:
        e = (e or "").strip()
        if e and e not in seen:
            seen.add(e)
            out.append(e)
    return out


def get_admin_emails() -> List[str]:
    qs = User.objects.filter(is_active=True, is_superuser=True).values_list("email", flat=True)
    admins = list(qs)
    admins += list(
        User.objects.filter(is_active=True, groups__name__in=["Admin", "Manager"])
        .values_list("email", flat=True)
        .distinct()
    )
    return _dedupe_emails(admins)


def _fmt_value(v: Any) -> Any:
    """
    Sanitize/format values before sending into templates:
      - datetimes -> 'YYYY-MM-DD HH:MM' in local tz
      - users -> full name or username
      - everything else -> as-is (stringified by template)
    Avoids templates trying to introspect Python attributes.
    """
    from datetime import datetime
    if isinstance(v, datetime):
        tz = timezone.get_current_timezone()
        aware = v if timezone.is_aware(v) else timezone.make_aware(v, tz)
        return aware.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    if hasattr(v, "get_full_name") or hasattr(v, "username"):
        try:
            return v.get_full_name() or v.username
        except Exception:
            pass
    return v


def _fmt_items(items: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    out = []
    for row in items or []:
        out.append({
            "label": str(row.get("label", "")),
            "value": _fmt_value(row.get("value")),
        })
    return out


def _fmt_rows(rows: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    out = []
    for r in rows or []:
        new_row = {}
        for k, v in r.items():
            new_row[str(k)] = _fmt_value(v)
        out.append(new_row)
    return out


def send_html_email(
    *,
    subject: str,
    template_name: str,
    context: Dict[str, Any],
    to: Sequence[str],
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    fail_silently: bool = False,
):
    to_list = _dedupe_emails(to or [])
    cc_list = _dedupe_emails(cc or [])
    bcc_list = _dedupe_emails(bcc or [])
    if not to_list:
        return

    # In DEBUG or if EMAIL_FAIL_SILENTLY is set, never crash on email send
    effective_fail_silently = (
        fail_silently
        or getattr(settings, "EMAIL_FAIL_SILENTLY", False)
        or getattr(settings, "DEBUG", False)
    )

    # Sanitize context payloads to keep templates simple/safe
    ctx = dict(context or {})
    if "items" in ctx and isinstance(ctx["items"], (list, tuple)):
        ctx["items"] = _fmt_items(ctx["items"])
    if "items_table" in ctx and isinstance(ctx["items_table"], (list, tuple)):
        ctx["items_table"] = _fmt_rows(ctx["items_table"])

    html_message = render_to_string(template_name, ctx)
    msg = EmailMultiAlternatives(
        subject=subject,
        body=html_message,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None),
        to=to_list,
        cc=cc_list or None,
        bcc=bcc_list or None,
    )
    msg.attach_alternative(html_message, "text/html")
    msg.send(fail_silently=effective_fail_silently)


# -------- Specific helpers (Checklist, Delegation, Help Ticket) ---------------

def send_checklist_assignment_to_user(*, task, complete_url: str, subject_prefix: str = "New Checklist Task Assigned"):
    if not task.assign_to or not (getattr(task.assign_to, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/checklist_assigned.html",
        context={
            "task": task,
            "assign_by": task.assign_by,
            "assign_to": task.assign_to,
            "complete_url": complete_url,
        },
        to=[task.assign_to.email],
    )


def send_checklist_admin_confirmation(*, task, subject_prefix: str = "Checklist Task Assignment"):
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix}",
            "items": [{
                "label": "Task",
                "value": task.task_name,
            }, {
                "label": "Assignee",
                "value": task.assign_to,
            }, {
                "label": "Planned Date",
                "value": task.planned_date,
            }, {
                "label": "Priority",
                "value": task.priority,
            }],
        },
        to=admin_emails,
    )


def send_checklist_unassigned_notice(*, task, old_user):
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"Checklist Task Unassigned: {task.task_name}",
        template_name="email/checklist_unassigned.html",
        context={
            "task": task,
            "old_user": old_user,
        },
        to=[old_user.email],
    )


def send_delegation_assignment_to_user(*, delegation, complete_url: str, subject_prefix: str = "New Delegation Task Assigned"):
    if not delegation.assign_to or not (getattr(delegation.assign_to, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        template_name="email/delegation_assigned.html",
        context={
            "delegation": delegation,
            "assign_by": delegation.assign_by,
            "assign_to": delegation.assign_to,
            "complete_url": complete_url,
        },
        to=[delegation.assign_to.email],
    )


def send_help_ticket_assignment_to_user(*, ticket, complete_url: str, subject_prefix: str = "New Help Ticket Assigned"):
    if not ticket.assign_to or not (getattr(ticket.assign_to, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/help_ticket_assigned.html",
        context={
            "ticket": ticket,
            "assign_by": ticket.assign_by,
            "assign_to": ticket.assign_to,
            "complete_url": complete_url,
        },
        to=[ticket.assign_to.email],
    )


def send_help_ticket_admin_confirmation(*, ticket, subject_prefix: str = "Help Ticket Assignment"):
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix}",
            "items": [{
                "label": "Title",
                "value": ticket.title,
            }, {
                "label": "Assignee",
                "value": ticket.assign_to,
            }, {
                "label": "Planned Date",
                "value": ticket.planned_date,
            }, {
                "label": "Priority",
                "value": ticket.priority,
            }],
        },
        to=admin_emails,
    )


def send_help_ticket_unassigned_notice(*, ticket, old_user):
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"Help Ticket Unassigned: {ticket.title}",
        template_name="email/help_ticket_unassigned.html",
        context={
            "ticket": ticket,
            "old_user": old_user,
        },
        to=[old_user.email],
    )


def send_admin_bulk_summary(*, title: str, rows: Sequence[dict]):
    admin_emails = get_admin_emails()
    if not admin_emails or not rows:
        return
    send_html_email(
        subject=title,
        template_name="email/admin_assignment_summary.html",
        context={"title": title, "items_table": rows},
        to=admin_emails,
    )
