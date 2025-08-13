# apps/tasks/email_utils.py
from typing import Iterable, List, Sequence, Optional, Dict, Any
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone
from datetime import datetime
import pytz
from datetime import time as _time

User = get_user_model()


# ----------------------------- helpers --------------------------------------- #

def _dedupe_emails(emails: Iterable[str]) -> List[str]:
    """Remove duplicates and empty values from an iterable of emails."""
    seen = set()
    out: List[str] = []
    for e in emails or []:
        e = (e or "").strip()
        if e and e not in seen:
            seen.add(e)
            out.append(e)
    return out


def get_admin_emails() -> List[str]:
    """
    Return a unique list of emails for users who should receive admin summaries:
    superusers + members of Admin/Manager/EA/CEO groups.
    """
    admins = list(
        User.objects.filter(is_active=True, is_superuser=True)
        .values_list("email", flat=True)
    )
    admins += list(
        User.objects.filter(
            is_active=True,
            groups__name__in=["Admin", "Manager", "EA", "CEO"]
        )
        .values_list("email", flat=True)
        .distinct()
    )
    return _dedupe_emails(admins)


def _fmt_value(v: Any) -> Any:
    """
    Sanitize/format values before passing to templates:
      - datetimes -> 'YYYY-MM-DD HH:MM' in local tz
      - users     -> full name or username
      - others    -> as-is
    """
    if isinstance(v, datetime):
        tz = timezone.get_current_timezone()
        aware = v if timezone.is_aware(v) else timezone.make_aware(v, tz)
        return aware.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    if hasattr(v, "get_full_name") or hasattr(v, "username"):
        try:
            name = getattr(v, "get_full_name", lambda: "")() or getattr(v, "username", "")
            return name
        except Exception:
            return str(v)
    return v


def _fmt_items(items: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    """Format a flat list of {'label','value'} rows for the admin summary template."""
    out: List[Dict[str, Any]] = []
    for row in items or []:
        out.append({
            "label": str(row.get("label", "")),
            "value": _fmt_value(row.get("value")),
        })
    return out


def _fmt_rows(rows: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    """Format table rows (list of dicts) for the admin summary template."""
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        new_row: Dict[str, Any] = {}
        for k, v in r.items():
            new_row[str(k)] = _fmt_value(v)
        out.append(new_row)
    return out


# -------- New helpers for unified user-facing templates ---------------------- #

IST = pytz.timezone("Asia/Kolkata")
_DEFAULT_ASSIGN_T = _time(10, 0)  # project default planning time

def _display_name(user) -> str:
    """Full name if available; else username; else 'System'."""
    if not user:
        return "System"
    try:
        full = getattr(user, "get_full_name", lambda: "")() or ""
        if str(full).strip():
            return str(full).strip()
        uname = getattr(user, "username", "") or ""
        return uname if uname else "System"
    except Exception:
        return "System"

def _fmt_dt_date(dt: Any) -> str:
    """
    IST string as 'YYYY-MM-DD' and add ' HH:MM' if time is meaningful
    (not 00:00 and not the default 10:00).
    """
    if not dt:
        return ""
    tz = timezone.get_current_timezone()
    aware = dt if timezone.is_aware(dt) else timezone.make_aware(dt, tz)
    ist = aware.astimezone(IST)
    base = ist.strftime("%Y-%m-%d")
    t = ist.timetz().replace(tzinfo=None)
    if t != _DEFAULT_ASSIGN_T and t != _time(0, 0):
        return f"{base} {ist.strftime('%H:%M')}"
    return base

def _send_unified_assignment_email(*, subject: str, to_email: str, context: Dict[str, Any]) -> None:
    """Render standardized TXT + HTML and send."""
    if not (to_email or "").strip():
        return
    text_body = render_to_string("email/task_assigned.txt", context)
    html_body = render_to_string("email/task_assigned.html", context)
    msg = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None),
        to=[to_email],
    )
    msg.attach_alternative(html_body, "text/html")
    msg.send(fail_silently=(getattr(settings, "EMAIL_FAIL_SILENTLY", False) or getattr(settings, "DEBUG", False)))


# ----------------------------- core sender ----------------------------------- #

def send_html_email(
    *,
    subject: str,
    template_name: str,
    context: Dict[str, Any],
    to: Sequence[str],
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    fail_silently: bool = False,
) -> None:
    """
    Render and send an HTML email using the given Django template.

    Notes:
    - In DEBUG (or if EMAIL_FAIL_SILENTLY=True), we never crash on send.
    - We lightly sanitize context for templates that expect 'items' or 'items_table'.
    """
    to_list = _dedupe_emails(to or [])
    cc_list = _dedupe_emails(cc or [])
    bcc_list = _dedupe_emails(bcc or [])
    if not to_list:
        return

    effective_fail_silently = (
        fail_silently
        or getattr(settings, "EMAIL_FAIL_SILENTLY", False)
        or getattr(settings, "DEBUG", False)
    )

    ctx = dict(context or {})
    if isinstance(ctx.get("items"), (list, tuple)):
        ctx["items"] = _fmt_items(ctx["items"])
    if isinstance(ctx.get("items_table"), (list, tuple)):
        ctx["items_table"] = _fmt_rows(ctx["items_table"])

    html_message = render_to_string(template_name, ctx)
    msg = EmailMultiAlternatives(
        subject=subject,
        body=html_message,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None)
                   or getattr(settings, "EMAIL_HOST_USER", None),
        to=to_list,
        cc=cc_list or None,
        bcc=bcc_list or None,
    )
    msg.attach_alternative(html_message, "text/html")
    msg.send(fail_silently=effective_fail_silently)


# -------- Specific helpers (Checklist, Delegation, Help Ticket) --------------- #

def send_checklist_assignment_to_user(
    *, task, complete_url: str, subject_prefix: str = "New Checklist Task Assigned"
) -> None:
    """Standardized user-facing email for Checklist."""
    if not task.assign_to or not (getattr(task.assign_to, "email", "") or "").strip():
        return
    ctx = {
        "kind": "Checklist",
        "task_title": task.task_name,
        "task_code": task.id,
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(task.assign_to),
        "complete_url": complete_url,
        "cta_text": "Press the button to mark the task as completed.",
    }
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {task.task_name}",
        to_email=task.assign_to.email,
        context=ctx,
    )


def send_delegation_assignment_to_user(
    *, delegation, complete_url: str, subject_prefix: str = "New Delegation Task Assigned"
) -> None:
    """Standardized user-facing email for Delegation."""
    if not delegation.assign_to or not (getattr(delegation.assign_to, "email", "") or "").strip():
        return
    ctx = {
        "kind": "Delegation",
        "task_title": delegation.task_name,
        "task_code": delegation.id,
        "planned_date_display": _fmt_dt_date(getattr(delegation, "planned_date", None)),
        "priority_display": getattr(delegation, "priority", "") or "",
        "assign_by_display": _display_name(getattr(delegation, "assign_by", None)),
        "assignee_name": _display_name(delegation.assign_to),
        "complete_url": complete_url,
        "cta_text": "Press the button to mark the task as completed.",
    }
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        to_email=delegation.assign_to.email,
        context=ctx,
    )


def send_help_ticket_assignment_to_user(
    *, ticket, complete_url: str, subject_prefix: str = "New Help Ticket Assigned"
) -> None:
    """Standardized user-facing email for Help Ticket."""
    if not ticket.assign_to or not (getattr(ticket.assign_to, "email", "") or "").strip():
        return
    ctx = {
        "kind": "Help Ticket",
        "task_title": ticket.title,
        "task_code": ticket.id,
        "planned_date_display": _fmt_dt_date(getattr(ticket, "planned_date", None)),
        "priority_display": getattr(ticket, "priority", "") or "",
        "assign_by_display": _display_name(getattr(ticket, "assign_by", None)),
        "assignee_name": _display_name(ticket.assign_to),
        "complete_url": complete_url,
        "cta_text": "Press the button to mark the ticket as closed / add notes.",
    }
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {ticket.title}",
        to_email=ticket.assign_to.email,
        context=ctx,
    )


# ---------------- Admin confirmations / summaries (unchanged) ---------------- #

def send_checklist_admin_confirmation(*, task, subject_prefix: str = "Checklist Task Assignment") -> None:
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": subject_prefix,
            "items": [
                {"label": "Task", "value": task.task_name},
                {"label": "Assignee", "value": task.assign_to},
                {"label": "Planned Date", "value": task.planned_date},
                {"label": "Priority", "value": task.priority},
            ],
        },
        to=admin_emails,
    )


def send_help_ticket_admin_confirmation(*, ticket, subject_prefix: str = "Help Ticket Assignment") -> None:
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": subject_prefix,
            "items": [
                {"label": "Title", "value": ticket.title},
                {"label": "Assignee", "value": ticket.assign_to},
                {"label": "Planned Date", "value": ticket.planned_date},
                {"label": "Priority", "value": ticket.priority},
            ],
        },
        to=admin_emails,
    )


def send_admin_bulk_summary(*, title: str, rows: Sequence[dict]) -> None:
    admin_emails = get_admin_emails()
    if not admin_emails or not rows:
        return
    send_html_email(
        subject=title,
        template_name="email/admin_assignment_summary.html",
        context={"title": title, "items_table": rows},
        to=admin_emails,
    )


# -------------------------- Unassigned notices (unchanged) ------------------- #

def send_checklist_unassigned_notice(*, task, old_user) -> None:
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"Checklist Task Unassigned: {task.task_name}",
        template_name="email/checklist_unassigned.html",
        context={"task": task, "old_user": old_user},
        to=[old_user.email],
    )


def send_help_ticket_unassigned_notice(*, ticket, old_user) -> None:
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    send_html_email(
        subject=f"Help Ticket Unassigned: {ticket.title}",
        template_name="email/help_ticket_unassigned.html",
        context={"ticket": ticket, "old_user": old_user},
        to=[old_user.email],
    )
