# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\utils.py

from typing import Iterable, List, Sequence, Optional, Dict, Any
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone
from django.urls import reverse
from datetime import datetime
import pytz
from datetime import time as dt_time

User = get_user_model()
IST = pytz.timezone("Asia/Kolkata")
_DEFAULT_ASSIGN_T = dt_time(10, 0)  # project default planning time

# Get site URL for complete URLs
site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")


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
    """Sanitize/format values before sending into templates"""
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
    """Format datetime for display"""
    if not dt:
        return ""
    tz = timezone.get_current_timezone()
    aware = dt if timezone.is_aware(dt) else timezone.make_aware(dt, tz)
    ist = aware.astimezone(IST)
    base = ist.strftime("%Y-%m-%d")
    t = ist.timetz().replace(tzinfo=None)
    if t != _DEFAULT_ASSIGN_T and t != dt_time(0, 0):
        return f"{base} {ist.strftime('%H:%M')}"
    return base


def _send_unified_assignment_email(*, subject: str, to_email: str, context: Dict[str, Any]) -> None:
    """Render standardized TXT + HTML and send."""
    if not (to_email or "").strip():
        return
    
    # Render both text and HTML versions
    text_body = render_to_string("email/task_assigned.txt", context)
    html_body = render_to_string("email/task_assigned.html", context)
    
    msg = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None),
        to=[to_email],
    )
    msg.attach_alternative(html_body, "text/html")
    msg.send(fail_silently=(
        getattr(settings, "EMAIL_FAIL_SILENTLY", False) or 
        getattr(settings, "DEBUG", False)
    ))


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
    """Send user-facing email for Checklist assignment"""
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
        "cta_text": "Click the button below to mark this task as completed.",
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {task.task_name}",
        to_email=task.assign_to.email,
        context=ctx,
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
    """Send user-facing email for Delegation assignment"""
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
        "cta_text": "Click the button below to mark this task as completed.",
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        to_email=delegation.assign_to.email,
        context=ctx,
    )


def send_help_ticket_assignment_to_user(*, ticket, complete_url: str, subject_prefix: str = "New Help Ticket Assigned"):
    """Send user-facing email for Help Ticket assignment"""
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
        "cta_text": "Click the button below to add notes or close this ticket.",
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {ticket.title}",
        to_email=ticket.assign_to.email,
        context=ctx,
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
    """Send clean admin bulk summary without emojis"""
    admin_emails = get_admin_emails()
    if not admin_emails or not rows:
        return
    
    # Clean up title - remove emojis and timing info for cleaner look
    clean_title = title.replace("⚡", "").replace("Fast Bulk Upload:", "Bulk Upload Summary:")
    if "in " in clean_title and "s" in clean_title:
        # Remove timing information from title
        clean_title = clean_title.split(" in ")[0]
    
    # Add complete URLs to each task
    enhanced_rows = []
    for row in rows:
        enhanced_row = dict(row)
        # Try to determine task type and add complete URL
        try:
            # This is a simple approach - you might need to adjust based on your data structure
            enhanced_row['complete_url'] = f"{site_url}/tasks/complete/"  # Generic URL
        except:
            enhanced_row['complete_url'] = None
        enhanced_rows.append(enhanced_row)
    
    send_html_email(
        subject=clean_title,
        template_name="email/admin_assignment_summary.html",
        context={
            "title": clean_title,
            "items_table": enhanced_rows
        },
        to=admin_emails,
    )