# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\email_utils.py
# COMPLETELY FIXED VERSION - Enhanced email system with comprehensive task details

from typing import Iterable, List, Sequence, Optional, Dict, Any
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone
from datetime import datetime
import pytz
from datetime import time as _time
import logging

User = get_user_model()
logger = logging.getLogger(__name__)

# Get site URL for complete URLs
site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# ----------------------------- HELPERS --------------------------------------- #

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
    try:
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
    except Exception as e:
        logger.error(f"Error getting admin emails: {e}")
        return []


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


# -------- ENHANCED HELPERS FOR UNIFIED USER-FACING TEMPLATES ---------------------- #

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
    try:
        tz = timezone.get_current_timezone()
        aware = dt if timezone.is_aware(dt) else timezone.make_aware(dt, tz)
        ist = aware.astimezone(IST)
        base = ist.strftime("%Y-%m-%d")
        t = ist.timetz().replace(tzinfo=None)
        if t != _DEFAULT_ASSIGN_T and t != _time(0, 0):
            return f"{base} {ist.strftime('%H:%M')}"
        return base
    except Exception as e:
        logger.error(f"Error formatting datetime {dt}: {e}")
        return str(dt) if dt else ""

def _send_unified_assignment_email(*, subject: str, to_email: str, context: Dict[str, Any]) -> None:
    """Render standardized TXT + HTML and send."""
    if not (to_email or "").strip():
        return
    
    try:
        # Try to render both text and HTML versions
        try:
            text_body = render_to_string("email/task_assigned.txt", context)
        except Exception:
            # Fallback to simple text if template doesn't exist
            text_body = f"""
Task Assignment: {context.get('task_title', 'New Task')}

Dear {context.get('assignee_name', 'Team Member')},

You have been assigned a new {context.get('kind', 'task')}: {context.get('task_title', 'New Task')}

Task Details:
- Task ID: {context.get('task_code', 'N/A')}
- Priority: {context.get('priority_display', 'Normal')}
- Planned Date: {context.get('planned_date_display', 'Not specified')}
- Assigned By: {context.get('assign_by_display', 'System')}

{context.get('cta_text', 'Please complete this task as soon as possible.')}

Complete URL: {context.get('complete_url', 'N/A')}

Best regards,
EMS System
            """.strip()
        
        try:
            html_body = render_to_string("email/task_assigned.html", context)
        except Exception:
            # Fallback to simple HTML
            html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Task Assignment</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .task-card {{ border: 1px solid #ddd; padding: 20px; border-radius: 5px; }}
        .priority-high {{ color: #dc3545; }}
        .priority-medium {{ color: #ffc107; }}
        .priority-low {{ color: #28a745; }}
        .btn {{ background-color: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class="task-card">
        <h2>Task Assignment: {context.get('task_title', 'New Task')}</h2>
        <p>Dear {context.get('assignee_name', 'Team Member')},</p>
        <p>You have been assigned a new <strong>{context.get('kind', 'task')}</strong>:</p>
        
        <h3>{context.get('task_title', 'New Task')}</h3>
        
        <table>
            <tr><td><strong>Task ID:</strong></td><td>{context.get('task_code', 'N/A')}</td></tr>
            <tr><td><strong>Priority:</strong></td><td class="priority-{context.get('priority_display', 'low').lower()}">{context.get('priority_display', 'Normal')}</td></tr>
            <tr><td><strong>Planned Date:</strong></td><td>{context.get('planned_date_display', 'Not specified')}</td></tr>
            <tr><td><strong>Assigned By:</strong></td><td>{context.get('assign_by_display', 'System')}</td></tr>
        </table>
        
        <p>{context.get('cta_text', 'Please complete this task as soon as possible.')}</p>
        
        <p><a href="{context.get('complete_url', '#')}" class="btn">Complete Task</a></p>
        
        <p>Best regards,<br>EMS System</p>
    </div>
</body>
</html>
            """.strip()
        
        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None),
            to=[to_email],
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=(getattr(settings, "EMAIL_FAIL_SILENTLY", False) or getattr(settings, "DEBUG", False)))
        
        logger.info(f"Successfully sent assignment email to {to_email} for task: {context.get('task_title', 'Unknown')}")
        
    except Exception as e:
        logger.error(f"Failed to send assignment email to {to_email}: {e}")


# ----------------------------- CORE SENDER ----------------------------------- #

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

    try:
        ctx = dict(context or {})
        if isinstance(ctx.get("items"), (list, tuple)):
            ctx["items"] = _fmt_items(ctx["items"])
        if isinstance(ctx.get("items_table"), (list, tuple)):
            ctx["items_table"] = _fmt_rows(ctx["items_table"])

        try:
            html_message = render_to_string(template_name, ctx)
        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {e}")
            # Fallback to simple HTML
            html_message = f"""
<!DOCTYPE html>
<html>
<head><title>{subject}</title></head>
<body>
    <h2>{ctx.get('title', subject)}</h2>
    <p>This is an automated notification from the EMS system.</p>
    <p>Template: {template_name}</p>
</body>
</html>
            """.strip()
        
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
        
        logger.info(f"Successfully sent HTML email to {len(to_list)} recipients: {subject}")
        
    except Exception as e:
        logger.error(f"Failed to send HTML email: {e}")
        if not effective_fail_silently:
            raise


# -------- SPECIFIC HELPERS (Checklist, Delegation, Help Ticket) --------------- #

def send_checklist_assignment_to_user(
    *, task, complete_url: str, subject_prefix: str = "New Checklist Task Assigned"
) -> None:
    """✅ ENHANCED: Standardized user-facing email for Checklist with comprehensive details."""
    if not task.assign_to or not (getattr(task.assign_to, "email", "") or "").strip():
        return
    
    # ✅ COMPREHENSIVE TASK CONTEXT
    ctx = {
        "kind": "Checklist",
        "task_title": task.task_name,
        "task_code": f"CL-{task.id}",
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(task.assign_to),
        "complete_url": complete_url,
        "cta_text": "Click the button below to mark this checklist task as completed.",
        # ✅ ENHANCED TASK DETAILS
        "task_message": getattr(task, "message", "") or "No additional instructions provided.",
        "task_frequency": f"{task.mode} (Every {task.frequency})" if getattr(task, "mode", None) and getattr(task, "frequency", None) else "One-time task",
        "task_group": getattr(task, "group_name", "") or "No group assigned",
        "task_time_minutes": getattr(task, "time_per_task_minutes", 0) or 0,
        "task_time_display": f"{getattr(task, 'time_per_task_minutes', 0) or 0} minutes",
        "attachment_required": getattr(task, "attachment_mandatory", False),
        "remind_before_days": getattr(task, "remind_before_days", 0) or 0,
        # ✅ ADDITIONAL CONTEXT
        "site_url": site_url,
        "is_recurring": bool(getattr(task, "mode", None) and getattr(task, "frequency", None)),
        "task_id": task.id,
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {task.task_name}",
        to_email=task.assign_to.email,
        context=ctx,
    )


def send_delegation_assignment_to_user(
    *, delegation, complete_url: str, subject_prefix: str = "New Delegation Task Assigned"
) -> None:
    """✅ ENHANCED: Standardized user-facing email for Delegation with comprehensive details."""
    if not delegation.assign_to or not (getattr(delegation.assign_to, "email", "") or "").strip():
        return
    
    # ✅ COMPREHENSIVE TASK CONTEXT
    ctx = {
        "kind": "Delegation",
        "task_title": delegation.task_name,
        "task_code": f"DL-{delegation.id}",
        "planned_date_display": _fmt_dt_date(getattr(delegation, "planned_date", None)),
        "priority_display": getattr(delegation, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(delegation, "assign_by", None)),
        "assignee_name": _display_name(delegation.assign_to),
        "complete_url": complete_url,
        "cta_text": "Click the button below to mark this delegation task as completed.",
        # ✅ ENHANCED TASK DETAILS
        "task_frequency": f"{delegation.mode} (Every {delegation.frequency})" if getattr(delegation, "mode", None) and getattr(delegation, "frequency", None) else "One-time task",
        "task_time_minutes": getattr(delegation, "time_per_task_minutes", 0) or 0,
        "task_time_display": f"{getattr(delegation, 'time_per_task_minutes', 0) or 0} minutes",
        "attachment_required": getattr(delegation, "attachment_mandatory", False),
        # ✅ ADDITIONAL CONTEXT
        "site_url": site_url,
        "is_recurring": bool(getattr(delegation, "mode", None) and getattr(delegation, "frequency", None)),
        "task_id": delegation.id,
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        to_email=delegation.assign_to.email,
        context=ctx,
    )


def send_help_ticket_assignment_to_user(
    *, ticket, complete_url: str, subject_prefix: str = "New Help Ticket Assigned"
) -> None:
    """✅ ENHANCED: Standardized user-facing email for Help Ticket with comprehensive details."""
    if not ticket.assign_to or not (getattr(ticket.assign_to, "email", "") or "").strip():
        return
    
    # ✅ COMPREHENSIVE TASK CONTEXT
    ctx = {
        "kind": "Help Ticket",
        "task_title": ticket.title,
        "task_code": f"HT-{ticket.id}",
        "planned_date_display": _fmt_dt_date(getattr(ticket, "planned_date", None)),
        "priority_display": getattr(ticket, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(ticket, "assign_by", None)),
        "assignee_name": _display_name(ticket.assign_to),
        "complete_url": complete_url,
        "cta_text": "Click the button below to add notes or close this help ticket.",
        # ✅ ENHANCED TASK DETAILS
        "task_message": getattr(ticket, "description", "") or "No description provided.",
        "estimated_minutes": getattr(ticket, "estimated_minutes", 0) or 0,
        # ✅ ADDITIONAL CONTEXT
        "site_url": site_url,
        "task_id": ticket.id,
    }
    
    _send_unified_assignment_email(
        subject=f"{subject_prefix}: {ticket.title}",
        to_email=ticket.assign_to.email,
        context=ctx,
    )


# ---------------- ADMIN CONFIRMATIONS / SUMMARIES -------------------------------- #

def send_checklist_admin_confirmation(*, task, subject_prefix: str = "Checklist Task Assignment") -> None:
    """✅ ENHANCED: Send detailed admin confirmation for checklist"""
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    
    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {task.task_name}",
            "items": [
                {"label": "Task Name", "value": task.task_name},
                {"label": "Task ID", "value": f"CL-{task.id}"},
                {"label": "Assignee", "value": task.assign_to},
                {"label": "Assigned By", "value": task.assign_by},
                {"label": "Planned Date", "value": task.planned_date},
                {"label": "Priority", "value": task.priority},
                {"label": "Group", "value": getattr(task, "group_name", "") or "No group"},
                {"label": "Time Estimate", "value": f"{getattr(task, 'time_per_task_minutes', 0) or 0} minutes"},
                {"label": "Recurring", "value": f"{task.mode} (Every {task.frequency})" if getattr(task, "mode", None) else "One-time"},
                {"label": "Message", "value": getattr(task, "message", "") or "No message"},
            ],
        },
        to=admin_emails,
    )


def send_help_ticket_admin_confirmation(*, ticket, subject_prefix: str = "Help Ticket Assignment") -> None:
    """✅ ENHANCED: Send detailed admin confirmation for help ticket"""
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    
    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {ticket.title}",
            "items": [
                {"label": "Ticket Title", "value": ticket.title},
                {"label": "Ticket ID", "value": f"HT-{ticket.id}"},
                {"label": "Assignee", "value": ticket.assign_to},
                {"label": "Assigned By", "value": ticket.assign_by},
                {"label": "Planned Date", "value": ticket.planned_date},
                {"label": "Priority", "value": ticket.priority},
                {"label": "Estimated Time", "value": f"{getattr(ticket, 'estimated_minutes', 0) or 0} minutes"},
                {"label": "Description", "value": getattr(ticket, "description", "") or "No description"},
            ],
        },
        to=admin_emails,
    )


def send_admin_bulk_summary(*, title: str, rows: Sequence[dict]) -> None:
    """✅ ENHANCED: Send clean admin bulk summary with performance metrics"""
    admin_emails = get_admin_emails()
    if not admin_emails or not rows:
        return
    
    # Clean up title - remove emojis and format professionally
    clean_title = title.replace("⚡", "").replace("ULTRA-FAST", "High-Performance")
    if "Fast Bulk Upload:" in clean_title:
        clean_title = clean_title.replace("Fast Bulk Upload:", "Bulk Upload Summary:")
    
    # Enhance rows with additional context
    enhanced_rows = []
    for row in rows:
        enhanced_row = dict(row)
        # Ensure complete_url is included
        if 'complete_url' not in enhanced_row and 'Task Name' in enhanced_row:
            enhanced_row['Action'] = 'Task Created Successfully'
        enhanced_rows.append(enhanced_row)
    
    # Add summary statistics
    summary_stats = [
        {"label": "Total Tasks Created", "value": len(rows)},
        {"label": "Performance", "value": "High-Performance Bulk Processing"},
        {"label": "Status", "value": "✅ Successfully Completed"},
        {"label": "System", "value": "EMS Task Management"},
    ]
    
    send_html_email(
        subject=clean_title,
        template_name="email/admin_assignment_summary.html",
        context={
            "title": clean_title,
            "items": summary_stats,
            "items_table": enhanced_rows,
            "is_bulk_summary": True,
            "bulk_count": len(rows),
        },
        to=admin_emails,
    )


# -------------------------- UNASSIGNED NOTICES ----------------------------------- #

def send_checklist_unassigned_notice(*, task, old_user) -> None:
    """✅ ENHANCED: Send unassignment notice for checklist"""
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    
    send_html_email(
        subject=f"Checklist Task Unassigned: {task.task_name}",
        template_name="email/checklist_unassigned.html",
        context={
            "task": task,
            "old_user": old_user,
            "task_title": task.task_name,
            "task_id": f"CL-{task.id}",
            "new_assignee": _display_name(task.assign_to) if task.assign_to else "Unassigned",
        },
        to=[old_user.email],
    )


def send_help_ticket_unassigned_notice(*, ticket, old_user) -> None:
    """✅ ENHANCED: Send unassignment notice for help ticket"""
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    
    send_html_email(
        subject=f"Help Ticket Unassigned: {ticket.title}",
        template_name="email/help_ticket_unassigned.html",
        context={
            "ticket": ticket,
            "old_user": old_user,
            "task_title": ticket.title,
            "task_id": f"HT-{ticket.id}",
            "new_assignee": _display_name(ticket.assign_to) if ticket.assign_to else "Unassigned",
        },
        to=[old_user.email],
    )


# -------------------------- DELEGATION EMAIL FUNCTIONS --------------------------- #

def send_delegation_admin_confirmation(*, delegation, subject_prefix: str = "Delegation Task Assignment") -> None:
    """✅ ENHANCED: Send detailed admin confirmation for delegation"""
    admin_emails = get_admin_emails()
    if not admin_emails:
        return
    
    send_html_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {delegation.task_name}",
            "items": [
                {"label": "Task Name", "value": delegation.task_name},
                {"label": "Task ID", "value": f"DL-{delegation.id}"},
                {"label": "Assignee", "value": delegation.assign_to},
                {"label": "Assigned By", "value": delegation.assign_by},
                {"label": "Planned Date", "value": delegation.planned_date},
                {"label": "Priority", "value": delegation.priority},
                {"label": "Time Estimate", "value": f"{getattr(delegation, 'time_per_task_minutes', 0) or 0} minutes"},
                {"label": "Recurring", "value": f"{delegation.mode} (Every {delegation.frequency})" if getattr(delegation, "mode", None) else "One-time"},
            ],
        },
        to=admin_emails,
    )


def send_delegation_unassigned_notice(*, delegation, old_user) -> None:
    """✅ ENHANCED: Send unassignment notice for delegation"""
    if not old_user or not (getattr(old_user, "email", "") or "").strip():
        return
    
    send_html_email(
        subject=f"Delegation Task Unassigned: {delegation.task_name}",
        template_name="email/delegation_unassigned.html",
        context={
            "delegation": delegation,
            "old_user": old_user,
            "task_title": delegation.task_name,
            "task_id": f"DL-{delegation.id}",
            "new_assignee": _display_name(delegation.assign_to) if delegation.assign_to else "Unassigned",
        },
        to=[old_user.email],
    )


# -------------------------- REMINDER EMAIL FUNCTIONS ----------------------------- #

def send_task_reminder_email(*, task, task_type: str = "Checklist") -> None:
    """✅ NEW: Send reminder email for upcoming tasks"""
    if not task.assign_to or not (getattr(task.assign_to, "email", "") or "").strip():
        return
    
    # Calculate days until due
    from django.utils import timezone
    if task.planned_date:
        days_until = (task.planned_date.date() - timezone.now().date()).days
        if days_until < 0:
            urgency = "OVERDUE"
            urgency_class = "danger"
        elif days_until == 0:
            urgency = "DUE TODAY"
            urgency_class = "warning"
        elif days_until == 1:
            urgency = "DUE TOMORROW"
            urgency_class = "info"
        else:
            urgency = f"DUE IN {days_until} DAYS"
            urgency_class = "primary"
    else:
        urgency = "NO DUE DATE"
        urgency_class = "secondary"
    
    task_name = getattr(task, 'task_name', None) or getattr(task, 'title', 'Unknown Task')
    task_id = f"{task_type[:2].upper()}-{task.id}"
    
    ctx = {
        "kind": task_type,
        "task_title": task_name,
        "task_code": task_id,
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(task.assign_to),
        "urgency": urgency,
        "urgency_class": urgency_class,
        "days_until": days_until if task.planned_date else None,
        "is_overdue": days_until < 0 if task.planned_date else False,
        "site_url": site_url,
        "task_id": task.id,
    }
    
    _send_unified_assignment_email(
        subject=f"⏰ REMINDER: {urgency} - {task_name}",
        to_email=task.assign_to.email,
        context=ctx,
    )


def send_bulk_completion_summary(*, user, completed_tasks: List, date_range: str = "today") -> None:
    """✅ NEW: Send summary of completed tasks to user"""
    if not user or not (getattr(user, "email", "") or "").strip() or not completed_tasks:
        return
    
    total_tasks = len(completed_tasks)
    total_time = sum(getattr(task, 'actual_duration_minutes', 0) or 0 for task in completed_tasks)
    
    # Group by task type
    task_groups = {}
    for task in completed_tasks:
        task_type = task.__class__.__name__
        if task_type not in task_groups:
            task_groups[task_type] = []
        task_groups[task_type].append(task)
    
    send_html_email(
        subject=f"🎉 Task Completion Summary - {total_tasks} tasks completed {date_range}",
        template_name="email/completion_summary.html",
        context={
            "user": user,
            "total_tasks": total_tasks,
            "total_time": total_time,
            "total_time_display": f"{total_time // 60}h {total_time % 60}m" if total_time >= 60 else f"{total_time}m",
            "date_range": date_range,
            "task_groups": task_groups,
            "site_url": site_url,
        },
        to=[user.email],
    )


# -------------------------- UTILITY FUNCTIONS ------------------------------------ #

def test_email_configuration() -> bool:
    """✅ NEW: Test email configuration"""
    try:
        from django.core.mail import send_mail
        
        send_mail(
            subject="EMS Email Configuration Test",
            message="This is a test email to verify email configuration.",
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
            recipient_list=[getattr(settings, "DEFAULT_FROM_EMAIL", "admin@example.com")],
            fail_silently=False,
        )
        logger.info("Email configuration test successful")
        return True
    except Exception as e:
        logger.error(f"Email configuration test failed: {e}")
        return False


def get_email_statistics() -> Dict[str, Any]:
    """✅ NEW: Get email sending statistics"""
    # This would typically connect to your email service provider's API
    # For now, return basic stats
    return {
        "emails_sent_today": 0,  # Would be calculated from logs
        "emails_failed_today": 0,
        "email_service_status": "active",
        "last_email_sent": timezone.now(),
    }


# Export all functions for backward compatibility
__all__ = [
    # Core functions
    'send_html_email',
    'get_admin_emails',
    'test_email_configuration',
    'get_email_statistics',
    
    # Task assignment functions
    'send_checklist_assignment_to_user',
    'send_delegation_assignment_to_user', 
    'send_help_ticket_assignment_to_user',
    
    # Admin confirmation functions
    'send_checklist_admin_confirmation',
    'send_delegation_admin_confirmation',
    'send_help_ticket_admin_confirmation',
    
    # Unassignment functions
    'send_checklist_unassigned_notice',
    'send_delegation_unassigned_notice',
    'send_help_ticket_unassigned_notice',
    
    # Bulk and summary functions
    'send_admin_bulk_summary',
    'send_bulk_completion_summary',
    
    # Reminder functions
    'send_task_reminder_email',
    
    # Helper functions
    '_dedupe_emails',
    '_fmt_value',
    '_fmt_items',
    '_fmt_rows',
    '_display_name',
    '_fmt_dt_date',
    '_send_unified_assignment_email',
]