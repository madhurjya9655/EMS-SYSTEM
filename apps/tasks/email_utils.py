# apps/tasks/email_utils.py
from __future__ import annotations

from typing import Iterable, List, Sequence, Optional, Dict, Any
from datetime import datetime, date as _date, time as _time
import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives, send_mail
from django.template.loader import render_to_string
from django.utils import timezone

# Prefer Python stdlib tz (Django 5+ defaults to zoneinfo)
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

User = get_user_model()
logger = logging.getLogger(__name__)


def _normalize_site_url(u: str) -> str:
    """Strip trailing slash and whitespace from SITE_URL-like values."""
    from urllib.parse import urlsplit, urlunsplit

    if not u:
        return ""
    parts = list(urlsplit(u.strip()))
    parts[2] = parts[2].rstrip("/")
    return urlunsplit(parts)


SITE_URL = _normalize_site_url(
    getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
)
IST = (
    ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
    if ZoneInfo
    else timezone.get_fixed_timezone(330)
)  # 330 mins = IST
DEFAULT_ASSIGN_T = _time(10, 0)


# -------------------------------------------------------------------
# Generic helpers
# -------------------------------------------------------------------
def _safe_console_text(s: object) -> str:
    """
    Console-safe text (avoids encoding errors in logs).
    Reused by various services (weekly_performance, signals, etc.).
    """
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    try:
        return text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    except Exception:
        return text


def _dedupe_emails(emails: Iterable[str]) -> List[str]:
    """Remove duplicates and empty values; preserve order."""
    seen = set()
    out: List[str] = []
    for e in emails or []:
        s = (e or "").strip()
        if s and "@" in s:
            low = s.lower()
            if low not in seen:
                seen.add(low)
                out.append(s)
    return out


def _without_emails(emails: Sequence[str], exclude: Optional[Sequence[str]]) -> List[str]:
    """Case-insensitive subtract of exclude from emails."""
    if not emails:
        return []
    excl = {e.strip().lower() for e in (exclude or []) if e}
    return [e for e in emails if e and e.strip().lower() not in excl]


def get_admin_emails(exclude: Optional[Sequence[str]] = None) -> List[str]:
    """
    Superusers + members of Admin/Manager/EA/CEO groups.
    Returns a deduped list of emails, excluding any in `exclude`.
    """
    try:
        qs = (
            User.objects.filter(is_active=True)
            .exclude(email__isnull=True)
            .exclude(email__exact="")
        )
        admins = list(qs.filter(is_superuser=True).values_list("email", flat=True))
        groups = (
            qs.filter(groups__name__in=["Admin", "Manager", "EA", "CEO"])
            .values_list("email", flat=True)
            .distinct()
        )
        groups = list(groups)
        all_emails = _dedupe_emails(admins + groups)
        if exclude:
            all_emails = _without_emails(all_emails, exclude)
        return all_emails
    except Exception as e:
        logger.error("get_admin_emails failed: %s", e)
        return []


def _display_name(user) -> str:
    """Full name if available; else username; else 'System'."""
    if not user:
        return "System"
    try:
        full = getattr(user, "get_full_name", lambda: "")() or ""
        if full.strip():
            return full.strip()
        uname = getattr(user, "username", "") or ""
        return uname if uname else "System"
    except Exception:
        return "System"


def _fmt_value(v: Any) -> Any:
    """Format values for admin summary templates."""
    if isinstance(v, datetime):
        tz = timezone.get_current_timezone()
        aware = v if timezone.is_aware(v) else timezone.make_aware(v, tz)
        return timezone.localtime(aware, tz).strftime("%Y-%m-%d %H:%M")
    if hasattr(v, "get_full_name") or hasattr(v, "username"):
        try:
            name = getattr(v, "get_full_name", lambda: "")() or getattr(
                v, "username", ""
            )
            return name
        except Exception:
            return str(v)
    return v


def _fmt_items(items: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    return [
        {"label": str(r.get("label", "")), "value": _fmt_value(r.get("value"))}
        for r in (items or [])
    ]


def _fmt_rows(rows: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        new_row: Dict[str, Any] = {}
        for k, v in r.items():
            new_row[str(k)] = _fmt_value(v)
        out.append(new_row)
    return out


def _fmt_dt_date(dt: Any) -> str:
    """
    IST string as 'YYYY-MM-DD' and add ' HH:MM' if time is meaningful
    (not 00:00 and not the default 10:00).

    Robust to receiving either a datetime or a date.
    """
    if not dt:
        return ""
    try:
        tz = IST or timezone.get_current_timezone()
        # If we were given a plain date (defensive), treat it as 00:00 local.
        if isinstance(dt, _date) and not isinstance(dt, datetime):
            dt = datetime(dt.year, dt.month, dt.day, 0, 0)
        # Ensure awareness in project TZ, then display in IST (or configured TZ).
        aware = dt if timezone.is_aware(dt) else timezone.make_aware(dt, tz)
        ist = timezone.localtime(aware, tz)
        base = ist.strftime("%Y-%m-%d")
        t = ist.timetz().replace(tzinfo=None)
        if t not in (DEFAULT_ASSIGN_T, _time(0, 0)):
            return f"{base} {ist.strftime('%H:%M')}"
        return base
    except Exception as e:
        logger.error("Failed to format datetime %r: %s", dt, e)
        return str(dt)


def _from_email() -> str:
    return (
        getattr(settings, "DEFAULT_FROM_EMAIL", None)
        or getattr(settings, "EMAIL_HOST_USER", None)
        or "EMS <no-reply@example.com>"
    )


def _fail_silently() -> bool:
    return bool(
        getattr(settings, "EMAIL_FAIL_SILENTLY", False)
        or getattr(settings, "DEBUG", False)
    )


# -------------------------------------------------------------------
# Core sending helpers
# -------------------------------------------------------------------
def _render_or_fallback(template_name: str, context: Dict[str, Any], fallback: str) -> str:
    try:
        return render_to_string(template_name, context)
    except Exception as e:
        logger.warning(
            "Template %s not found or failed to render (%s). Using fallback.",
            template_name,
            e,
        )
        return fallback


def _send_unified_assignment_email(
    *, subject: str, to_email: str, context: Dict[str, Any], cc: Optional[Sequence[str]] = None, bcc: Optional[Sequence[str]] = None
) -> None:
    """Render standardized TXT + HTML and send safely (assignee-only)."""
    to_email = (to_email or "").strip()
    if not to_email:
        return

    cc_list = _dedupe_emails(cc or [])
    bcc_list = _dedupe_emails(bcc or [])

    # Text fallback (simple/plain)
    text_fallback = (
        f"Task Assignment: {context.get('task_title', 'New Task')}\n\n"
        f"Dear {context.get('assignee_name', 'Team Member')},\n\n"
        f"You have been assigned a new {context.get('kind', 'task')}.\n"
        f"Task ID: {context.get('task_code', 'N/A')}\n"
        f"Priority: {context.get('priority_display', 'Normal')}\n"
        f"Planned Date: {context.get('planned_date_display', 'Not specified')}\n"
        f"Assigned By: {context.get('assign_by_display', 'System')}\n\n"
        f"{context.get('cta_text', 'Please complete this task as soon as possible.')}\n"
        f"Open URL: {context.get('complete_url', 'N/A')}\n"
        f"\nRegards,\nEMS System"
    )

    # HTML fallback (simple)
    html_fallback = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{subject}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    .card {{ border: 1px solid #ddd; padding: 16px; border-radius: 6px; }}
    .btn {{ display: inline-block; background: #0d6efd; color: #fff; padding: 10px 16px; text-decoration: none; border-radius: 4px; }}
    .muted {{ color: #666; }}
    table td {{ padding: 4px 8px; vertical-align: top; }}
  </style>
</head>
<body>
  <div class="card">
    <h2>{context.get('task_title', 'New Task')}</h2>
    <p>Dear {context.get('assignee_name', 'Team Member')},</p>
    <p>You have been assigned a new <strong>{context.get('kind', 'task')}</strong>.</p>
    <table>
      <tr><td><strong>Task ID:</strong></td><td>{context.get('task_code', 'N/A')}</td></tr>
      <tr><td><strong>Priority:</strong></td><td>{context.get('priority_display', 'Normal')}</td></tr>
      <tr><td><strong>Planned Date:</strong></td><td>{context.get('planned_date_display', 'Not specified')}</td></tr>
      <tr><td><strong>Assigned By:</strong></td><td>{context.get('assign_by_display', 'System')}</td></tr>
    </table>
    <p>{context.get('cta_text', 'Please complete this task as soon as possible.')}</p>
    <p><a href="{context.get('complete_url', '#')}" class="btn">Open Task</a></p>
    <p class="muted">EMS System</p>
  </div>
</body>
</html>
""".strip()

    try:
        text_body = _render_or_fallback("email/task_assigned.txt", context, text_fallback)
        html_body = _render_or_fallback("email/task_assigned.html", context, html_fallback)

        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=_from_email(),
            to=[to_email],
            cc=cc_list or None,
            bcc=bcc_list or None,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=_fail_silently())
        logger.info(
            "Sent assignment email to %s (%s)%s",
            to_email,
            subject,
            f" [cc={', '.join(cc_list)}]" if cc_list else "",
        )
    except Exception as e:
        logger.error("Failed sending assignment email to %s: %s", to_email, e)


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
    """Render and send an HTML email using a Django template, with safe fallbacks."""
    to_list = _dedupe_emails(to or [])
    if not to_list:
        return

    cc_list = _dedupe_emails(cc or [])
    bcc_list = _dedupe_emails(bcc or [])

    effective_fail_silently = fail_silently or _fail_silently()

    try:
        ctx = dict(context or {})
        if isinstance(ctx.get("items"), (list, tuple)):
            ctx["items"] = _fmt_items(ctx["items"])
        if isinstance(ctx.get("items_table"), (list, tuple)):
            ctx["items_table"] = _fmt_rows(ctx["items_table"])

        # Render; fallback to a minimal shell if missing
        html_message = _render_or_fallback(
            template_name,
            ctx,
            f"<html><body><h3>{ctx.get('title', subject)}</h3><p>Automated notification.</p></body></html>",
        )

        msg = EmailMultiAlternatives(
            subject=subject,
            body=html_message,
            from_email=_from_email(),
            to=to_list,
            cc=cc_list or None,
            bcc=bcc_list or None,
        )
        msg.attach_alternative(html_message, "text/html")
        msg.send(fail_silently=effective_fail_silently)

        logger.info("Sent HTML email to %d recipient(s): %s", len(to_list), subject)
    except Exception as e:
        logger.error("send_html_email failed: %s", e)
        if not effective_fail_silently:
            raise


# BACKWARDS-COMPAT SHIM for older code (e.g. weekly_performance)
def _send_email(
    subject: str,
    template_name: str,
    context: Dict[str, Any],
    to: Sequence[str],
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    fail_silently: bool = False,
) -> None:
    """
    Legacy wrapper so existing code that calls `_send_email(...)` keeps working.

    Typical usage pattern we support:
        _send_email("Subject", "template.html", ctx, ["to@example.com"])
    """
    send_html_email(
        subject=subject,
        template_name=template_name,
        context=context,
        to=to,
        cc=cc,
        bcc=bcc,
        fail_silently=fail_silently,
    )


# -------------------------------------------------------------------
# Subject builder (prevents duplication with scheduler)
# -------------------------------------------------------------------
def _build_subject(subject_prefix: str, task_title: str) -> str:
    """
    If 'subject_prefix' already looks like a full subject (e.g. the scheduler's
    '✅ Task Reminder: <name> scheduled for <date>, <time>'), use it as-is.
    Otherwise, treat it as a prefix and append ': <task_title>'.
    """
    sp = (subject_prefix or "").strip()
    if not sp:
        return task_title

    low = sp.lower()
    # Heuristics that strongly indicate a fully-formed subject
    markers = ("reminder", "scheduled for", "due", "overdue")
    if any(m in low for m in markers):
        return sp
    # If it already includes the task title, assume it's full
    if task_title and task_title.strip().lower() in low:
        return sp
    return f"{sp}: {task_title}"


# -------------------------------------------------------------------
# CC rule for Delegation (Pankaj)
# -------------------------------------------------------------------
def _should_cc_assigner_for_delegation(assign_by) -> List[str]:
    """
    Returns a list containing the assigner's email if settings say we should CC them
    for delegation emails and if the assigner matches by email/username.
    """
    try:
        cfg = getattr(settings, "ASSIGNER_CC_FOR_DELEGATION", {}) or {}
        emails = {e.strip().lower() for e in (cfg.get("emails") or []) if e}
        usernames = {u.strip().lower() for u in (cfg.get("usernames") or []) if u}
        a_email = (getattr(assign_by, "email", "") or "").strip().lower()
        a_username = (getattr(assign_by, "username", "") or "").strip().lower()
        if (a_email and a_email in emails) or (a_username and a_username in usernames):
            return _dedupe_emails([getattr(assign_by, "email", "") or ""])
    except Exception as e:
        logger.error("CC rule evaluation failed: %s", e)
    return []


# -------------------------------------------------------------------
# Task-specific senders (Assignment / Admin confirmations)
# -------------------------------------------------------------------
def send_checklist_assignment_to_user(
    *, task, complete_url: str, subject_prefix: str = "Checklist Assigned"
) -> None:
    """
    User-facing email for Checklist (assignee-only).
    NOTE: Assignee must receive the email even if assigner == assignee.
    """
    to_email = getattr(getattr(task, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(task, "task_name", "Checklist")
    subject = _build_subject(subject_prefix, task_title)

    ctx = {
        "kind": "Checklist",
        "task_title": task_title,
        "task_code": f"CL-{task.id}",
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(getattr(task, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the task and mark it complete when done.",
        # extra details
        "task_message": getattr(task, "message", "") or "",
        "instructions": getattr(task, "message", "") or "",
        "task_frequency": (
            f"{getattr(task, 'mode', '')} (Every {getattr(task, 'frequency', '')})"
            if getattr(task, "mode", None) and getattr(task, "frequency", None)
            else "One-time task"
        ),
        "task_group": getattr(task, "group_name", "") or "No group",
        "task_time_minutes": getattr(task, "time_per_task_minutes", 0) or 0,
        "attachment_required": getattr(task, "attachment_mandatory", False),
        "remind_before_days": getattr(task, "remind_before_days", 0) or 0,
        "site_url": SITE_URL,
        "is_recurring": bool(
            getattr(task, "mode", None) and getattr(task, "frequency", None)
        ),
        "task_id": task.id,
    }

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
    )


def send_delegation_assignment_to_user(
    *, delegation, complete_url: str, subject_prefix: str = "Delegation Assigned"
) -> None:
    """
    User-facing email for Delegation (assignee-only).
    NOTE: Assignee must receive the email even if assigner == assignee.
    """
    to_email = getattr(getattr(delegation, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(delegation, "task_name", "Delegation")
    subject = _build_subject(subject_prefix, task_title)

    ctx = {
        "kind": "Delegation",
        "task_title": task_title,
        "task_code": f"DL-{delegation.id}",
        "planned_date_display": _fmt_dt_date(getattr(delegation, "planned_date", None)),
        "priority_display": getattr(delegation, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(delegation, "assign_by", None)),
        "assignee_name": _display_name(getattr(delegation, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the task and mark it complete when done.",
        "instructions": getattr(delegation, "message", "") or "",
        "task_frequency": (
            f"{getattr(delegation, 'mode', '')} (Every {getattr(delegation, 'frequency', '')})"
            if getattr(delegation, "mode", None) and getattr(delegation, "frequency", None)
            else "One-time task"
        ),
        "task_time_minutes": getattr(delegation, "time_per_task_minutes", 0) or 0,
        "attachment_required": getattr(delegation, "attachment_mandatory", False),
        "site_url": SITE_URL,
        "is_recurring": bool(
            getattr(delegation, "mode", None) and getattr(delegation, "frequency", None)
        ),
        "task_id": delegation.id,
    }

    cc_list = _should_cc_assigner_for_delegation(getattr(delegation, "assign_by", None))

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
        cc=cc_list,
    )


def send_help_ticket_assignment_to_user(
    *, ticket, complete_url: str, subject_prefix: str = "Help Ticket Assigned"
) -> None:
    """
    User-facing email for Help Ticket (assignee-only).
    NOTE: Assignee must receive the email even if assigner == assignee.
    """
    to_email = getattr(getattr(ticket, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(ticket, "title", "Help Ticket")
    subject = _build_subject(subject_prefix, task_title)

    ctx = {
        "kind": "Help Ticket",
        "task_title": task_title,
        "task_code": f"HT-{ticket.id}",
        "planned_date_display": _fmt_dt_date(getattr(ticket, "planned_date", None)),
        "priority_display": getattr(ticket, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(ticket, "assign_by", None)),
        "assignee_name": _display_name(getattr(ticket, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the ticket to add notes or close it when resolved.",
        "task_message": getattr(ticket, "description", "") or "",
        "instructions": getattr(ticket, "description", "") or "",
        "estimated_minutes": getattr(ticket, "estimated_minutes", 0) or 0,
        "site_url": SITE_URL,
        "task_id": ticket.id,
    }

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
    )


def send_checklist_admin_confirmation(*, task, subject_prefix: str = "Checklist Assignment") -> None:
    """
    Detailed admin confirmation for checklist.

    IMPORTANT: Excludes the assigner from recipients to satisfy
    “Assigner should never receive emails”.
    """
    exclude: List[str] = []
    try:
        if getattr(task, "assign_by", None) and getattr(task.assign_by, "email", None):
            exclude = [task.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {task.task_name}",
            "items": _fmt_items(
                [
                    {"label": "Task Name", "value": task.task_name},
                    {"label": "Task ID", "value": f"CL-{task.id}"},
                    {"label": "Assignee", "value": task.assign_to},
                    {"label": "Assigned By", "value": task.assign_by},
                    {"label": "Planned Date", "value": task.planned_date},
                    {"label": "Priority", "value": task.priority},
                    {
                        "label": "Group",
                        "value": getattr(task, "group_name", "") or "No group",
                    },
                    {
                        "label": "Time Estimate",
                        "value": f"{getattr(task, 'time_per_task_minutes', 0) or 0} minutes",
                    },
                    {
                        "label": "Recurring",
                        "value": f"{task.mode} (Every {task.frequency})"
                        if getattr(task, "mode", None)
                        else "One-time",
                    },
                    {
                        "label": "Message",
                        "value": getattr(task, "message", "") or "No message",
                    },
                ]
            ),
        },
        to=admins,
    )


def send_delegation_admin_confirmation(
    *, delegation, subject_prefix: str = "Delegation Assignment"
) -> None:
    """Detailed admin confirmation for delegation (assigner excluded)."""
    exclude: List[str] = []
    try:
        if getattr(delegation, "assign_by", None) and getattr(
            delegation.assign_by, "email", None
        ):
            exclude = [delegation.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {delegation.task_name}",
            "items": _fmt_items(
                [
                    {"label": "Task Name", "value": delegation.task_name},
                    {"label": "Task ID", "value": f"DL-{delegation.id}"},
                    {"label": "Assignee", "value": delegation.assign_to},
                    {"label": "Assigned By", "value": delegation.assign_by},
                    {"label": "Planned Date", "value": delegation.planned_date},
                    {"label": "Priority", "value": delegation.priority},
                    {
                        "label": "Time Estimate",
                        "value": f"{getattr(delegation, 'time_per_task_minutes', 0) or 0} minutes",
                    },
                    {
                        "label": "Recurring",
                        "value": f"{delegation.mode} (Every {delegation.frequency})"
                        if getattr(delegation, "mode", None)
                        else "One-time",
                    },
                ]
            ),
        },
        to=admins,
    )


def send_help_ticket_admin_confirmation(
    *, ticket, subject_prefix: str = "Help Ticket Assignment"
) -> None:
    """Detailed admin confirmation for help ticket (assigner excluded)."""
    exclude: List[str] = []
    try:
        if getattr(ticket, "assign_by", None) and getattr(ticket.assign_by, "email", None):
            exclude = [ticket.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {ticket.title}",
            "items": _fmt_items(
                [
                    {"label": "Ticket Title", "value": ticket.title},
                    {"label": "Ticket ID", "value": f"HT-{ticket.id}"},
                    {"label": "Assignee", "value": ticket.assign_to},
                    {"label": "Assigned By", "value": ticket.assign_by},
                    {"label": "Planned Date", "value": ticket.planned_date},
                    {"label": "Priority", "value": ticket.priority},
                    {
                        "label": "Estimated Time",
                        "value": f"{getattr(ticket, 'estimated_minutes', 0) or 0} minutes",
                    },
                    {
                        "label": "Description",
                        "value": getattr(ticket, "description", "") or "No description",
                    },
                ]
            ),
        },
        to=admins,
    )


# -------------------------------------------------------------------
# Unassignment notices (assignee only)
# -------------------------------------------------------------------
def send_checklist_unassigned_notice(*, task, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Checklist Unassigned: {task.task_name}",
        template_name="email/checklist_unassigned.html",
        context={
            "task": task,
            "old_user": old_user,
            "task_title": task.task_name,
            "task_id": f"CL-{task.id}",
            "new_assignee": _display_name(getattr(task, "assign_to", None))
            if getattr(task, "assign_to", None)
            else "Unassigned",
        },
        to=[email],
    )


def send_delegation_unassigned_notice(*, delegation, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Delegation Unassigned: {delegation.task_name}",
        template_name="email/delegation_unassigned.html",
        context={
            "delegation": delegation,
            "old_user": old_user,
            "task_title": delegation.task_name,
            "task_id": f"DL-{delegation.id}",
            "new_assignee": _display_name(getattr(delegation, "assign_to", None))
            if getattr(delegation, "assign_to", None)
            else "Unassigned",
        },
        to=[email],
    )


def send_help_ticket_unassigned_notice(*, ticket, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Help Ticket Unassigned: {ticket.title}",
        template_name="email/help_ticket_unassigned.html",
        context={
            "ticket": ticket,
            "old_user": old_user,
            "task_title": ticket.title,
            "task_id": f"HT-{ticket.id}",
            "new_assignee": _display_name(getattr(ticket, "assign_to", None))
            if getattr(ticket, "assign_to", None)
            else "Unassigned",
        },
        to=[email],
    )


# -------------------------------------------------------------------
# Reminders & Summaries
# -------------------------------------------------------------------
def send_task_reminder_email(*, task, task_type: str = "Checklist") -> None:
    """Reminder email for upcoming/overdue tasks (assignee only)."""
    to_email = getattr(getattr(task, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    if getattr(task, "planned_date", None):
        pd = getattr(task, "planned_date")
        if isinstance(pd, datetime):
            pd_date = timezone.localtime(pd, IST or timezone.get_current_timezone()).date()
        else:
            pd_date = pd
        days_until = (pd_date - timezone.localdate()).days
        if days_until < 0:
            urgency = "OVERDUE"
        elif days_until == 0:
            urgency = "DUE TODAY"
        elif days_until == 1:
            urgency = "DUE TOMORROW"
        else:
            urgency = f"DUE IN {days_until} DAYS"
    else:
        days_until = None
        urgency = "NO DUE DATE"

    task_name = getattr(task, "task_name", None) or getattr(task, "title", "Task")
    task_code = f"{task_type[:2].upper()}-{task.id}"

    ctx = {
        "kind": task_type,
        "task_title": task_name,
        "task_code": task_code,
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(getattr(task, "assign_to", None)),
        "urgency": urgency,
        "days_until": days_until,
        "site_url": SITE_URL,
        "task_id": task.id,
        "cta_text": "Please review and complete this item.",
        "complete_url": SITE_URL,
    }

    cc_list: List[str] = []
    if (task_type or "").strip().lower() == "delegation":
        cc_list = _should_cc_assigner_for_delegation(getattr(task, "assign_by", None))

    _send_unified_assignment_email(
        subject=f"Reminder: {urgency} - {task_name}",
        to_email=to_email,
        context=ctx,
        cc=cc_list,
    )


def send_admin_bulk_summary(
    *, title: str, rows: Sequence[dict], exclude_assigner_email: Optional[str] = None
) -> None:
    """
    Send clean admin bulk summary with basic stats.
    IMPORTANT: will exclude the assigner email if provided.
    """
    exclude = [exclude_assigner_email] if exclude_assigner_email else None
    admins = get_admin_emails(exclude=exclude)
    if not admins or not rows:
        return

    summary_stats = [
        {"label": "Total Items", "value": len(rows)},
        {"label": "Status", "value": "Completed"},
        {"label": "System", "value": "EMS Task Management"},
    ]

    send_html_email(
        subject=title,
        template_name="email/admin_assignment_summary.html",
        context={
            "title": title,
            "items": _fmt_items(summary_stats),
            "items_table": _fmt_rows(rows),
            "is_bulk_summary": True,
            "bulk_count": len(rows),
        },
        to=admins,
    )


def send_bulk_completion_summary(
    *, user, completed_tasks: List, date_range: str = "today"
) -> None:
    """Send summary of completed tasks to a user (assignee)."""
    email = getattr(user, "email", "") or ""
    if not email.strip() or not completed_tasks:
        return

    total_tasks = len(completed_tasks)
    total_time = sum(
        getattr(t, "actual_duration_minutes", 0) or 0 for t in completed_tasks
    )

    task_groups: Dict[str, List[Any]] = {}
    for t in completed_tasks:
        task_groups.setdefault(t.__class__.__name__, []).append(t)

    send_html_email(
        subject=f"Task Completion Summary - {total_tasks} tasks {date_range}",
        template_name="email/completion_summary.html",
        context={
            "user": user,
            "total_tasks": total_tasks,
            "total_time": total_time,
            "total_time_display": f"{total_time // 60}h {total_time % 60}m"
            if total_time >= 60
            else f"{total_time}m",
            "date_range": date_range,
            "task_groups": task_groups,
            "site_url": SITE_URL,
        },
        to=[email],
    )


# -------------------------------------------------------------------
# Welcome email for new users
# -------------------------------------------------------------------
def send_welcome_email(*, user: User, raw_password: Optional[str] = None) -> None:
    """
    Welcome mail with login details. Skips if user has no email.
    This does NOT CC/BCC anyone (assigner never receives).
    """
    to_email = (getattr(user, "email", "") or "").strip()
    if not to_email:
        return

    username = getattr(user, "username", "") or ""
    ctx = {
        "title": "Welcome to EMS",
        "full_name": _display_name(user),
        "username": username,
        "raw_password": raw_password or "",
        "login_url": SITE_URL,
    }

    fallback_html = f"""
    <html><body>
      <h3>Welcome to EMS</h3>
      <p>Hi {ctx['full_name']},</p>
      <p>Your account has been created.</p>
      <p><strong>Username:</strong> {username}</p>
      {"<p><strong>Password:</strong> " + ctx["raw_password"] + "</p>" if raw_password else ""}
      <p><a href="{SITE_URL}">Login here</a></p>
    </body></html>
    """.strip()

    html_body = _render_or_fallback("email/welcome_user.html", ctx, fallback_html)

    try:
        msg = EmailMultiAlternatives(
            subject="👋 Welcome to EMS",
            body=html_body,
            from_email=_from_email(),
            to=[to_email],
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=_fail_silently())
        logger.info("Welcome email sent to %s", to_email)
    except Exception as e:
        logger.error("Failed to send welcome email to %s: %s", to_email, e)


# -------------------------------------------------------------------
# Diagnostics
# -------------------------------------------------------------------
def test_email_configuration() -> bool:
    """Send a single test message to DEFAULT_FROM_EMAIL; return True on success."""
    try:
        from_addr = _from_email()
        to_addr = from_addr
        send_mail(
            subject="EMS Email Configuration Test",
            message="This is a test email to verify email configuration.",
            from_email=from_addr,
            recipient_list=[to_addr],
            fail_silently=False,
        )
        logger.info("Email configuration test successful")
        return True
    except Exception as e:
        logger.error("Email configuration test failed: %s", e)
        return False


def get_email_statistics() -> Dict[str, Any]:
    """Return basic, placeholder stats (extend with provider API if needed)."""
    return {
        "emails_sent_today": 0,
        "emails_failed_today": 0,
        "email_service_status": "active",
        "last_email_sent": timezone.now(),
    }


__all__ = [
    # core
    "send_html_email",
    "get_admin_emails",
    "test_email_configuration",
    "get_email_statistics",
    # assignments
    "send_checklist_assignment_to_user",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
    # admin confirmations (assigner auto-excluded)
    "send_checklist_admin_confirmation",
    "send_delegation_admin_confirmation",
    "send_help_ticket_admin_confirmation",
    # unassign notices
    "send_checklist_unassigned_notice",
    "send_delegation_unassigned_notice",
    "send_help_ticket_unassigned_notice",
    # summaries / reminders
    "send_admin_bulk_summary",
    "send_bulk_completion_summary",
    "send_task_reminder_email",
    # welcome
    "send_welcome_email",
    # helpers
    "_dedupe_emails",
    "_fmt_value",
    "_fmt_items",
    "_fmt_rows",
    "_display_name",
    "_fmt_dt_date",
    "_render_or_fallback",
    "_send_unified_assignment_email",
    "_safe_console_text",
    "_send_email",
]
