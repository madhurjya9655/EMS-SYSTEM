# apps/tasks/utils.py  (fully updated)

from __future__ import annotations
import logging
import sys
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Dict
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.mail import EmailMultiAlternatives
from django.core.validators import validate_email
from django.db import transaction, connection
from django.template import TemplateDoesNotExist
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone
from apps.settings.models import Holiday
from .models import Checklist, Delegation, HelpTicket
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)
User = get_user_model()

# tz handling (ZoneInfo preferred)
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
except Exception:
    import pytz
    IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))

def _normalize_site_url(u: str) -> str:
    if not u:
        return ""
    parts = list(urlsplit(u.strip()))
    parts[2] = parts[2].rstrip("/")
    return urlunsplit(parts)

SITE_URL: str = _normalize_site_url(getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com"))
DEFAULT_FROM_EMAIL: Optional[str] = getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None)
EMAIL_FAIL_SILENTLY: bool = bool(getattr(settings, "EMAIL_FAIL_SILENTLY", False) or getattr(settings, "DEBUG", False))
SEND_WELCOME_EMAILS: bool = bool(getattr(settings, "SEND_WELCOME_EMAILS", True))
DT_FMT = "%a, %d %b %Y • %I:%M %p %Z"

def _safe_console_text(s: object) -> str:
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    enc = getattr(sys.stdout, "encoding", None) or getattr(sys.stderr, "encoding", None) or "utf-8"
    try:
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return text.encode("ascii", errors="replace").decode("ascii", errors="replace")

def _esc(s: object) -> str:
    from django.utils.html import escape
    return escape("" if s is None else str(s))

def _fmt_dt(dt: Optional[datetime], *, tz=IST, fmt: str = DT_FMT) -> str:
    if not dt:
        return "Not specified"
    tz = tz or timezone.get_current_timezone()
    if timezone.is_naive(dt):
        try:
            dt = timezone.make_aware(dt, tz)
        except Exception:
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
    try:
        dt_local = timezone.localtime(dt, tz)
    except Exception:
        dt_local = dt
    return dt_local.strftime(fmt)

def _safe_list(items: Iterable[Optional[str]]) -> List[str]:
    seen = set()
    out: List[str] = []
    for it in items:
        if not it:
            continue
        s = str(it).strip()
        if not s or s.lower() == "none":
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out

def _maybe_valid_email(s: str) -> Optional[str]:
    try:
        validate_email(s)
        return s
    except ValidationError:
        return None

def _collect_cc_emails(obj) -> List[str]:
    """
    Collect possible CC lists from common fields (notify_to, cc relations, etc.)
    NOTE: We no longer CC on *assignee* emails; this is useful for admin summaries or notices.
    """
    emails: List[str] = []

    def _append_user(u):
        try:
            e = (getattr(u, "email", "") or "").strip()
            if e and (valid := _maybe_valid_email(e)):
                emails.append(valid)
        except Exception:
            pass

    try:
        for attr in ("cc", "cc_users"):
            rel = getattr(obj, attr, None)
            if rel is not None:
                try:
                    for u in rel.all():
                        _append_user(u)
                except Exception:
                    for u in list(rel or []):
                        _append_user(u)
    except Exception:
        pass

    for attr in ("cc_list", "cc_emails"):
        try:
            val = getattr(obj, attr, None)
            if isinstance(val, (list, tuple, set)):
                for it in val:
                    s = (str(it) or "").strip()
                    if s and (valid := _maybe_valid_email(s)):
                        emails.append(valid)
            elif isinstance(val, str):
                parts = [p.strip() for p in val.split(",")]
                for s in parts:
                    if s and (valid := _maybe_valid_email(s)):
                        emails.append(valid)
        except Exception:
            pass

    try:
        nt = getattr(obj, "notify_to", None)
        if nt:
            _append_user(nt)
    except Exception:
        pass

    return _safe_list(emails)

def _send_email(
    subject: str,
    recipients: Sequence[str],
    html_body: str,
    text_body: Optional[str] = None,
    *,
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    reply_to: Optional[Sequence[str]] = None,
    fail_silently: Optional[bool] = None,
) -> bool:
    rcpts = _safe_list(filter(None, (r.strip() for r in recipients if r))) if recipients else []
    if not rcpts:
        logger.info(_safe_console_text(f"Skipped email '{subject}': no recipients"))
        return False
    if fail_silently is None:
        fail_silently = EMAIL_FAIL_SILENTLY
    cc_list = _safe_list(cc or [])
    bcc_list = _safe_list(bcc or [])
    reply_to_list = _safe_list(reply_to or [])
    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=(text_body or " "),
            from_email=DEFAULT_FROM_EMAIL,
            to=rcpts,
            cc=cc_list or None,
            bcc=bcc_list or None,
            reply_to=reply_to_list or None,
        )
        msg.attach_alternative(html_body, "text/html")
        try:
            msg.extra_headers = {
                "List-ID": "bos-ems <notifications.bos-ems>",
                "X-Auto-Response-Suppress": "All",
            }
        except Exception:
            pass
        msg.send(fail_silently=fail_silently)
        cc_log = f" cc={', '.join(cc_list)}" if cc_list else ""
        bcc_log = f" bcc={', '.join(bcc_list)}" if bcc_list else ""
        rto_log = f" reply_to={', '.join(reply_to_list)}" if reply_to_list else ""
        logger.debug(_safe_console_text(f"Sent email '{subject}' to {', '.join(rcpts)}{cc_log}{bcc_log}{rto_log}"))
        return True
    except Exception as e:
        logger.error(_safe_console_text(f"Email send failed for '{subject}': {e}"))
        return False

def _cta_button(url: str, label: str = "Open / Complete") -> str:
    return f"""
      <div style="margin:24px 0">
        <a href="{_esc(url)}" style="
            background:#4f46e5;color:#fff;text-decoration:none;
            padding:12px 18px;border-radius:8px;display:inline-block;
            font-weight:600;">
          {_esc(label)}
        </a>
      </div>
    """

def _shell_html(title: str, heading: str, body_html: str) -> str:
    return f"""<!doctype html>
<html><body style="font-family:Inter,Arial,Helvetica,sans-serif;background:#f6f8fb;margin:0;padding:0">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f8fb">
    <tr><td align="center" style="padding:24px">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;background:#ffffff;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.05);overflow:hidden">
        <tr>
          <td style="background:linear-gradient(135deg,#4f46e5,#6366f1);padding:18px 22px;color:#fff;font-weight:700;font-size:18px">
            {_esc(title)}
          </td>
        </tr>
        <tr>
          <td style="padding:22px 22px 8px 22px;color:#0f172a;font-size:18px;font-weight:700">
            {_esc(heading)}
          </td>
        </tr>
        <tr>
          <td style="padding:0 22px 22px 22px;color:#334155;font-size:14px;line-height:1.6">
            {body_html}
          </td>
        </tr>
        <tr>
          <td style="padding:16px 22px;color:#64748b;font-size:12px;border-top:1px solid #e2e8f0">
            <div>Sent by BOS EMS</div>
            <div><a href="{_esc(SITE_URL)}" style="color:#4f46e5;text-decoration:none">{_esc(SITE_URL)}</a></div>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

def _html_instructions_block(label: str, text: str) -> str:
    from django.utils.html import escape
    safe = escape(text).replace("\n", "<br/>")
    return f"""
      <div style="margin-top:14px;padding:12px 14px;background:#f8fafc;border-left:4px solid #4f46e5;border-radius:8px">
        <div style="font-weight:700;margin-bottom:6px;color:#0f172a">{_esc(label)}</div>
        <div style="color:#334155;font-size:14px;line-height:1.6">{safe}</div>
      </div>
    """

def _txt_instructions_block(label: str, text: str) -> str:
    sep = "-" * len(label)
    return f"\n{label}\n{sep}\n{text}\n"

def _extract_message_for(obj) -> tuple[str, str]:
    label = ""
    text = ""
    for attr in ("message", "description"):
        try:
            val = getattr(obj, attr, None)
            if val:
                text = str(val).strip()
                if text:
                    label = "Instructions" if attr == "message" else "Description"
                    break
        except Exception:
            continue
    if not text:
        return "", ""
    return label, text

def _assignment_html(
    task_title: str,
    task_name: str,
    assigner: Optional[User],
    planned_dt: Optional[datetime],
    complete_url: Optional[str],
    *,
    extra_kv: Optional[Dict[str, str]] = None,
    message_tuple: tuple[str, str] = ("", ""),
    cta_label: str = "Open / Complete",
) -> str:
    planned_str = _fmt_dt(planned_dt, tz=IST)
    who = (assigner.get_full_name() or assigner.username) if assigner else "System"
    lines = [
        f"<strong>Task:</strong> {_esc(task_name)}",
        f"<strong>Planned:</strong> {_esc(planned_str)}",
        f"<strong>Assigned by:</strong> {_esc(who)}",
    ]
    if extra_kv:
        lines.extend([f"<strong>{_esc(k)}:</strong> {_esc(v)}" for k, v in extra_kv.items()])
    body = "<p>" + "<br/>".join(lines) + "</p>"
    if message_tuple and message_tuple[0]:
        body += _html_instructions_block(message_tuple[0], message_tuple[1])
    if complete_url:
        body += _cta_button(complete_url, cta_label)
    return _shell_html(task_title, "You have a new assignment", body)

def _assignment_text(
    task_name: str,
    planned_dt: Optional[datetime],
    assigner: Optional[User],
    extra_lines: Optional[List[str]] = None,
    message_tuple: tuple[str, str] = ("", ""),
    action_url: Optional[str] = None,
) -> str:
    planned_str = _fmt_dt(planned_dt, tz=IST)
    who = (assigner.get_full_name() or assigner.username) if assigner else "System"
    parts = [
        f"Task: {task_name}",
        f"Planned: {planned_str}",
        f"Assigned by: {who}",
    ]
    if extra_lines:
        parts.extend(extra_lines)
    txt = "\n".join(parts)
    if message_tuple and message_tuple[0]:
        txt += _txt_instructions_block(message_tuple[0], message_tuple[1])
    if action_url:
        txt += f"\nOpen/Complete: {action_url}\n"
    txt += f"\n--\nBOS EMS • {SITE_URL}\n"
    return txt

def _info_html(title: str, heading: str, lines: List[str]) -> str:
    body = "<p>" + "<br/>".join(lines) + "</p>"
    return _shell_html(title, heading, body)

def _on_commit(fn):
    try:
        if connection.in_atomic_block:
            transaction.on_commit(fn)
        else:
            fn()
    except Exception as e:
        logger.error(_safe_console_text(f"Deferred email send failed: {e}"))

def is_working_day(d) -> bool:
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()

# ---------------------------
# User-facing assignment mails
# ---------------------------

def send_checklist_assignment_to_user(
    *, task: Checklist, complete_url: Optional[str], subject_prefix: str = "Checklist Assigned",
) -> None:
    if not task or not task.assign_to:
        return

    # Suppress self-assign emails (assigner == assignee)
    try:
        if task.assign_by_id and task.assign_by_id == task.assign_to_id:
            logger.info("Checklist email suppressed (self-assign) for CL-%s", getattr(task, "id", "?"))
            return
    except Exception:
        pass

    subject = f"{subject_prefix}: {task.task_name}"
    msg_tuple = _extract_message_for(task)
    html = _assignment_html(
        task_title="Checklist Task",
        task_name=task.task_name,
        assigner=task.assign_by,
        planned_dt=task.planned_date,
        complete_url=complete_url,
        message_tuple=msg_tuple,
        cta_label="Open Checklist",
    )
    text = _assignment_text(
        task_name=task.task_name,
        planned_dt=task.planned_date,
        assigner=task.assign_by,
        message_tuple=msg_tuple,
        action_url=complete_url,
    )
    recipients = [getattr(task.assign_to, "email", "")]
    # Assignee-only — no CC/BCC on user-facing assignment.
    reply_to = [getattr(task.assign_by, "email", "")] if getattr(task, "assign_by", None) else None
    _on_commit(lambda: _send_email(subject, recipients, html, text_body=text, reply_to=reply_to))

def send_delegation_assignment_to_user(
    *, delegation: Delegation, complete_url: Optional[str], subject_prefix: str = "Delegation Assigned",
) -> None:
    if not delegation or not delegation.assign_to:
        return

    try:
        if delegation.assign_by_id and delegation.assign_by_id == delegation.assign_to_id:
            logger.info("Delegation email suppressed (self-assign) for DL-%s", getattr(delegation, "id", "?"))
            return
    except Exception:
        pass

    subject = f"{subject_prefix}: {delegation.task_name}"
    msg_tuple = _extract_message_for(delegation)
    html = _assignment_html(
        task_title="Delegation Task",
        task_name=delegation.task_name,
        assigner=delegation.assign_by,
        planned_dt=delegation.planned_date,
        complete_url=complete_url,
        message_tuple=msg_tuple,
        cta_label="Open Delegation",
    )
    text = _assignment_text(
        task_name=delegation.task_name,
        planned_dt=delegation.planned_date,
        assigner=delegation.assign_by,
        message_tuple=msg_tuple,
        action_url=complete_url,
    )
    recipients = [getattr(delegation.assign_to, "email", "")]
    reply_to = [getattr(delegation.assign_by, "email", "")] if getattr(delegation, "assign_by", None) else None
    _on_commit(lambda: _send_email(subject, recipients, html, text_body=text, reply_to=reply_to))

def send_help_ticket_assignment_to_user(
    *, ticket: HelpTicket, complete_url: Optional[str], subject_prefix: str = "Help Ticket Assigned",
) -> None:
    if not ticket or not ticket.assign_to:
        return

    try:
        if ticket.assign_by_id and ticket.assign_by_id == ticket.assign_to_id:
            logger.info("Help-ticket email suppressed (self-assign) for HT-%s", getattr(ticket, "id", "?"))
            return
    except Exception:
        pass

    subject = f"{subject_prefix}: {ticket.title}"
    msg_tuple = _extract_message_for(ticket)
    html = _assignment_html(
        task_title="Help Ticket",
        task_name=ticket.title,
        assigner=ticket.assign_by,
        planned_dt=ticket.planned_date,
        complete_url=complete_url,
        extra_kv={"Ticket ID": f"HT-{ticket.id}"},
        message_tuple=msg_tuple,
        cta_label="Open Ticket",
    )
    text = _assignment_text(
        task_name=ticket.title,
        planned_dt=ticket.planned_date,
        assigner=ticket.assign_by,
        message_tuple=msg_tuple,
        action_url=complete_url,
        extra_lines=[f"Ticket ID: HT-{ticket.id}"],
    )
    recipients = [getattr(ticket.assign_to, "email", "")]
    reply_to = [getattr(ticket.assign_by, "email", "")] if getattr(ticket, "assign_by", None) else None
    _on_commit(lambda: _send_email(subject, recipients, html, text_body=text, reply_to=reply_to))

def send_recurring_assignment_to_user(
    *, task: Checklist, complete_url: Optional[str], subject_prefix: str = "Recurring Checklist Generated",
) -> None:
    # Reuse checklist sender (inherits self-assign suppression & assignee-only)
    send_checklist_assignment_to_user(task=task, complete_url=complete_url, subject_prefix=subject_prefix)

# ---------------------------
# Admin confirmations / notices
# ---------------------------

def send_checklist_admin_confirmation(
    *, task: Checklist, subject_prefix: str = "Checklist Task Assignment",
) -> None:
    subject = f"{subject_prefix}: {task.task_name}"
    planned_str = _fmt_dt(task.planned_date, tz=IST)
    assignee_name = task.assign_to.get_full_name() or task.assign_to.username
    who = task.assign_by.get_full_name() or task.assign_by.username if task.assign_by else "System"
    lines = [
        f"<strong>Task:</strong> {_esc(task.task_name)}",
        f"<strong>Planned:</strong> {_esc(planned_str)}",
        f"<strong>Assigned to:</strong> {_esc(assignee_name)}",
        f"<strong>Assigned by:</strong> {_esc(who)}",
    ]
    msg_label, msg_text = _extract_message_for(task)
    if msg_label:
        lines.append(f"<strong>{_esc(msg_label)}:</strong> {_esc(msg_text)}")
    html = _info_html("Checklist Assignment - Admin Copy", "Assignment Confirmation", lines)

    # EXCLUDE assigner to respect “assigner should never receive emails”.
    recipients = _safe_list([
        getattr(getattr(task, "notify_to", None), "email", ""),
    ])
    if not recipients:
        recipients = [email for _, email in getattr(settings, "ADMINS", [])]
    if recipients:
        _on_commit(lambda: _send_email(subject, recipients, html))

def send_checklist_unassigned_notice(*, task: Checklist, old_user: User) -> None:
    if not old_user or not getattr(old_user, "email", None):
        return
    subject = f"Checklist Reassigned: {task.task_name}"
    now_assignee = task.assign_to.get_full_name() or task.assign_to.username
    lines = [
        f"The task <strong>{_esc(task.task_name)}</strong> is no longer assigned to you.",
        f"It is now assigned to <strong>{_esc(now_assignee)}</strong>.",
    ]
    html = _info_html("Checklist Reassigned", "You are unassigned from a task", lines)
    _on_commit(lambda: _send_email(subject, [old_user.email], html))

def send_help_ticket_admin_confirmation(
    *, ticket: HelpTicket, subject_prefix: str = "Help Ticket Assignment",
) -> None:
    subject = f"{subject_prefix}: {ticket.title}"
    planned_str = _fmt_dt(ticket.planned_date, tz=IST)
    assignee_name = ticket.assign_to.get_full_name() or ticket.assign_to.username
    who = ticket.assign_by.get_full_name() or ticket.assign_by.username if ticket.assign_by else "System"
    lines = [
        f"<strong>Ticket:</strong> {_esc(ticket.title)} (HT-{_esc(ticket.id)})",
        f"<strong>Planned:</strong> {_esc(planned_str)}",
        f"<strong>Assigned to:</strong> {_esc(assignee_name)}",
        f"<strong>Assigned by:</strong> {_esc(who)}",
    ]
    msg_label, msg_text = _extract_message_for(ticket)
    if msg_label:
        lines.append(f"<strong>{_esc(msg_label)}:</strong> {_esc(msg_text)}")
    html = _info_html("Help Ticket - Admin Copy", "Assignment Confirmation", lines)

    # EXCLUDE assigner (align with policy)
    recipients = _safe_list([
        getattr(getattr(ticket, "notify_to", None), "email", ""),
    ])
    if not recipients:
        recipients = [email for _, email in getattr(settings, "ADMINS", [])]
    if recipients:
        _on_commit(lambda: _send_email(subject, recipients, html))

def send_help_ticket_unassigned_notice(*, ticket: HelpTicket, old_user: User) -> None:
    if not old_user or not getattr(old_user, "email", None):
        return
    subject = f"Help Ticket Reassigned: {ticket.title} (HT-{ticket.id})"
    now_assignee = ticket.assign_to.get_full_name() or ticket.assign_to.username
    lines = [
        f"The help ticket <strong>{_esc(ticket.title)}</strong> (HT-{_esc(ticket.id)}) is no longer assigned to you.",
        f"It is now assigned to <strong>{_esc(now_assignee)}</strong>.",
    ]
    html = _info_html("Help Ticket Reassigned", "You are unassigned from a ticket", lines)
    _on_commit(lambda: _send_email(subject, [old_user.email], html))

def send_admin_bulk_summary(*, title: str, rows: List[dict]) -> None:
    subject = title or "Bulk Upload Summary"
    def _table() -> str:
        if not rows:
            return "<p>No rows.</p>"
        headers = ["Task Name", "Assign To", "Planned Date", "Priority"]
        def esc(s: object) -> str:
            from django.utils.html import escape
            return escape("" if s is None else str(s))
        trs = []
        for r in rows:
            name = esc(r.get("Task Name", ""))
            if r.get("complete_url"):
                name = f'<a href="{esc(r.get("complete_url"))}">{name}</a>'
            assign_to = esc(r.get("Assign To", ""))
            planned = esc(r.get("Planned Date", ""))
            priority = esc(r.get("Priority", ""))
            trs.append(f"<tr><td>{name}</td><td>{assign_to}</td><td>{planned}</td><td>{priority}</td></tr>")
        head = "<tr>" + "".join(f"<th style='text-align:left;padding:8px 10px;border-bottom:1px solid #e5e7eb'>{h}</th>" for h in headers) + "</tr>"
        body = "".join(trs)
        return f"""
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
                 style="border-collapse:collapse;font-size:13px">
            {head}
            {body}
          </table>
        """
    html = _shell_html("Bulk Upload Summary", "Tasks Created (preview of first few)", _table())

    admin_rcpts = [email for _, email in getattr(settings, "ADMINS", [])]
    staff_emails = list(
        User.objects.filter(is_active=True, is_superuser=True)
        .exclude(email="")
        .values_list("email", flat=True)[:50]
    )
    rcpts = _safe_list([*admin_rcpts, *staff_emails])
    if rcpts:
        _on_commit(lambda: _send_email(subject, rcpts, html))

# ---------------------------
# Welcome / Handover
# ---------------------------

def send_new_user_welcome(
    user: User, *, temp_password: Optional[str] = None, login_url: Optional[str] = None, subject_prefix: str = "Welcome to BOS EMS",
) -> None:
    try:
        if not SEND_WELCOME_EMAILS:
            logger.info("Welcome emails disabled via SEND_WELCOME_EMAILS")
            return
        if not user or not getattr(user, "email", None):
            return
        display_name = (user.get_full_name() or user.username or "there").strip() or "there"
        resolved_login = login_url
        if not resolved_login:
            try:
                resolved_login = f"{SITE_URL}{reverse('login')}"
            except Exception:
                resolved_login = f"{SITE_URL}/accounts/login/"
        ctx = {
            "user": user,
            "display_name": display_name,
            "username": user.username,
            "email": user.email,
            "temp_password": temp_password,
            "site_url": SITE_URL,
            "login_url": resolved_login,
        }
        text_body = None
        html_body = None
        try:
            html_body = render_to_string("email/new_user_welcome.html", ctx)
        except TemplateDoesNotExist:
            pass
        try:
            text_body = render_to_string("email/new_user_welcome.txt", ctx)
        except TemplateDoesNotExist:
            pass
        if not html_body:
            lines = [
                f"Hi {display_name},",
                "Your account has been created in BOS EMS.",
                f"Username: {user.username}",
            ]
            if temp_password:
                lines.append(f"Temporary Password: {temp_password}")
            lines.append("You can log in using the button below.")
            body = "<p>" + "<br/>".join(_esc(x) for x in lines) + "</p>" + _cta_button(resolved_login, "Go to Login")
            html_body = _shell_html("Welcome to BOS EMS", "Account Created", body)
        subject = subject_prefix
        _on_commit(lambda: _send_email(subject, [user.email], html_body, text_body))
        logger.info(_safe_console_text(f"Sent welcome email to {user.email}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Failed to send welcome email to {getattr(user, 'email', '?')}: {e}"))

def send_task_handover_notice(
    *, to_user: User, cc_users_or_emails: Iterable, handover_lines: List[str], leave_window: str, actor_name: str,
    subject_prefix: str = "Task Handover (Leave)",
) -> None:
    if not to_user or not getattr(to_user, "email", None):
        return
    subject = f"{subject_prefix}: {actor_name}"
    body = "<p>" + "<br/>".join([
        f"<strong>From:</strong> {_esc(actor_name)}",
        f"<strong>Leave Window:</strong> {_esc(leave_window)}",
        "The following items are temporarily handed over to you:",
    ]) + "</p>"
    if handover_lines:
        from django.utils.html import escape
        body += "<ul style='margin-top:6px'>" + "".join(f"<li>{escape(x)}</li>" for x in handover_lines) + "</ul>"
    html = _shell_html("Task Handover", "Temporary delegation due to leave", body)
    cc_raw: List[str] = []
    for x in cc_users_or_emails or []:
        if isinstance(x, str):
            if valid := _maybe_valid_email(x):
                cc_raw.append(valid)
        else:
            e = getattr(x, "email", "") or ""
            if valid := _maybe_valid_email(e):
                cc_raw.append(valid)
    cc_emails = _safe_list(cc_raw)
    _on_commit(lambda: _send_email(subject, [to_user.email], html, cc=cc_emails))

__all__ = [
    "send_checklist_assignment_to_user",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
    "send_recurring_assignment_to_user",
    "send_checklist_admin_confirmation",
    "send_checklist_unassigned_notice",
    "send_help_ticket_admin_confirmation",
    "send_help_ticket_unassigned_notice",
    "send_admin_bulk_summary",
    "_safe_console_text",
    "is_working_day",
    "send_new_user_welcome",
    "send_task_handover_notice",
]
