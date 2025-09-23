import logging
import sys
from datetime import datetime
from typing import Iterable, List, Optional, Sequence

import pytz
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.db import transaction, connection
from django.utils import timezone
from django.urls import reverse
from django.template.loader import render_to_string
from django.template import TemplateDoesNotExist

from apps.settings.models import Holiday
from .models import Checklist, Delegation, HelpTicket

logger = logging.getLogger(__name__)
User = get_user_model()

# ---- Constants ----
IST = pytz.timezone("Asia/Kolkata")
SITE_URL: str = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
DEFAULT_FROM_EMAIL: Optional[str] = getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(
    settings, "EMAIL_HOST_USER", None
)
EMAIL_FAIL_SILENTLY: bool = bool(getattr(settings, "EMAIL_FAIL_SILENTLY", False) or getattr(settings, "DEBUG", False))
SEND_WELCOME_EMAILS: bool = bool(getattr(settings, "SEND_WELCOME_EMAILS", True))


# ======================================================================
#                    LOGGING / ENCODING SAFETY HELPERS
# ======================================================================

def _safe_console_text(s: object) -> str:
    """
    Return a version of `s` that can be safely written to the current console stream
    (e.g., Windows CP1252) without raising UnicodeEncodeError.
    Always replaces non-encodable characters.
    """
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)

    enc = getattr(sys.stdout, "encoding", None) or getattr(sys.stderr, "encoding", None) or "utf-8"
    try:
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        # Ultimate fallback to pure ASCII with replacements
        return text.encode("ascii", errors="replace").decode("ascii", errors="replace")


# ======================================================================
#                            UTILITY HELPERS
# ======================================================================

def _safe_list(items: Iterable[Optional[str]]) -> List[str]:
    """Deduplicate + drop blanks while preserving order."""
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


def _collect_cc_emails(obj) -> List[str]:
    """
    Best-effort collector that supports multiple shapes of "cc" on task-like objects:
      • ManyToMany to User on `cc` or `cc_users`
      • Iterable[str] on `cc_list` / `cc_emails`
      • Comma-separated string on `cc_emails`
      • Single user on `notify_to` (legacy) will be included too
    """
    emails: List[str] = []

    def _append_user(u):
        try:
            e = (getattr(u, "email", "") or "").strip()
            if e:
                emails.append(e)
        except Exception:
            pass

    try:
        # M2M: .cc or .cc_users
        for attr in ("cc", "cc_users"):
            rel = getattr(obj, attr, None)
            if rel is not None:
                try:
                    for u in rel.all():
                        _append_user(u)
                except Exception:
                    # If it's already a list
                    for u in list(rel or []):
                        _append_user(u)
    except Exception:
        pass

    # Plain lists
    for attr in ("cc_list", "cc_emails"):
        try:
            val = getattr(obj, attr, None)
            if isinstance(val, (list, tuple, set)):
                for it in val:
                    s = (str(it) or "").strip()
                    if s:
                        emails.append(s)
            elif isinstance(val, str):
                parts = [p.strip() for p in val.split(",")]
                for s in parts:
                    if s:
                        emails.append(s)
        except Exception:
            pass

    # Legacy: single notify_to user
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
    fail_silently: Optional[bool] = None,
) -> None:
    """
    Small wrapper around EmailMultiAlternatives with logging.
    Uses console-safe logging to avoid UnicodeEncodeError on Windows terminals.

    Supports multiple CC recipients via `cc`.
    """
    rcpts = _safe_list(recipients)
    if not rcpts:
        logger.info(_safe_console_text(f"Skipped email '{subject}': no recipients"))
        return

    if fail_silently is None:
        fail_silently = EMAIL_FAIL_SILENTLY

    cc_list = _safe_list(cc or [])

    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=(text_body or " "),
            from_email=DEFAULT_FROM_EMAIL,
            to=rcpts,
            cc=cc_list or None,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=fail_silently)
        cc_log = f" cc={', '.join(cc_list)}" if cc_list else ""
        logger.debug(_safe_console_text(f"Sent email '{subject}' to {', '.join(rcpts)}{cc_log}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Email send failed for '{subject}': {e}"))


def _cta_button(url: str, label: str = "Open / Complete") -> str:
    return f"""
      <div style="margin:24px 0">
        <a href="{url}" style="
            background:#4f46e5;color:#fff;text-decoration:none;
            padding:12px 18px;border-radius:8px;display:inline-block;
            font-weight:600;">
          {label}
        </a>
      </div>
    """


def _shell_html(title: str, heading: str, body_html: str) -> str:
    """Simple, inline-styled shell suitable for most mailbox clients."""
    return f"""<!doctype html>
<html><body style="font-family:Inter,Arial,Helvetica,sans-serif;background:#f6f8fb;margin:0;padding:0">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f8fb">
    <tr><td align="center" style="padding:24px">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;background:#ffffff;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.05);overflow:hidden">
        <tr>
          <td style="background:linear-gradient(135deg,#4f46e5,#6366f1);padding:18px 22px;color:#fff;font-weight:700;font-size:18px">
            {title}
          </td>
        </tr>
        <tr>
          <td style="padding:22px 22px 8px 22px;color:#0f172a;font-size:18px;font-weight:700">
            {heading}
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
            <div><a href="{SITE_URL}" style="color:#4f46e5;text-decoration:none">{SITE_URL}</a></div>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body></html>"""


def _assignment_html(
    task_title: str,
    task_name: str,
    assigner: Optional[User],
    planned_dt: Optional[datetime],
    complete_url: Optional[str],
    extra_lines: Optional[List[str]] = None,
    cta_label: str = "Open / Complete",
) -> str:
    planned_str = (
        timezone.localtime(planned_dt, IST).strftime("%a, %d %b %Y • %I:%M %p IST")
        if planned_dt else "Not specified"
    )
    who = (assigner.get_full_name() or assigner.username) if assigner else "System"
    lines = [
        f"<strong>Task:</strong> {task_name}",
        f"<strong>Planned:</strong> {planned_str}",
        f"<strong>Assigned by:</strong> {who}",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    body = "<p>" + "<br/>".join(lines) + "</p>"
    if complete_url:
        body += _cta_button(complete_url, cta_label)
    return _shell_html(task_title, "You have a new assignment", body)


def _info_html(title: str, heading: str, lines: List[str]) -> str:
    body = "<p>" + "<br/>".join(lines) + "</p>"
    return _shell_html(title, heading, body)


def _on_commit(fn):
    """
    Run callable after successful DB commit when inside an atomic block; otherwise run immediately.
    This prevents calling transaction.on_commit() from background threads where no transaction is active.
    """
    try:
        if connection.in_atomic_block:
            transaction.on_commit(fn)
        else:
            fn()
    except Exception:
        # As a final fallback, run immediately
        try:
            fn()
        except Exception as e:
            logger.error(_safe_console_text(f"Deferred email send failed: {e}"))


def is_working_day(d) -> bool:
    """Shared helper: Mon–Sat, excluding configured holidays (Sunday = 6)."""
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()


# ======================================================================
#                         OUTBOUND EMAIL FUNCTIONS
# ======================================================================

def send_checklist_assignment_to_user(
    *,
    task: Checklist,
    complete_url: Optional[str],
    subject_prefix: str = "Checklist Assigned",
) -> None:
    """Notify assignee for a Checklist task (supports multi-CC)."""
    if not task or not task.assign_to:
        return
    subject = f"{subject_prefix}: {task.task_name}"
    html = _assignment_html(
        task_title="Checklist Task",
        task_name=task.task_name,
        assigner=task.assign_by,
        planned_dt=task.planned_date,
        complete_url=complete_url,
        cta_label="Open Checklist",
    )
    recipients = [task.assign_to.email]
    cc_emails = _collect_cc_emails(task)
    _on_commit(lambda: _send_email(subject, recipients, html, cc=cc_emails))


def send_delegation_assignment_to_user(
    *,
    delegation: Delegation,
    complete_url: Optional[str],
    subject_prefix: str = "Delegation Assigned",
) -> None:
    """Notify assignee for a Delegation task (supports multi-CC)."""
    if not delegation or not delegation.assign_to:
        return
    subject = f"{subject_prefix}: {delegation.task_name}"
    html = _assignment_html(
        task_title="Delegation Task",
        task_name=delegation.task_name,
        assigner=delegation.assign_by,
        planned_dt=delegation.planned_date,
        complete_url=complete_url,
        cta_label="Open Delegation",
    )
    recipients = [delegation.assign_to.email]
    cc_emails = _collect_cc_emails(delegation)
    _on_commit(lambda: _send_email(subject, recipients, html, cc=cc_emails))


def send_help_ticket_assignment_to_user(
    *,
    ticket: HelpTicket,
    complete_url: Optional[str],
    subject_prefix: str = "Help Ticket Assigned",
) -> None:
    """Notify assignee for a Help Ticket (supports multi-CC)."""
    if not ticket or not ticket.assign_to:
        return
    subject = f"{subject_prefix}: {ticket.title}"
    planned = ticket.planned_date
    html = _assignment_html(
        task_title="Help Ticket",
        task_name=ticket.title,
        assigner=ticket.assign_by,
        planned_dt=planned,
        complete_url=complete_url,
        cta_label="Open Ticket",
        extra_lines=[f"<strong>Ticket ID:</strong> HT-{ticket.id}"],
    )
    recipients = [ticket.assign_to.email]
    cc_emails = _collect_cc_emails(ticket)
    _on_commit(lambda: _send_email(subject, recipients, html, cc=cc_emails))


def send_recurring_assignment_to_user(
    *,
    task: Checklist,
    complete_url: Optional[str],
    subject_prefix: str = "Recurring Checklist Generated",
) -> None:
    """Explicit function for recurring checklist emails (alias of checklist assignment)."""
    send_checklist_assignment_to_user(task=task, complete_url=complete_url, subject_prefix=subject_prefix)


def send_checklist_admin_confirmation(
    *,
    task: Checklist,
    subject_prefix: str = "Checklist Task Assignment",
) -> None:
    """Confirmation to assigner / notify emails."""
    subject = f"{subject_prefix}: {task.task_name}"
    planned_str = (
        timezone.localtime(task.planned_date, IST).strftime("%a, %d %b %Y • %I:%M %p IST")
        if task.planned_date else "Not specified"
    )
    assignee_name = task.assign_to.get_full_name() or task.assign_to.username
    who = task.assign_by.get_full_name() or task.assign_by.username if task.assign_by else "System"
    lines = [
        f"<strong>Task:</strong> {task.task_name}",
        f"<strong>Planned:</strong> {planned_str}",
        f"<strong>Assigned to:</strong> {assignee_name}",
        f"<strong>Assigned by:</strong> {who}",
    ]
    html = _info_html("Checklist Assignment - Admin Copy", "Assignment Confirmation", lines)
    recipients = _safe_list([
        getattr(task.assign_by, "email", ""),
        getattr(getattr(task, "notify_to", None), "email", ""),
    ])
    if not recipients:
        # fallback to ADMINS if configured
        recipients = [email for _, email in getattr(settings, "ADMINS", [])]
    if recipients:
        _on_commit(lambda: _send_email(subject, recipients, html))


def send_checklist_unassigned_notice(
    *,
    task: Checklist,
    old_user: User,
) -> None:
    """Notify previous assignee that a task was reassigned away from them."""
    if not old_user or not getattr(old_user, "email", None):
        return
    subject = f"Checklist Reassigned: {task.task_name}"
    now_assignee = task.assign_to.get_full_name() or task.assign_to.username
    lines = [
        f"The task <strong>{task.task_name}</strong> is no longer assigned to you.",
        f"It is now assigned to <strong>{now_assignee}</strong>.",
    ]
    html = _info_html("Checklist Reassigned", "You are unassigned from a task", lines)
    _on_commit(lambda: _send_email(subject, [old_user.email], html))


def send_help_ticket_admin_confirmation(
    *,
    ticket: HelpTicket,
    subject_prefix: str = "Help Ticket Assignment",
) -> None:
    subject = f"{subject_prefix}: {ticket.title}"
    planned_str = (
        timezone.localtime(ticket.planned_date, IST).strftime("%a, %d %b %Y • %I:%M %p IST")
        if ticket.planned_date else "Not specified"
    )
    assignee_name = ticket.assign_to.get_full_name() or ticket.assign_to.username
    who = ticket.assign_by.get_full_name() or ticket.assign_by.username if ticket.assign_by else "System"
    lines = [
        f"<strong>Ticket:</strong> {ticket.title} (HT-{ticket.id})",
        f"<strong>Planned:</strong> {planned_str}",
        f"<strong>Assigned to:</strong> {assignee_name}",
        f"<strong>Assigned by:</strong> {who}",
    ]
    html = _info_html("Help Ticket - Admin Copy", "Assignment Confirmation", lines)
    recipients = _safe_list([
        getattr(ticket.assign_by, "email", ""),
        getattr(getattr(ticket, "notify_to", None), "email", ""),
    ])
    if not recipients:
        recipients = [email for _, email in getattr(settings, "ADMINS", [])]
    if recipients:
        _on_commit(lambda: _send_email(subject, recipients, html))


def send_help_ticket_unassigned_notice(
    *,
    ticket: HelpTicket,
    old_user: User,
) -> None:
    if not old_user or not getattr(old_user, "email", None):
        return
    subject = f"Help Ticket Reassigned: {ticket.title} (HT-{ticket.id})"
    now_assignee = ticket.assign_to.get_full_name() or ticket.assign_to.username
    lines = [
        f"The help ticket <strong>{ticket.title}</strong> (HT-{ticket.id}) is no longer assigned to you.",
        f"It is now assigned to <strong>{now_assignee}</strong>.",
    ]
    html = _info_html("Help Ticket Reassigned", "You are unassigned from a ticket", lines)
    _on_commit(lambda: _send_email(subject, [old_user.email], html))


def send_admin_bulk_summary(
    *,
    title: str,
    rows: List[dict],
) -> None:
    """
    Send a compact summary to admins after bulk upload.
    Caller must pass ASCII-safe subject (title) like:
      "Bulk Upload: 162 Checklist Tasks Created"
    """
    subject = title or "Bulk Upload Summary"

    def _table() -> str:
        if not rows:
            return "<p>No rows.</p>"
        headers = ["Task Name", "Assign To", "Planned Date", "Priority"]

        # build rows safely (avoid XSS via simple escaping)
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
        head = "<tr>" + "".join(
            f"<th style='text-align:left;padding:8px 10px;border-bottom:1px solid #e5e7eb'>{h}</th>" for h in headers
        ) + "</tr>"
        body = "".join(trs)
        return f"""
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
                 style="border-collapse:collapse;font-size:13px">
            {head}
            {body}
          </table>
        """

    html = _shell_html("Bulk Upload Summary", "Tasks Created (preview of first few)", _table())

    # recipients: ADMINS + active superusers with emails
    rcpts = [email for _, email in getattr(settings, "ADMINS", [])]
    staff = User.objects.filter(is_active=True, is_superuser=True)[:50]
    rcpts.extend([u.email for u in staff if u.email])
    rcpts = _safe_list(rcpts)
    if rcpts:
        _on_commit(lambda: _send_email(subject, rcpts, html))


# ======================================================================
#                     NEW USER WELCOME EMAIL (🎉)
# ======================================================================

def send_new_user_welcome(
    user: User,
    *,
    temp_password: Optional[str] = None,
    login_url: Optional[str] = None,
    subject_prefix: str = "Welcome to BOS EMS",
) -> None:
    """
    Send a welcome email to a newly-created user.
    Rules:
      • Only the new user receives it (never the assigner/admin).
      • If SEND_WELCOME_EMAILS=False, skip.
      • Uses HTML & TXT templates if present; falls back to a styled shell.
    """
    try:
        if not SEND_WELCOME_EMAILS:
            logger.info("Welcome emails disabled via SEND_WELCOME_EMAILS")
            return
        if not user or not getattr(user, "email", None):
            return

        display_name = (user.get_full_name() or user.username or "there").strip() or "there"

        # Login URL resolution
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
            "temp_password": temp_password,  # include only if you pass it
            "site_url": SITE_URL,
            "login_url": resolved_login,
        }

        # Try template render; fallback to inline shell
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
            body = "<p>" + "<br/>".join(lines) + "</p>" + _cta_button(resolved_login, "Go to Login")
            html_body = _shell_html("Welcome to BOS EMS", "Account Created", body)

        subject = subject_prefix

        _on_commit(lambda: _send_email(subject, [user.email], html_body, text_body))
        logger.info(_safe_console_text(f"Sent welcome email to {user.email}"))

    except Exception as e:
        logger.error(_safe_console_text(f"Failed to send welcome email to {getattr(user, 'email', '?')}: {e}"))


# ======================================================================
#                     TASK HANDOVER EMAIL (Leave)
# ======================================================================

def send_task_handover_notice(
    *,
    to_user: User,
    cc_users_or_emails: Iterable,
    handover_lines: List[str],
    leave_window: str,
    actor_name: str,
    subject_prefix: str = "Task Handover (Leave)",
) -> None:
    """
    Send a concise handover summary to the delegate with CC to others.
    `handover_lines` should be a list of bullet strings describing each handed-over task.
    """
    if not to_user or not getattr(to_user, "email", None):
        return

    subject = f"{subject_prefix}: {actor_name}"
    body = "<p>" + "<br/>".join([
        f"<strong>From:</strong> {actor_name}",
        f"<strong>Leave Window:</strong> {leave_window}",
        "The following items are temporarily handed over to you:",
    ]) + "</p>"

    if handover_lines:
        body += "<ul style='margin-top:6px'>" + "".join(f"<li>{_safe_console_text(x)}</li>" for x in handover_lines) + "</ul>"

    html = _shell_html("Task Handover", "Temporary delegation due to leave", body)

    # Collect CCs (accept users or emails)
    cc_raw: List[str] = []
    for x in cc_users_or_emails or []:
        if isinstance(x, str):
            cc_raw.append(x)
        else:
            cc_raw.append(getattr(x, "email", "") or "")
    cc_emails = _safe_list(cc_raw)

    _on_commit(lambda: _send_email(subject, [to_user.email], html, cc=cc_emails))


__all__ = [
    # core senders
    "send_checklist_assignment_to_user",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
    "send_recurring_assignment_to_user",
    "send_checklist_admin_confirmation",
    "send_checklist_unassigned_notice",
    "send_help_ticket_admin_confirmation",
    "send_help_ticket_unassigned_notice",
    "send_admin_bulk_summary",
    # helpers
    "_safe_console_text",
    "is_working_day",
    # new
    "send_new_user_welcome",
    "send_task_handover_notice",
]
