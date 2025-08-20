# apps/tasks/utils.py
import logging
import sys
from typing import Iterable, List, Optional, Sequence

import pytz
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.db import transaction
from django.utils import timezone

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


# ======================================================================
#                    LOGGING / ENCODING SAFETY HELPERS
# ======================================================================

def _safe_console_text(s: object) -> str:
    """
    Return a version of `s` that can be safely written to the current console stream
    (e.g., Windows CP1252) without raising UnicodeEncodeError.
    """
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    enc = getattr(sys.stderr, "encoding", None) or getattr(sys.stdout, "encoding", None) or "utf-8"
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


def _send_email(
    subject: str,
    recipients: Sequence[str],
    html_body: str,
    text_body: Optional[str] = None,
    fail_silently: Optional[bool] = None,
) -> None:
    """
    Small wrapper around EmailMultiAlternatives with logging.
    Uses console-safe logging to avoid UnicodeEncodeError on Windows terminals.
    """
    rcpts = _safe_list(recipients)
    if not rcpts:
        logger.info(_safe_console_text(f"Skipped email '{subject}': no recipients"))
        return

    if fail_silently is None:
        fail_silently = EMAIL_FAIL_SILENTLY

    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=(text_body or " "),
            from_email=DEFAULT_FROM_EMAIL,
            to=rcpts,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=fail_silently)
        logger.debug(_safe_console_text(f"Sent email '{subject}' to {', '.join(rcpts)}"))
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
    planned_dt: Optional[timezone.datetime],
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
    """Run callable after successful DB commit; if not in a transaction, run immediately."""
    try:
        transaction.on_commit(fn)
    except Exception:
        # outside atomic block
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
    """Notify assignee for a Checklist task."""
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
    _on_commit(lambda: _send_email(subject, recipients, html))


def send_delegation_assignment_to_user(
    *,
    delegation: Delegation,
    complete_url: Optional[str],
    subject_prefix: str = "Delegation Assigned",
) -> None:
    """Notify assignee for a Delegation task."""
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
    _on_commit(lambda: _send_email(subject, recipients, html))


def send_help_ticket_assignment_to_user(
    *,
    ticket: HelpTicket,
    complete_url: Optional[str],
    subject_prefix: str = "Help Ticket Assigned",
) -> None:
    """Notify assignee for a Help Ticket."""
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
    _on_commit(lambda: _send_email(subject, recipients, html))


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
    rows: dicts like {"Task Name": ..., "Assign To": ..., "Planned Date": ..., "Priority": ..., "complete_url": ...}
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
