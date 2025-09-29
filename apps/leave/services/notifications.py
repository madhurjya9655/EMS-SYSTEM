# apps/leave/services/notifications.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

from django.conf import settings
from django.core import signing
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import get_template
from django.urls import reverse
from django.utils import timezone

from django.contrib.auth import get_user_model
from apps.leave.models import (
    LeaveRequest,
    LeaveDecisionAudit,
    DecisionAction,
    LeaveHandover,
    ApproverMapping,
)

logger = logging.getLogger(__name__)
User = get_user_model()

IST = ZoneInfo("Asia/Kolkata")
TOKEN_SALT = getattr(settings, "LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
TOKEN_MAX_AGE_SECONDS = getattr(settings, "LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _site_base() -> str:
    base = (
        getattr(settings, "SITE_URL", "")
        or getattr(settings, "SITE_BASE_URL", "")
        or "http://localhost:8000"
    ).strip()
    return base.rstrip("/") + "/"


def _abs_url(path: str) -> str:
    return urljoin(_site_base(), path.lstrip("/"))


def _format_ist(dt) -> str:
    try:
        return timezone.localtime(dt, IST).strftime("%d %b %Y, %I:%M %p")
    except Exception:
        return str(dt)


def _email_enabled() -> bool:
    try:
        return bool(getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        return True


def _render_pair(html_tpl: str, txt_tpl: str, context: Dict) -> Tuple[str, str]:
    html = get_template(html_tpl).render(context)
    txt = get_template(txt_tpl).render(context)
    return html, txt


def _send(subject: str, to_addr: str, cc: List[str], reply_to: List[str], html: str, txt: str) -> bool:
    """
    Send an email and log success/failure with full context.
    Returns True on success, False on any failure.
    """
    if not to_addr:
        logger.warning("Leave email suppressed: empty TO address. subject=%r cc=%s", subject, cc)
        return False

    from_email = getattr(settings, "LEAVE_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    fail_silently = getattr(settings, "EMAIL_FAIL_SILENTLY", True)
    backend_name = getattr(settings, "EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")

    # Log what we are about to do (masked password)
    try:
        host = getattr(settings, "EMAIL_HOST", None)
        port = getattr(settings, "EMAIL_PORT", None)
        user = getattr(settings, "EMAIL_HOST_USER", None)
        use_tls = getattr(settings, "EMAIL_USE_TLS", None)
        use_ssl = getattr(settings, "EMAIL_USE_SSL", None)
        logger.info(
            "Leave email attempt: backend=%s host=%s port=%s user=%s TLS=%s SSL=%s "
            "from=%s to=%s cc=%s reply_to=%s subject=%r fail_silently=%s",
            backend_name, host, port, user, use_tls, use_ssl, from_email, to_addr, cc, reply_to, subject, fail_silently
        )
    except Exception:
        pass

    try:
        # Use an explicit connection so we can log open/close events
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=txt,
                from_email=from_email,
                to=[to_addr],
                cc=cc or None,
                reply_to=reply_to or None,
                connection=conn,
            )
            msg.attach_alternative(html, "text/html")
            sent = msg.send(fail_silently=fail_silently)

        if sent:
            logger.info("Leave email sent OK: to=%s cc=%s subject=%r", to_addr, cc, subject)
            return True
        else:
            # Django returns the number of successfully delivered messages; 0 means failure
            logger.error("Leave email send returned 0: to=%s cc=%s subject=%r", to_addr, cc, subject)
            return False
    except Exception as exc:
        # If fail_silently=True, Django would suppress; we still log here.
        logger.exception("Leave email send FAILED: to=%s cc=%s subject=%r error=%s", to_addr, cc, subject, exc)
        return False


def _already_sent_recent(leave: LeaveRequest, kind_hint: str | None = None, within_seconds: int = 90) -> bool:
    """Light duplicate suppression using EMAIL_SENT audits."""
    try:
        since = timezone.now() - timezone.timedelta(seconds=within_seconds)
        qs = LeaveDecisionAudit.objects.filter(
            leave=leave, action=DecisionAction.EMAIL_SENT, decided_at__gte=since
        )
        if kind_hint:
            qs = qs.filter(extra__kind=kind_hint)
        return qs.exists()
    except Exception:
        return False


def _dedupe_lower(emails: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for e in emails or []:
        if not e:
            continue
        low = (e or "").strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        out.append(low)
    return out


@dataclass
class _TokenLinks:
    approve: Optional[str]
    reject: Optional[str]


def _build_token_links(leave: LeaveRequest, recipient_email: str) -> _TokenLinks:
    """Create one-click links bound to the recipient's address."""
    recipient_email = (recipient_email or "").strip().lower()
    if not recipient_email:
        return _TokenLinks(None, None)

    payload_base = {
        "leave_id": int(leave.id),
        "actor_email": recipient_email,
        "manager_email": recipient_email,  # legacy key for older handlers
    }

    approve_token = signing.dumps({**payload_base, "action": "approve"}, salt=TOKEN_SALT)
    reject_token = signing.dumps({**payload_base, "action": "reject"}, salt=TOKEN_SALT)

    approve_url = _abs_url(reverse("leave:leave_action_via_token", args=[approve_token])) + "?a=APPROVED"
    reject_url = _abs_url(reverse("leave:leave_action_via_token", args=[reject_token])) + "?a=REJECTED"
    return _TokenLinks(approve=approve_url, reject=reject_url)


def _duration_days_ist(leave: LeaveRequest) -> float:
    """Inclusive day count in IST, respecting half-day."""
    if not (leave.start_at and leave.end_at):
        return 0.0
    s = timezone.localtime(leave.start_at, IST).date()
    e = timezone.localtime(leave.end_at, IST).date()
    if e < s:
        s, e = e, s
    days = (e - s).days + 1
    if leave.is_half_day and days == 1:
        return 0.5
    return float(days)


def _employee_display_name(user) -> str:
    try:
        return (getattr(user, "get_full_name", lambda: "")() or user.username or "").strip()
    except Exception:
        return (getattr(user, "username", "") or "").strip()


def _manager_display_name(leave: LeaveRequest, manager_email: str) -> Optional[str]:
    em = (manager_email or "").strip().lower()
    if not em:
        return None
    try:
        rp = getattr(leave, "reporting_person", None)
        if rp and (rp.email or "").strip().lower() == em:
            full = (getattr(rp, "get_full_name", lambda: "")() or "").strip()
            return full or (getattr(rp, "username", "") or "").strip()
        u = User.objects.filter(email__iexact=em).only("first_name", "last_name", "username").first()
        if u:
            full = (getattr(u, "get_full_name", lambda: "")() or "").strip()
            return full or (u.username or "").strip()
    except Exception:
        pass
    return None


def _default_cc_emails_for_employee(emp: User) -> List[str]:
    """
    Collect default CC from ApproverMapping:
      - ApproverMapping.default_cc_users (M2M)
      - legacy ApproverMapping.cc_person.email if present
    Lowercased, deduped, falsy filtered.
    """
    out: List[str] = []
    try:
        mapping = (
            ApproverMapping.objects.select_related("cc_person")
            .prefetch_related("default_cc_users")
            .filter(employee=emp)
            .first()
        )
        if mapping:
            out.extend(
                [
                    u.email.strip().lower()
                    for u in mapping.default_cc_users.all()
                    if getattr(u, "email", None)
                ]
            )
            if getattr(mapping, "cc_person", None) and getattr(mapping.cc_person, "email", None):
                out.append(mapping.cc_person.email.strip().lower())
    except Exception:
        pass
    return _dedupe_lower(out)


def _resolve_recipients(
    leave: LeaveRequest,
    manager_email: Optional[str],
    cc_list: Optional[Iterable[str]],
) -> Tuple[str, List[str]]:
    """
    Return (to, cc) using explicit args > model snapshot > routing fallback.
    CCs are a merge of:
      • per-request leave.cc_users
      • ApproverMapping.default_cc_users for the employee
      • legacy ApproverMapping.cc_person.email
      • any explicit cc_list provided by caller
    All lowercased & deduped.
    """
    # Selected CCs by the employee (per-request)
    selected_ccs: List[str] = []
    try:
        selected_ccs = [
            (u.email or "").strip().lower() for u in leave.cc_users.all() if getattr(u, "email", None)
        ]
    except Exception:
        selected_ccs = []

    # Admin defaults from mapping (M2M + legacy single)
    defaults = _default_cc_emails_for_employee(leave.employee)

    # Explicit list from caller (if any)
    explicit = [(e or "").strip().lower() for e in (cc_list or []) if e]

    merged_cc = _dedupe_lower([*explicit, *selected_ccs, *defaults])

    if manager_email:
        return manager_email.strip().lower(), merged_cc

    # Prefer model snapshot
    if getattr(leave, "reporting_person", None) and getattr(leave.reporting_person, "email", None):
        to_addr = leave.reporting_person.email.strip().lower()
        # include legacy snapshot on the leave row as well (rare but safe)
        legacy_on_leave = []
        if getattr(leave, "cc_person", None) and getattr(leave, "cc_person").email:
            legacy_on_leave.append(leave.cc_person.email.strip().lower())
        cc = _dedupe_lower([*merged_cc, *legacy_on_leave])
        return to_addr, cc

    # Fallback: dynamic routing
    try:
        from apps.users.routing import recipients_for_leave
        emp_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip().lower()
        mapping = recipients_for_leave(emp_email) or {}
        to_addr = (mapping.get("to") or "").strip().lower()
        dynamic_cc = [e.strip().lower() for e in (mapping.get("cc") or []) if e]
        cc = _dedupe_lower([*merged_cc, *dynamic_cc])
        return to_addr, cc
    except Exception:
        logger.warning("Could not resolve recipients for leave id=%s", getattr(leave, "id", None))
        return "", merged_cc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def send_leave_request_email(
    leave: LeaveRequest,
    manager_email: Optional[str] = None,
    cc_list: Optional[Iterable[str]] = None,
    *,
    force: bool = False,
) -> None:
    """Send the initial leave request to the Reporting Person (TO) and CC (admin + selected)."""
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping request email for leave #%s.", leave.id)
        return

    # Light duplicate suppression (unless force=True)
    if not force and _already_sent_recent(leave, kind_hint="request"):
        logger.info("Suppressing duplicate request email for leave #%s (recent audit).", leave.id)
        return

    to_addr, cc = _resolve_recipients(leave, manager_email, cc_list)
    if not to_addr:
        logger.warning("Request email suppressed: no RP email for leave #%s.", leave.id)
        return

    tokens = _build_token_links(leave, to_addr)

    # Approval page (login) with hint
    approval_page_url = _abs_url(reverse("leave:approval_page", args=[leave.id]))
    approve_url = f"{approval_page_url}?a=APPROVED"
    reject_url = f"{approval_page_url}?a=REJECTED"

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    manager_name = _manager_display_name(leave, to_addr)

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Leave Request — {employee_name} ({_format_ist(leave.start_at)} to {_format_ist(leave.end_at)})"

    # Collect handover summary for email
    handover_summary = []
    try:
        handovers = LeaveHandover.objects.filter(leave_request=leave).select_related('new_assignee')
        for handover in handovers:
            task_title = handover.get_task_title()
            task_url = handover.get_task_url()
            if task_url:
                task_url = _abs_url(task_url)
            handover_summary.append({
                'task_type': handover.get_task_type_display(),
                'task_id': handover.original_task_id,
                'task_title': task_title,
                'task_url': task_url,
                'assignee_name': _employee_display_name(handover.new_assignee),
                'message': handover.message,
            })
    except Exception:
        pass

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "reason": leave.reason or "",
        "employee_name": employee_name,
        "employee_email": (leave.employee_email or getattr(leave.employee, "email", "") or "").strip(),
        "employee_designation": getattr(leave, "employee_designation", "") or "",
        "is_half_day": bool(getattr(leave, "is_half_day", False)),
        "duration_days": _duration_days_ist(leave),
        "manager_name": manager_name,
        "manager_email": to_addr,
        "cc_list": list(cc or []),
        # Handover summary
        "handover_summary": handover_summary,
        "has_handovers": len(handover_summary) > 0,
        # Buttons (approval page)
        "approve_url": approve_url,
        "reject_url": reject_url,
        "approval_page_url": approval_page_url,
        # One-click tokens for the RP (TO) only
        "token_approve_url": tokens.approve,
        "token_reject_url": tokens.reject,
    }

    html, txt = _render_pair("email/leave_applied.html", "email/leave_applied.txt", ctx)
    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(subject, to_addr, cc=list(cc or []), reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Leave request email NOT delivered for leave #%s (see earlier logs for details).", leave.id)


def send_leave_decision_email(leave: LeaveRequest) -> None:
    """Send the approve/reject decision email to the employee."""
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping decision email for leave #%s.", leave.id)
        return

    # Light suppression
    if _already_sent_recent(leave, kind_hint="decision"):
        logger.info("Suppressing duplicate decision email for leave #%s (recent audit).", leave.id)
        return

    to_addr: Optional[str] = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip()
    if not to_addr:
        logger.info("Decision email suppressed: employee has no email (leave #%s).", leave.id)
        return

    status_label = leave.get_status_display()
    approver_name = ""
    try:
        ap = getattr(leave, "approver", None) or getattr(leave, "reporting_person", None)
        if ap:
            approver_name = (getattr(ap, "get_full_name", lambda: "")() or ap.username or "").strip()
    except Exception:
        approver_name = ""

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "employee_name": leave.employee_name or _employee_display_name(leave.employee),
        "decided_at_ist": _format_ist(leave.decided_at or timezone.now()),
        "decision_comment": (leave.decision_comment or "").strip(),
        "status": status_label,
        "is_half_day": bool(getattr(leave, "is_half_day", False)),
        "duration_days": _duration_days_ist(leave),
        "reason": leave.reason or "",
        "approver_name": approver_name,
    }

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Leave {status_label} — #{leave.id}"

    html, txt = _render_pair("email/leave_decision.html", "email/leave_decision.txt", ctx)

    reply_to: List[str] = []
    try:
        if getattr(leave.approver, "email", ""):
            reply_to.append(leave.approver.email)
        else:
            # fallback so employee can reply to their manager/route
            from apps.users.routing import recipients_for_leave
            routing = recipients_for_leave(ctx["employee_name"])
            if routing.get("to"):
                reply_to.append(routing["to"])
    except Exception:
        pass

    ok = _send(subject, to_addr, cc=[], reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Leave decision email NOT delivered for leave #%s (see earlier logs for details).", leave.id)


def send_handover_email(leave: LeaveRequest, assignee, handovers: List) -> None:
    """Send handover notification to the delegate about assigned tasks."""
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping handover email for leave #%s.", leave.id)
        return

    to_addr = (assignee.email or "").strip()
    if not to_addr:
        logger.warning("Handover email suppressed: assignee %s has no email", assignee)
        return

    if not handovers:
        return

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    assignee_name = _employee_display_name(assignee)

    # Prepare handover details with task links
    handover_details = []
    for handover in handovers:
        task_title = handover.get_task_title()
        task_url = handover.get_task_url()
        if task_url:
            task_url = _abs_url(task_url)

        handover_details.append({
            'task_name': task_title,
            'task_type': handover.get_task_type_display(),
            'task_id': handover.original_task_id,
            'task_url': task_url,
            'message': handover.message,
        })

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Task Handover: {employee_name} ({_format_ist(leave.start_at)} - {_format_ist(leave.end_at)})"

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "duration_days": _duration_days_ist(leave),
        "is_half_day": bool(getattr(leave, "is_half_day", False)),
        "employee_name": employee_name,
        "employee_email": (leave.employee_email or getattr(leave.employee, "email", "") or "").strip(),
        "assignee_name": assignee_name,
        "handovers": handover_details,
        "handover_message": handovers[0].message if handovers else "",
    }

    html, txt = _render_pair("email/leave_handover.html", "email/leave_handover.txt", ctx)
    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(subject, to_addr, cc=[], reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Handover email NOT delivered for leave #%s (see earlier logs for details).", leave.id)


def send_delegation_reminder_email(reminder) -> None:
    """Send reminder email for delegated task."""
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping delegation reminder email.")
        return

    handover = reminder.leave_handover
    leave = handover.leave_request
    assignee = handover.new_assignee

    to_addr = (assignee.email or "").strip()
    if not to_addr:
        logger.warning("Reminder email suppressed: assignee %s has no email", assignee)
        return

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    assignee_name = _employee_display_name(assignee)
    task_title = handover.get_task_title()
    task_url = handover.get_task_url()
    if task_url:
        task_url = _abs_url(task_url)

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Reminder: {task_title} (delegated by {employee_name})"

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "task_title": task_title,
        "task_url": task_url,
        "task_type": handover.get_task_type_display(),
        "task_id": handover.original_task_id,
        "employee_name": employee_name,
        "employee_email": (leave.employee_email or getattr(leave.employee, "email", "") or "").strip(),
        "assignee_name": assignee_name,
        "original_message": handover.message,
        "interval_days": reminder.interval_days,
        "total_sent": reminder.total_sent,
        "effective_end_date": handover.effective_end_date,
    }

    html, txt = _render_pair("email/delegation_reminder.html", "email/delegation_reminder.txt", ctx)
    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(subject, to_addr, cc=[], reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Delegation reminder email NOT delivered (handover id=%s).", handover.id)
