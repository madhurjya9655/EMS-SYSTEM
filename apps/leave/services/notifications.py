# apps/leave/services/notifications.py
#
# FIX 2026-03-10 — Bug 1 (Critical):
#   _already_sent_recent() used `timezone.timedelta(...)` which raises
#   AttributeError at runtime because django.utils.timezone has no timedelta
#   attribute. The exception was silently swallowed by `except Exception:
#   return False`, so duplicate-suppression NEVER worked — leave emails
#   (request, decision, handover reminder) could fire repeatedly.
#
#   Fix: added `from datetime import timedelta` at the top of the file and
#   replaced the single occurrence of `timezone.timedelta(...)` with
#   `timedelta(...)`.  No other changes.

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta                       # ← FIX: import timedelta
from typing import Dict, List, Optional, Tuple, Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import signing
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import get_template
from django.urls import reverse
from django.utils import timezone

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


def _abs_url(path: str | None) -> str:
    if not path:
        return _site_base()
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
    try:
        html = get_template(html_tpl).render(context)
        txt = get_template(txt_tpl).render(context)
        return html, txt
    except Exception:
        kind = "notification"
        if "leave_type" in context and "approve_url" in context:
            kind = "leave request"
        elif "status" in context and "approver_name" in context:
            kind = "leave decision"
        elif "handovers" in context:
            kind = "handover"
        elif "task_type" in context and "task_name" in context:
            kind = "task completed"
        lines = [f"{kind.title()} from EMS"]
        for k, v in context.items():
            try:
                lines.append(f"- {k}: {v}")
            except Exception:
                continue
        txt = "\n".join(lines)
        html = "<br/>".join(lines)
        logger.warning("Template render failed for %s/%s; using inline fallback.", html_tpl, txt_tpl, exc_info=True)
        return html, txt


def _send(subject: str, to_addr: str, cc: List[str], reply_to: List[str], html: str, txt: str) -> bool:
    if not to_addr:
        logger.warning("Leave email suppressed: empty TO address. subject=%r cc=%s", subject, cc)
        return False

    from_email = getattr(settings, "LEAVE_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    fail_silently = getattr(settings, "EMAIL_FAIL_SILENTLY", True)
    backend_name = getattr(settings, "EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")

    try:
        host = getattr(settings, "EMAIL_HOST", None)
        port = getattr(settings, "EMAIL_PORT", None)
        user = getattr(settings, "EMAIL_HOST_USER", None)
        use_tls = getattr(settings, "EMAIL_USE_TLS", None)
        use_ssl = getattr(settings, "EMAIL_USE_SSL", None)
        logger.info(
            "Leave email attempt: backend=%s host=%s port=%s user=%s TLS=%s SSL=%s "
            "from=%s to=%s cc=%s reply_to=%s subject=%r fail_silently=%s",
            backend_name, host, port, user, use_tls, use_ssl,
            from_email, to_addr, cc, reply_to, subject, fail_silently,
        )
    except Exception:
        pass

    try:
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

        logger.error("Leave email send returned 0: to=%s cc=%s subject=%r", to_addr, cc, subject)
        return False
    except Exception as exc:
        logger.exception("Leave email send FAILED: to=%s cc=%s subject=%r error=%s", to_addr, cc, subject, exc)
        return False


def _already_sent_recent(leave: LeaveRequest, kind_hint: str | None = None, within_seconds: int = 90) -> bool:
    """Light duplicate suppression using EMAIL_SENT audits."""
    try:
        # FIX: was `timezone.timedelta(...)` — django.utils.timezone has no timedelta.
        # That AttributeError was silently swallowed, making this check always return
        # False and allowing duplicate emails to fire on every call.
        since = timezone.now() - timedelta(seconds=within_seconds)   # ← FIX
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
    recipient_email = (recipient_email or "").strip().lower()
    if not recipient_email:
        return _TokenLinks(None, None)

    payload_base = {
        "leave_id": int(leave.id),
        "actor_email": recipient_email,
        "manager_email": recipient_email,
    }
    approve_token = signing.dumps({**payload_base, "action": "approve"}, salt=TOKEN_SALT)
    reject_token = signing.dumps({**payload_base, "action": "reject"}, salt=TOKEN_SALT)
    approve_url = _abs_url(reverse("leave:leave_action_via_token", args=[approve_token])) + "?a=APPROVED"
    reject_url = _abs_url(reverse("leave:leave_action_via_token", args=[reject_token])) + "?a=REJECTED"
    return _TokenLinks(approve=approve_url, reject=reject_url)


def _duration_days_ist(leave: LeaveRequest) -> float:
    if not (leave.start_at and leave.end_at):
        return 0.0
    s = timezone.localtime(leave.start_at, IST).date()
    e = timezone.localtime(leave.end_at, IST).date()
    if e < s:
        s, e = e, s
    days = (e - s).days + 1
    if getattr(leave, "is_half_day", False) and days == 1:
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
    out: List[str] = []
    try:
        mapping = (
            ApproverMapping.objects.select_related("cc_person")
            .prefetch_related("default_cc_users")
            .filter(employee=emp)
            .first()
        )
        if mapping:
            out.extend([
                u.email.strip().lower()
                for u in mapping.default_cc_users.all()
                if getattr(u, "email", None)
            ])
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
    selected_ccs: List[str] = []
    try:
        selected_ccs = [
            (u.email or "").strip().lower() for u in leave.cc_users.all() if getattr(u, "email", None)
        ]
    except Exception:
        selected_ccs = []

    defaults = _default_cc_emails_for_employee(leave.employee)
    explicit = [(e or "").strip().lower() for e in (cc_list or []) if e]
    merged_cc = _dedupe_lower([*explicit, *selected_ccs, *defaults])

    if manager_email:
        return manager_email.strip().lower(), merged_cc

    if getattr(leave, "reporting_person", None) and getattr(leave.reporting_person, "email", None):
        to_addr = leave.reporting_person.email.strip().lower()
        legacy_on_leave = []
        if getattr(leave, "cc_person", None) and getattr(leave, "cc_person").email:
            legacy_on_leave.append(leave.cc_person.email.strip().lower())
        cc = _dedupe_lower([*merged_cc, *legacy_on_leave])
        return to_addr, cc

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
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping request email for leave #%s.", leave.id)
        return

    if not force and _already_sent_recent(leave, kind_hint="request"):
        logger.info("Suppressing duplicate request email for leave #%s (recent audit).", leave.id)
        return

    to_addr, cc = _resolve_recipients(leave, manager_email, cc_list)
    if not to_addr:
        logger.warning("Request email suppressed: no RP email for leave #%s.", leave.id)
        return

    tokens = _build_token_links(leave, to_addr)
    approval_page_url = _abs_url(reverse("leave:approval_page", args=[leave.id]))
    approve_url = f"{approval_page_url}?a=APPROVED"
    reject_url = f"{approval_page_url}?a=REJECTED"

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    manager_name = _manager_display_name(leave, to_addr)
    leave_type_name = getattr(leave.leave_type, "name", str(leave.leave_type))
    subject = f"Leave Request - {employee_name} ({leave_type_name})"

    handover_summary = []
    try:
        handovers = LeaveHandover.objects.filter(leave_request=leave).select_related("new_assignee")
        for handover in handovers:
            task_title = handover.get_task_title()
            task_url = _abs_url(handover.get_task_url())
            handover_summary.append({
                "task_type": handover.get_task_type_display(),
                "task_id": handover.original_task_id,
                "task_title": task_title,
                "task_url": task_url,
                "assignee_name": _employee_display_name(handover.new_assignee),
                "message": handover.message,
            })
    except Exception:
        pass

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": leave_type_name,
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
        "handover_summary": handover_summary,
        "has_handovers": len(handover_summary) > 0,
        "approve_url": approve_url,
        "reject_url": reject_url,
        "approval_page_url": approval_page_url,
        "token_approve_url": tokens.approve,
        "token_reject_url": tokens.reject,
    }

    html, txt = _render_pair("email/leave_applied.html", "email/leave_applied.txt", ctx)
    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(subject, to_addr, cc=list(cc or []), reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Leave request email NOT delivered for leave #%s (see earlier logs for details).", leave.id)
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT, extra={"kind": "request"})
    except Exception:
        logger.exception("Failed to log EMAIL_SENT (request) for leave #%s", leave.id)


def send_leave_decision_email(leave: LeaveRequest) -> None:
    if not _email_enabled():
        logger.info("Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping decision email for leave #%s.", leave.id)
        return

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
            from apps.users.routing import recipients_for_leave
            emp_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip()
            routing = recipients_for_leave(emp_email)
            if routing.get("to"):
                reply_to.append(routing["to"])
    except Exception:
        pass

    ok = _send(subject, to_addr, cc=[], reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Leave decision email NOT delivered for leave #%s", leave.id)
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT, extra={"kind": "decision"})
    except Exception:
        logger.exception("Failed to log EMAIL_SENT (decision) for leave #%s", leave.id)


def send_handover_email(leave: LeaveRequest, assignee, handovers: List) -> None:
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

    handover_details = []
    for handover in handovers:
        task_title = handover.get_task_title()
        task_url = _abs_url(handover.get_task_url())
        handover_details.append({
            "task_name": task_title,
            "task_type": handover.get_task_type_display(),
            "task_id": handover.original_task_id,
            "task_url": task_url,
            "message": handover.message,
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
        logger.error("Handover email NOT delivered for leave #%s", leave.id)
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.HANDOVER_EMAIL_SENT,
                extra={"assignee_id": getattr(assignee, "id", None)},
            )
    except Exception:
        logger.exception("Failed to log HANDOVER_EMAIL_SENT for leave #%s", leave.id)


def send_delegation_reminder_email(reminder) -> None:
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
    task_url = _abs_url(handover.get_task_url())

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
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={"kind": "handover_reminder", "handover_id": handover.id},
            )
    except Exception:
        logger.exception("Failed to log EMAIL_SENT (handover_reminder) for leave #%s", leave.id)


# ---------------------------------------------------------------------------
# Task completion notifications
# ---------------------------------------------------------------------------
def _task_type_and_url(task) -> Tuple[str, Optional[str]]:
    task_type = "unknown"
    url_path = None
    try:
        from apps.tasks.models import Checklist, Delegation, HelpTicket
        if isinstance(task, Checklist):
            task_type = "Checklist"
            url_path = reverse("tasks:checklist_detail", args=[task.id])
        elif isinstance(task, Delegation):
            task_type = "Delegation"
            url_path = reverse("tasks:delegation_detail", args=[task.id])
        elif isinstance(task, HelpTicket):
            task_type = "Help Ticket"
            url_path = reverse("tasks:help_ticket_details", args=[task.id])
    except Exception:
        pass
    return task_type, _abs_url(url_path) if url_path else None


def _find_related_leave(task) -> Optional[LeaveRequest]:
    try:
        from apps.tasks.models import Checklist, Delegation, HelpTicket
        if isinstance(task, Checklist):
            tname = "checklist"
        elif isinstance(task, Delegation):
            tname = "delegation"
        elif isinstance(task, HelpTicket):
            tname = "help_ticket"
        else:
            return None
        ho = (
            LeaveHandover.objects.filter(task_type=tname, original_task_id=task.id)
            .select_related("leave_request")
            .order_by("-id")
            .first()
        )
        return ho.leave_request if ho else None
    except Exception:
        return None


def send_task_completion_email(original_assignee: User, delegate: User, task, context: Dict) -> None:
    if not _email_enabled():
        logger.info("Email disabled; skipping task completion email.")
        return

    to_addr = (getattr(original_assignee, "email", "") or "").strip()
    if not to_addr:
        logger.info("Task completion email suppressed: original assignee has no email.")
        return

    task_type, task_url = _task_type_and_url(task)
    task_name = getattr(task, "task_name", None) or getattr(task, "title", f"{task_type} #{getattr(task, 'id', '')}")
    completed_at = context.get("completed_at") or timezone.now()
    planned_date = context.get("planned_date")
    leave = _find_related_leave(task)

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = (
        f"{subject_prefix}{task_type} Completed by "
        f"{getattr(delegate, 'get_full_name', lambda: '')() or delegate.username}: {task_name}"
    )

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "task_type": task_type,
        "task_id": getattr(task, "id", None),
        "task_name": task_name,
        "task_url": task_url,
        "delegate_name": (getattr(delegate, "get_full_name", lambda: "")() or delegate.username),
        "delegate_email": (getattr(delegate, "email", "") or "").strip(),
        "original_assignee_name": (getattr(original_assignee, "get_full_name", lambda: "")() or original_assignee.username),
        "planned_date_ist": _format_ist(planned_date) if planned_date else None,
        "completed_at_ist": _format_ist(completed_at),
        "leave_window": {
            "exists": bool(leave),
            "start_at_ist": _format_ist(leave.start_at) if leave else None,
            "end_at_ist": _format_ist(leave.end_at) if leave else None,
            "employee_name": getattr(leave, "employee_name", "") if leave else None,
        },
    }

    html, txt = _render_pair("email/task_completed.html", "email/task_completed.txt", ctx)
    reply_to = [ctx["delegate_email"]] if ctx["delegate_email"] else []

    ok = _send(subject, to_addr, cc=[], reply_to=reply_to, html=html, txt=txt)
    if not ok:
        logger.error("Task completion email NOT delivered: task=%s to=%s", getattr(task, "id", None), to_addr)
        return

    try:
        if leave and LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={"kind": "task_completed", "task_type": task_type, "task_id": getattr(task, "id", None)},
            )
    except Exception:
        logger.exception("Failed to log EMAIL_SENT (task_completed) for leave #%s", getattr(leave, "id", None))


def send_handover_completion_email(handover: LeaveHandover) -> None:
    try:
        if not _email_enabled():
            return
        task = handover.get_task_object()
        if not task:
            logger.info("Completion email skipped: task not found for handover id=%s", getattr(handover, "id", None))
            return
        original = handover.original_assignee
        delegate = handover.new_assignee
        context = {
            "completed_at": timezone.now(),
            "planned_date": getattr(task, "planned_date", None),
        }
        send_task_completion_email(original, delegate, task, context)
    except Exception:
        logger.exception("Failed in send_handover_completion_email for handover id=%s", getattr(handover, "id", None))


__all__ = [
    "send_leave_request_email",
    "send_leave_decision_email",
    "send_handover_email",
    "send_delegation_reminder_email",
    "send_task_completion_email",
    "send_handover_completion_email",
]