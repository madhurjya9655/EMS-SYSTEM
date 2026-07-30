# apps/leave/services/notifications.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import signing
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import get_template
from django.urls import reverse
from django.utils import timezone
from django.utils.html import escape

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
        elif "available_leave_after_application" in context:
            kind = "leave application confirmation"
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

        logger.warning(
            "Template render failed for %s/%s; using inline fallback.",
            html_tpl,
            txt_tpl,
            exc_info=True,
        )

        return html, txt


def resolve_leave_recipients(
    primary_to: str,
    existing_cc: Optional[Iterable[str]] = None,
    reply_to_emails: Optional[Iterable[str]] = None,
) -> Tuple[List[str], List[str]]:
    """Return clean TO and CC lists without any global recipients.

    Business rule:
    - TO is the employee's reporting officer.
    - CC contains only the employee's admin-managed Default CC users.
    - Duplicate addresses are removed.
    - An address already present in TO or Reply-To is removed from CC.
    """
    final_to = _dedupe_lower([primary_to])

    blocked_from_cc = {
        (email or "").strip().lower()
        for email in [*final_to, *(reply_to_emails or [])]
        if (email or "").strip()
    }

    final_cc: List[str] = []
    seen = set()

    for email in existing_cc or []:
        normalized = (email or "").strip().lower()
        if not normalized or normalized in seen or normalized in blocked_from_cc:
            continue
        seen.add(normalized)
        final_cc.append(normalized)

    return final_to, final_cc

def _send(
    subject: str,
    to_addr: str,
    cc: List[str],
    reply_to: List[str],
    html: str,
    txt: str,
) -> bool:
    if not to_addr:
        logger.warning(
            "Leave email suppressed: empty TO address. subject=%r cc=%s",
            subject,
            cc,
        )
        return False

    to_list, cc = resolve_leave_recipients(
        primary_to=to_addr,
        existing_cc=cc,
        reply_to_emails=reply_to,
    )

    from_email = (
        getattr(settings, "LEAVE_EMAIL_FROM", None)
        or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    )

    fail_silently = getattr(settings, "EMAIL_FAIL_SILENTLY", True)
    backend_name = getattr(
        settings,
        "EMAIL_BACKEND",
        "django.core.mail.backends.smtp.EmailBackend",
    )

    try:
        host = getattr(settings, "EMAIL_HOST", None)
        port = getattr(settings, "EMAIL_PORT", None)
        user = getattr(settings, "EMAIL_HOST_USER", None)
        use_tls = getattr(settings, "EMAIL_USE_TLS", None)
        use_ssl = getattr(settings, "EMAIL_USE_SSL", None)

        logger.info(
            "Leave email attempt: backend=%s host=%s port=%s user=%s TLS=%s SSL=%s "
            "from=%s to=%s cc=%s reply_to=%s subject=%r fail_silently=%s",
            backend_name,
            host,
            port,
            user,
            use_tls,
            use_ssl,
            from_email,
            to_addr,
            cc,
            reply_to,
            subject,
            fail_silently,
        )
    except Exception:
        pass

    try:
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=txt,
                from_email=from_email,
                to=to_list,
                cc=cc or None,
                reply_to=reply_to or None,
                connection=conn,
            )

            msg.attach_alternative(html, "text/html")

            sent = msg.send(fail_silently=fail_silently)

        if sent:
            logger.info(
                "Leave email sent OK: to=%s cc=%s subject=%r",
                to_addr,
                cc,
                subject,
            )
            return True

        logger.error(
            "Leave email send returned 0: to=%s cc=%s subject=%r",
            to_addr,
            cc,
            subject,
        )
        return False

    except Exception as exc:
        logger.exception(
            "Leave email send FAILED: to=%s cc=%s subject=%r error=%s",
            to_addr,
            cc,
            subject,
            exc,
        )
        return False


def _already_sent_recent(
    leave: LeaveRequest,
    kind_hint: str | None = None,
    within_seconds: int = 90,
) -> bool:
    """
    Light duplicate suppression using EMAIL_SENT audits.
    """
    try:
        since = timezone.now() - timedelta(seconds=within_seconds)

        qs = LeaveDecisionAudit.objects.filter(
            leave=leave,
            action=DecisionAction.EMAIL_SENT,
            decided_at__gte=since,
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

    approve_token = signing.dumps(
        {**payload_base, "action": "approve"},
        salt=TOKEN_SALT,
    )

    reject_token = signing.dumps(
        {**payload_base, "action": "reject"},
        salt=TOKEN_SALT,
    )

    approve_url = (
        _abs_url(reverse("leave:leave_action_via_token", args=[approve_token]))
        + "?a=APPROVED"
    )

    reject_url = (
        _abs_url(reverse("leave:leave_action_via_token", args=[reject_token]))
        + "?a=REJECTED"
    )

    return _TokenLinks(
        approve=approve_url,
        reject=reject_url,
    )


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
        return (
            getattr(user, "get_full_name", lambda: "")()
            or user.username
            or ""
        ).strip()
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

        u = (
            User.objects.filter(email__iexact=em)
            .only("first_name", "last_name", "username")
            .first()
        )

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
    manager_email: Optional[str] = None,
    cc_list: Optional[Iterable[str]] = None,
) -> Tuple[str, List[str]]:
    """Resolve recipients only from the employee's ApproverMapping.

    The Employee page writes the reporting officer and Default CC users into
    ApproverMapping. That mapping is the single source of truth. Legacy values
    stored on an old LeaveRequest are used only when no mapping exists.
    """
    mapping_rp = None
    mapping_cc_users: List[User] = []

    try:
        mapping_rp, mapping_cc_users = ApproverMapping.resolve_multi_for(leave.employee)
    except Exception:
        logger.exception(
            "Could not resolve ApproverMapping for leave id=%s",
            getattr(leave, "id", None),
        )

    to_addr = ""
    if mapping_rp and getattr(mapping_rp, "email", None):
        to_addr = mapping_rp.email.strip().lower()
    elif manager_email:
        to_addr = manager_email.strip().lower()
    elif getattr(leave, "reporting_person", None) and getattr(leave.reporting_person, "email", None):
        to_addr = leave.reporting_person.email.strip().lower()

    mapped_cc = [
        (user.email or "").strip().lower()
        for user in mapping_cc_users
        if getattr(user, "email", None)
    ]

    # Existing cc_list is only a compatibility fallback when no Default CC is configured.
    fallback_cc = [
        (email or "").strip().lower()
        for email in (cc_list or [])
        if email
    ]

    cc = _dedupe_lower(mapped_cc if mapped_cc else fallback_cc)
    cc = [email for email in cc if email != to_addr]

    return to_addr, cc



def _build_leave_cc_fyi_message(
    *,
    leave: LeaveRequest,
    employee_name: str,
    leave_type_name: str,
    handover_summary: List[Dict],
) -> Tuple[str, str]:
    """
    Build the information-only message sent to Leave CC recipients.

    Security rule:
    - No approval URL.
    - No rejection URL.
    - No signed token.
    - No manager action page link.

    CC recipients are observers only.
    """
    employee_email = (
        leave.employee_email
        or getattr(leave.employee, "email", "")
        or ""
    ).strip()

    start_at_ist = _format_ist(leave.start_at)
    end_at_ist = _format_ist(leave.end_at)
    duration_days = _duration_days_ist(leave)
    reason = leave.reason or ""
    half_day_label = "Yes" if bool(getattr(leave, "is_half_day", False)) else "No"

    text_lines = [
        "Leave Request — Information Only",
        "",
        "You are receiving this email as a CC recipient.",
        "Only the assigned Reporting Person in the TO field can approve or reject this leave.",
        "",
        f"Leave ID: {leave.id}",
        f"Employee: {employee_name}",
        f"Employee Email: {employee_email or '-'}",
        f"Leave Type: {leave_type_name}",
        f"From (IST): {start_at_ist}",
        f"To (IST): {end_at_ist}",
        f"Duration: {duration_days:g} day(s)",
        f"Half Day: {half_day_label}",
        f"Reason: {reason or '-'}",
    ]

    if handover_summary:
        text_lines.extend(["", "Task Handovers:"])

        for item in handover_summary:
            text_lines.append(
                "- {task_type} #{task_id}: {task_title} -> {assignee_name}".format(
                    task_type=item.get("task_type") or "Task",
                    task_id=item.get("task_id") or "-",
                    task_title=item.get("task_title") or "-",
                    assignee_name=item.get("assignee_name") or "-",
                )
            )

    text_lines.extend(
        [
            "",
            "No action is required from you.",
            "Approve and Reject actions are available only to the assigned Reporting Person.",
        ]
    )

    txt = "\n".join(text_lines)

    handover_html = ""

    if handover_summary:
        rows = []

        for item in handover_summary:
            rows.append(
                "<li>"
                f"<strong>{escape(str(item.get('task_type') or 'Task'))} "
                f"#{escape(str(item.get('task_id') or '-'))}</strong>: "
                f"{escape(str(item.get('task_title') or '-'))} "
                f"&rarr; {escape(str(item.get('assignee_name') or '-'))}"
                "</li>"
            )

        handover_html = (
            "<h3 style=\"margin:24px 0 8px;font-size:16px;\">Task Handovers</h3>"
            "<ul style=\"margin:0;padding-left:20px;\">"
            + "".join(rows)
            + "</ul>"
        )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Leave Request — Information Only</title>
</head>
<body style="margin:0;padding:24px;background:#f5f7fb;font-family:Arial,sans-serif;color:#1f2937;">
  <div style="max-width:680px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
    <div style="padding:20px 24px;background:#eff6ff;border-bottom:1px solid #bfdbfe;">
      <h1 style="margin:0;font-size:20px;color:#1d4ed8;">Leave Request — Information Only</h1>
    </div>

    <div style="padding:24px;">
      <div style="padding:12px 14px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;color:#92400e;margin-bottom:20px;">
        You are receiving this email as a CC recipient. Only the assigned Reporting Person
        in the TO field can approve or reject this leave.
      </div>

      <table role="presentation" style="width:100%;border-collapse:collapse;">
        <tr><td style="padding:7px 0;font-weight:bold;width:170px;">Leave ID</td><td style="padding:7px 0;">#{escape(str(leave.id))}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">Employee</td><td style="padding:7px 0;">{escape(employee_name)}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">Employee Email</td><td style="padding:7px 0;">{escape(employee_email or '-')}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">Leave Type</td><td style="padding:7px 0;">{escape(leave_type_name)}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">From (IST)</td><td style="padding:7px 0;">{escape(start_at_ist)}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">To (IST)</td><td style="padding:7px 0;">{escape(end_at_ist)}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">Duration</td><td style="padding:7px 0;">{duration_days:g} day(s)</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;">Half Day</td><td style="padding:7px 0;">{half_day_label}</td></tr>
        <tr><td style="padding:7px 0;font-weight:bold;vertical-align:top;">Reason</td><td style="padding:7px 0;">{escape(reason or '-')}</td></tr>
      </table>

      {handover_html}

      <div style="margin-top:24px;padding:12px 14px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;color:#4b5563;">
        No action is required from you. This message intentionally contains no Approve or Reject links.
      </div>
    </div>
  </div>
</body>
</html>"""

    return html, txt




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
    """
    Send one Leave request message with the Reporting Person in TO and all configured recipients in CC.

    TO / Reporting Person:
    - Receives the actionable Leave request email.
    - Receives Approve and Reject links.
    - Is the only person allowed to decide the Leave.

    CC recipients receive the same single message through the CC header.
    Backend authorization remains the security boundary: only the assigned
    LeaveRequest.reporting_person may approve or reject.
    """
    if not _email_enabled():
        logger.info(
            "Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping request email for leave #%s.",
            leave.id,
        )
        return

    if not force and _already_sent_recent(leave, kind_hint="request"):
        logger.info(
            "Suppressing duplicate request email for leave #%s (recent audit).",
            leave.id,
        )
        return

    to_addr, cc = _resolve_recipients(
        leave,
        manager_email,
        cc_list,
    )

    if not to_addr:
        logger.warning(
            "Request email suppressed: no RP email for leave #%s.",
            leave.id,
        )
        return

    # Signed tokens are generated only for the TO / Reporting Person.
    tokens = _build_token_links(leave, to_addr)

    approval_page_url = _abs_url(
        reverse("leave:approval_page", args=[leave.id])
    )
    approve_url = f"{approval_page_url}?a=APPROVED"
    reject_url = f"{approval_page_url}?a=REJECTED"

    employee_name = (
        leave.employee_name
        or _employee_display_name(leave.employee)
    )
    manager_name = _manager_display_name(leave, to_addr)
    leave_type_name = getattr(
        leave.leave_type,
        "name",
        str(leave.leave_type),
    )

    subject = (
        f"Leave Request - {employee_name} ({leave_type_name})"
    )

    handover_summary: List[Dict] = []

    try:
        handovers = (
            LeaveHandover.objects
            .filter(leave_request=leave)
            .select_related("new_assignee")
        )

        for handover in handovers:
            task_title = handover.get_task_title()
            task_url = _abs_url(handover.get_task_url())

            handover_summary.append(
                {
                    "task_type": handover.get_task_type_display(),
                    "task_id": handover.original_task_id,
                    "task_title": task_title,
                    "task_url": task_url,
                    "assignee_name": _employee_display_name(
                        handover.new_assignee
                    ),
                    "message": handover.message,
                }
            )
    except Exception:
        logger.exception(
            "Could not build handover summary for leave #%s.",
            leave.id,
        )

    employee_email = (
        leave.employee_email
        or getattr(leave.employee, "email", "")
        or ""
    ).strip()

    manager_ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": leave_type_name,
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "reason": leave.reason or "",
        "employee_name": employee_name,
        "employee_email": employee_email,
        "employee_designation": (
            getattr(leave, "employee_designation", "")
            or ""
        ),
        "is_half_day": bool(
            getattr(leave, "is_half_day", False)
        ),
        "duration_days": _duration_days_ist(leave),
        "manager_name": manager_name,
        "manager_email": to_addr,

        "cc_list": cc,

        "handover_summary": handover_summary,
        "has_handovers": bool(handover_summary),
        "approve_url": approve_url,
        "reject_url": reject_url,
        "approval_page_url": approval_page_url,
        "token_approve_url": tokens.approve,
        "token_reject_url": tokens.reject,
        "is_actionable_recipient": True,
        "is_cc_information_only": False,
    }

    manager_html, manager_txt = _render_pair(
        "email/leave_applied.html",
        "email/leave_applied.txt",
        manager_ctx,
    )

    reply_to = [employee_email] if employee_email else []

    # One SMTP message only: Reporting Person stays in TO and every configured
    # recipient is placed in the same CC header.  No per-CC loop is used.
    manager_ok = _send(
        subject,
        to_addr,
        cc=cc,
        reply_to=reply_to,
        html=manager_html,
        txt=manager_txt,
    )

    if not manager_ok:
        logger.error(
            "Leave request email NOT delivered to TO=%s CC=%s for leave #%s.",
            to_addr,
            cc,
            leave.id,
        )
        return

    logger.info(
        "Leave request delivery complete for leave #%s: to=%s cc=%s one_message=True",
        leave.id,
        to_addr,
        cc,
    )

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={
                    "kind": "request",
                    "actionable_to": to_addr,
                    "cc": cc,
                    "message_count": 1,
                },
            )
    except Exception:
        logger.exception(
            "Failed to log EMAIL_SENT (request) for leave #%s",
            leave.id,
        )


def send_leave_applied_confirmation_email(
    leave: LeaveRequest,
    *,
    force: bool = False,
    override_to: Optional[str] = None,
) -> None:
    """
    Send confirmation email to the employee after leave application.

    Shows available leave balance after application.

    Current production leave rule:
    - Pending + Approved leave reduces paid balance immediately.
    - Half-day leave reduces paid balance by 0.5 day.
    - Rejected and Cancelled leave does not reduce paid balance.
    """
    if not _email_enabled():
        logger.info(
            "Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping employee confirmation for leave #%s.",
            leave.id,
        )
        return

    if not force and _already_sent_recent(leave, kind_hint="employee_confirmation"):
        logger.info(
            "Suppressing duplicate employee confirmation email for leave #%s.",
            leave.id,
        )
        return

    to_addr = (
        override_to
        or leave.employee_email
        or getattr(leave.employee, "email", "")
        or ""
    ).strip()

    if not to_addr:
        logger.info(
            "Employee confirmation email suppressed: employee has no email (leave #%s).",
            leave.id,
        )
        return

    try:
        from apps.leave.utils import get_employee_leave_balance_summary

        balance = get_employee_leave_balance_summary(
            leave.employee,
            getattr(leave, "start_date", None),
        )
    except Exception:
        logger.exception(
            "Could not calculate leave balance for employee confirmation email leave #%s.",
            leave.id,
        )
        balance = None

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    leave_type_name = getattr(leave.leave_type, "name", str(leave.leave_type))

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": leave_type_name,
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "reason": leave.reason or "",
        "employee_name": employee_name,
        "employee_email": (
            leave.employee_email
            or getattr(leave.employee, "email", "")
            or ""
        ).strip(),
        "status": leave.get_status_display(),
        "is_half_day": bool(getattr(leave, "is_half_day", False)),
        "duration_days": _duration_days_ist(leave),
        "total_paid_leaves": getattr(balance, "total_paid_leaves", ""),
        "paid_leaves_taken": getattr(balance, "paid_leaves_taken", ""),
        "available_leave_after_application": getattr(balance, "remaining_paid_leaves", ""),
        "unpaid_leaves": getattr(balance, "unpaid_leaves", ""),
        "carry_forward_adjustment": getattr(balance, "carry_forward_adjustment", ""),
    }

    available = ctx["available_leave_after_application"]

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Leave Submitted - Available Leave {available}"

    html, txt = _render_pair(
        "email/leave_applied_employee.html",
        "email/leave_applied_employee.txt",
        ctx,
    )

    reply_to = []

    try:
        if getattr(leave.reporting_person, "email", ""):
            reply_to.append(leave.reporting_person.email)
    except Exception:
        pass

    ok = _send(
        subject,
        to_addr,
        cc=[],
        reply_to=reply_to,
        html=html,
        txt=txt,
    )

    if not ok:
        logger.error(
            "Employee leave confirmation email NOT delivered for leave #%s.",
            leave.id,
        )
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={"kind": "employee_confirmation"},
            )
    except Exception:
        logger.exception(
            "Failed to log EMAIL_SENT (employee_confirmation) for leave #%s",
            leave.id,
        )


def send_leave_decision_email(leave: LeaveRequest) -> None:
    if not _email_enabled():
        logger.info(
            "Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping decision email for leave #%s.",
            leave.id,
        )
        return

    if _already_sent_recent(leave, kind_hint="decision"):
        logger.info(
            "Suppressing duplicate decision email for leave #%s (recent audit).",
            leave.id,
        )
        return

    to_addr: Optional[str] = (
        leave.employee_email
        or getattr(leave.employee, "email", "")
        or ""
    ).strip()

    if not to_addr:
        logger.info(
            "Decision email suppressed: employee has no email (leave #%s).",
            leave.id,
        )
        return

    status_label = leave.get_status_display()

    approver_name = ""

    try:
        ap = getattr(leave, "approver", None) or getattr(leave, "reporting_person", None)

        if ap:
            approver_name = (
                getattr(ap, "get_full_name", lambda: "")()
                or ap.username
                or ""
            ).strip()
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

    html, txt = _render_pair(
        "email/leave_decision.html",
        "email/leave_decision.txt",
        ctx,
    )

    reply_to: List[str] = []

    try:
        if getattr(leave.approver, "email", ""):
            reply_to.append(leave.approver.email)
        else:
            reporting_person, _cc_users = ApproverMapping.resolve_multi_for(leave.employee)
            if reporting_person and getattr(reporting_person, "email", None):
                reply_to.append(reporting_person.email)
    except Exception:
        pass

    ok = _send(
        subject,
        to_addr,
        cc=[],
        reply_to=reply_to,
        html=html,
        txt=txt,
    )

    if not ok:
        logger.error(
            "Leave decision email NOT delivered for leave #%s",
            leave.id,
        )
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={"kind": "decision"},
            )
    except Exception:
        logger.exception(
            "Failed to log EMAIL_SENT (decision) for leave #%s",
            leave.id,
        )


def send_handover_email(
    leave: LeaveRequest,
    assignee,
    handovers: List,
) -> None:
    if not _email_enabled():
        logger.info(
            "Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping handover email for leave #%s.",
            leave.id,
        )
        return

    to_addr = (assignee.email or "").strip()

    if not to_addr:
        logger.warning(
            "Handover email suppressed: assignee %s has no email",
            assignee,
        )
        return

    if not handovers:
        return

    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    assignee_name = _employee_display_name(assignee)

    handover_details = []

    for handover in handovers:
        task_title = handover.get_task_title()
        task_url = _abs_url(handover.get_task_url())

        handover_details.append(
            {
                "task_name": task_title,
                "task_type": handover.get_task_type_display(),
                "task_id": handover.original_task_id,
                "task_url": task_url,
                "message": handover.message,
            }
        )

    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = (
        f"{subject_prefix}Task Handover: {employee_name} "
        f"({_format_ist(leave.start_at)} - {_format_ist(leave.end_at)})"
    )

    ctx = {
        "site_url": _site_base().rstrip("/"),
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _format_ist(leave.start_at),
        "end_at_ist": _format_ist(leave.end_at),
        "duration_days": _duration_days_ist(leave),
        "is_half_day": bool(getattr(leave, "is_half_day", False)),
        "employee_name": employee_name,
        "employee_email": (
            leave.employee_email
            or getattr(leave.employee, "email", "")
            or ""
        ).strip(),
        "assignee_name": assignee_name,
        "handovers": handover_details,
        "handover_message": handovers[0].message if handovers else "",
    }

    html, txt = _render_pair(
        "email/leave_handover.html",
        "email/leave_handover.txt",
        ctx,
    )

    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(
        subject,
        to_addr,
        cc=[],
        reply_to=reply_to,
        html=html,
        txt=txt,
    )

    if not ok:
        logger.error(
            "Handover email NOT delivered for leave #%s",
            leave.id,
        )
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.HANDOVER_EMAIL_SENT,
                extra={"assignee_id": getattr(assignee, "id", None)},
            )
    except Exception:
        logger.exception(
            "Failed to log HANDOVER_EMAIL_SENT for leave #%s",
            leave.id,
        )


def send_delegation_reminder_email(reminder) -> None:
    if not _email_enabled():
        logger.info(
            "Email disabled via FEATURES.EMAIL_NOTIFICATIONS; skipping delegation reminder email."
        )
        return

    handover = reminder.leave_handover
    leave = handover.leave_request
    assignee = handover.new_assignee

    to_addr = (assignee.email or "").strip()

    if not to_addr:
        logger.warning(
            "Reminder email suppressed: assignee %s has no email",
            assignee,
        )
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
        "employee_email": (
            leave.employee_email
            or getattr(leave.employee, "email", "")
            or ""
        ).strip(),
        "assignee_name": assignee_name,
        "original_message": handover.message,
        "interval_days": reminder.interval_days,
        "total_sent": reminder.total_sent,
        "effective_end_date": handover.effective_end_date,
    }

    html, txt = _render_pair(
        "email/delegation_reminder.html",
        "email/delegation_reminder.txt",
        ctx,
    )

    reply_to = [e for e in [ctx["employee_email"]] if e]

    ok = _send(
        subject,
        to_addr,
        cc=[],
        reply_to=reply_to,
        html=html,
        txt=txt,
    )

    if not ok:
        logger.error(
            "Delegation reminder email NOT delivered (handover id=%s).",
            handover.id,
        )
        return

    try:
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={
                    "kind": "handover_reminder",
                    "handover_id": handover.id,
                },
            )
    except Exception:
        logger.exception(
            "Failed to log EMAIL_SENT (handover_reminder) for leave #%s",
            leave.id,
        )


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
            LeaveHandover.objects.filter(
                task_type=tname,
                original_task_id=task.id,
            )
            .select_related("leave_request")
            .order_by("-id")
            .first()
        )

        return ho.leave_request if ho else None

    except Exception:
        return None


def send_task_completion_email(
    original_assignee: User,
    delegate: User,
    task,
    context: Dict,
) -> None:
    if not _email_enabled():
        logger.info("Email disabled; skipping task completion email.")
        return

    to_addr = (getattr(original_assignee, "email", "") or "").strip()

    if not to_addr:
        logger.info(
            "Task completion email suppressed: original assignee has no email."
        )
        return

    task_type, task_url = _task_type_and_url(task)

    task_name = (
        getattr(task, "task_name", None)
        or getattr(task, "title", f"{task_type} #{getattr(task, 'id', '')}")
    )

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
        "delegate_name": (
            getattr(delegate, "get_full_name", lambda: "")()
            or delegate.username
        ),
        "delegate_email": (getattr(delegate, "email", "") or "").strip(),
        "original_assignee_name": (
            getattr(original_assignee, "get_full_name", lambda: "")()
            or original_assignee.username
        ),
        "planned_date_ist": _format_ist(planned_date) if planned_date else None,
        "completed_at_ist": _format_ist(completed_at),
        "leave_window": {
            "exists": bool(leave),
            "start_at_ist": _format_ist(leave.start_at) if leave else None,
            "end_at_ist": _format_ist(leave.end_at) if leave else None,
            "employee_name": getattr(leave, "employee_name", "") if leave else None,
        },
    }

    html, txt = _render_pair(
        "email/task_completed.html",
        "email/task_completed.txt",
        ctx,
    )

    reply_to = [ctx["delegate_email"]] if ctx["delegate_email"] else []

    ok = _send(
        subject,
        to_addr,
        cc=[],
        reply_to=reply_to,
        html=html,
        txt=txt,
    )

    if not ok:
        logger.error(
            "Task completion email NOT delivered: task=%s to=%s",
            getattr(task, "id", None),
            to_addr,
        )
        return

    try:
        if leave and LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                extra={
                    "kind": "task_completed",
                    "task_type": task_type,
                    "task_id": getattr(task, "id", None),
                },
            )
    except Exception:
        logger.exception(
            "Failed to log EMAIL_SENT (task_completed) for leave #%s",
            getattr(leave, "id", None),
        )


def send_handover_completion_email(handover: LeaveHandover) -> None:
    try:
        if not _email_enabled():
            return

        task = handover.get_task_object()

        if not task:
            logger.info(
                "Completion email skipped: task not found for handover id=%s",
                getattr(handover, "id", None),
            )
            return

        original = handover.original_assignee
        delegate = handover.new_assignee

        context = {
            "completed_at": timezone.now(),
            "planned_date": getattr(task, "planned_date", None),
        }

        send_task_completion_email(
            original,
            delegate,
            task,
            context,
        )

    except Exception:
        logger.exception(
            "Failed in send_handover_completion_email for handover id=%s",
            getattr(handover, "id", None),
        )


__all__ = [
    "resolve_leave_recipients",
    "send_leave_request_email",
    "send_leave_applied_confirmation_email",
    "send_leave_decision_email",
    "send_handover_email",
    "send_delegation_reminder_email",
    "send_task_completion_email",
    "send_handover_completion_email",
]