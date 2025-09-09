from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

from django.conf import settings
from django.core import signing
from django.core.mail import EmailMultiAlternatives
from django.template.loader import get_template
from django.urls import reverse
from django.utils import timezone

from apps.users.routing import recipients_for_leave

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
TOKEN_SALT = "leave-action-v1"
TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7  # (kept for reference; validation happens in view)


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def _site_base() -> str:
    base = getattr(settings, "SITE_URL", "").strip() or "http://localhost:8000"
    return base.rstrip("/") + "/"

def _abs_url(path: str) -> str:
    return urljoin(_site_base(), path.lstrip("/"))

def _format_ist(dt) -> str:
    try:
        return timezone.localtime(dt, IST).strftime("%Y-%m-%d %H:%M IST")
    except Exception:
        return str(dt)

def _email_enabled() -> bool:
    # Respect your feature flag; default True
    try:
        return bool(getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        return True

def _render_pair(html_tpl: str, txt_tpl: str, context: Dict) -> Tuple[str, str]:
    html = get_template(html_tpl).render(context)
    txt = get_template(txt_tpl).render(context)
    return html, txt

def _send(subject: str, to_addr: str, cc: List[str], reply_to: List[str], html: str, txt: str) -> None:
    if not to_addr:
        logger.warning("Leave email suppressed: empty TO address.")
        return
    msg = EmailMultiAlternatives(
        subject=subject,
        body=txt,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        to=[to_addr],
        cc=cc or None,
        reply_to=reply_to or None,
    )
    msg.attach_alternative(html, "text/html")
    msg.send(fail_silently=getattr(settings, "EMAIL_FAIL_SILENTLY", True))

def _audit_email_sent(leave, email_kind: str, to_addr: str, cc: List[str]) -> None:
    # Best-effort; don’t break if model not available.
    try:
        from apps.leave.models import LeaveDecisionAudit, DecisionAction  # type: ignore
        LeaveDecisionAudit.objects.create(
            leave=leave,
            action=DecisionAction.EMAIL_SENT,
            decided_by=None,
            extra={"kind": email_kind, "to": to_addr, "cc": cc},
        )
    except Exception:
        pass


@dataclass
class _TokenLinks:
    approve: str | None
    reject: str | None


def _build_token_links(leave, recipient_email: str) -> _TokenLinks:
    """
    Create one-click secure links for Approve/Reject for a specific recipient.
    We include both 'actor_email' and (legacy) 'manager_email' in payload for compatibility.
    """
    recipient_email = (recipient_email or "").strip().lower()
    if not recipient_email:
        return _TokenLinks(None, None)

    payload_base = {
        "leave_id": int(leave.id),
        "actor_email": recipient_email,
        "manager_email": recipient_email,  # legacy key, kept for older handlers
    }

    approve_token = signing.dumps({**payload_base, "action": "approve"}, salt=TOKEN_SALT)
    reject_token = signing.dumps({**payload_base, "action": "reject"}, salt=TOKEN_SALT)

    approve_url = _abs_url(reverse("leave:leave_action_via_token", args=[approve_token]))
    reject_url = _abs_url(reverse("leave:leave_action_via_token", args=[reject_token]))
    return _TokenLinks(approve=approve_url, reject=reject_url)


def _duration_days_ist(leave) -> float:
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


# ---------------------------------------------------------------------------
# Public API (called from models.signals)
# ---------------------------------------------------------------------------

def send_leave_applied_email(leave) -> None:
    """
    Send request emails to ALL approvers:
      • Manager (routing 'to')
      • Each CC (routing 'cc')
    Each recipient gets token links bound to THEIR email (one-click; 7 days; one-time).
    Also provides a link to the approval page (login).
    """
    if not _email_enabled():
        return

    try:
        employee_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip()
        routing = recipients_for_leave(employee_email)
        manager_addr: str = (routing.get("to") or "").strip().lower()
        cc_list: List[str] = [e.strip().lower() for e in (routing.get("cc") or []) if e]

        # Specific approval page for this leave
        approval_page_url = _abs_url(reverse("leave:approval_page", args=[leave.id]))

        # Manager display name (best effort)
        manager_name = ""
        try:
            rp = getattr(leave, "reporting_person", None)
            if rp:
                manager_name = (getattr(rp, "get_full_name", lambda: "")() or rp.username or "").strip()
        except Exception:
            manager_name = ""

        # Common context (recipient-specific details added per email)
        base_ctx = {
            "site_url": _site_base().rstrip("/"),
            "leave_id": leave.id,
            "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
            "start_at_ist": _format_ist(leave.start_at),
            "end_at_ist": _format_ist(leave.end_at),
            "reason": leave.reason or "",
            "employee_name": leave.employee_name or (getattr(leave.employee, "get_full_name", lambda: "")() or leave.employee.username),
            "employee_email": employee_email,
            "employee_designation": leave.employee_designation or "",
            "is_half_day": bool(getattr(leave, "is_half_day", False)),
            "duration_days": _duration_days_ist(leave),
            "manager_name": manager_name,
            # Buttons in the template should open the approval page for this leave
            "approve_url": approval_page_url,
            "reject_url": approval_page_url,
            "approval_page_url": approval_page_url,
        }

        subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
        subject = f"{subject_prefix}Leave Request — {base_ctx['employee_name']} (#{leave.id})"

        # Per-recipient send so tokens are bound to the exact address
        recipients: List[str] = [e for e in [manager_addr, *cc_list] if e]

        for rec in recipients:
            tokens = _build_token_links(leave, rec)
            ctx = {
                **base_ctx,
                "manager_email": manager_addr,  # who routing picked as manager
                "cc_list": cc_list,             # info display
                "recipient_email": rec,
                "token_approve_url": tokens.approve,
                "token_reject_url": tokens.reject,
            }
            html, txt = _render_pair("email/leave_applied.html", "email/leave_applied.txt", ctx)
            reply_to = [e for e in [employee_email] if e]
            _send(subject, rec, cc=[], reply_to=reply_to, html=html, txt=txt)
            _audit_email_sent(leave, "applied", rec, [])

    except Exception as e:
        logger.exception("send_leave_applied_email failed for Leave #%s: %s", getattr(leave, "id", "?"), e)


def send_leave_decision_email(leave) -> None:
    """
    Send decision email back to the Employee.
    """
    if not _email_enabled():
        return

    try:
        employee_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip()
        if not employee_email:
            logger.warning("Decision email suppressed: employee has no email (leave #%s).", leave.id)
            return

        # Approver display name (best effort)
        approver_name = ""
        try:
            ap = getattr(leave, "approver", None) or getattr(leave, "reporting_person", None)
            if ap:
                approver_name = (getattr(ap, "get_full_name", lambda: "")() or ap.username or "").strip()
        except Exception:
            approver_name = ""

        status_label = leave.get_status_display()
        context = {
            "site_url": _site_base().rstrip("/"),
            "leave_id": leave.id,
            "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
            "start_at_ist": _format_ist(leave.start_at),
            "end_at_ist": _format_ist(leave.end_at),
            "employee_name": leave.employee_name or (getattr(leave.employee, "get_full_name", lambda: "")() or leave.employee.username),
            "decided_at_ist": _format_ist(leave.decided_at or timezone.now()),
            "decision_comment": leave.decision_comment or "",
            "status": status_label,
            "is_half_day": bool(getattr(leave, "is_half_day", False)),
            "duration_days": _duration_days_ist(leave),
            "reason": leave.reason or "",
            "approver_name": approver_name,
        }

        subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
        subject = f"{subject_prefix}Leave {status_label} — #{leave.id}"

        html, txt = _render_pair("email/leave_decision.html", "email/leave_decision.txt", context)

        # Reply-to goes to approver/manager where possible
        reply_to: List[str] = []
        try:
            if getattr(leave.approver, "email", ""):
                reply_to.append(leave.approver.email)
            else:
                routing = recipients_for_leave(employee_email)
                if routing.get("to"):
                    reply_to.append(routing["to"])
        except Exception:
            pass

        _send(subject, employee_email, [], reply_to, html, txt)
        _audit_email_sent(leave, "decision", employee_email, [])

    except Exception as e:
        logger.exception("send_leave_decision_email failed for Leave #%s: %s", getattr(leave, "id", "?"), e)
