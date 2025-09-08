# apps/leave/services/notifications.py
from __future__ import annotations

import logging
from datetime import timedelta, date
from typing import Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import signing
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from apps.leave.models import (
    LeaveRequest,
    LeaveDecisionAudit,
    DecisionAction,
)

logger = logging.getLogger(__name__)
User = get_user_model()

IST = ZoneInfo("Asia/Kolkata")
TOKEN_SALT = getattr(settings, "LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
TOKEN_MAX_AGE_SECONDS = getattr(settings, "LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _site_base_url() -> str:
    base = (
        getattr(settings, "SITE_URL", "")
        or getattr(settings, "SITE_BASE_URL", "")
        or "http://localhost:8000"
    ).strip()
    return base.rstrip("/")


def _abs(path: str) -> str:
    return f"{_site_base_url()}{path}"


def _ist(dt):
    try:
        return timezone.localtime(dt, IST)
    except Exception:
        return dt


def _format_dates_for_subject(lr: LeaveRequest) -> str:
    """
    Compact human-readable range for email subject.
    Uses IST, inclusive end date.
    """
    try:
        s = _ist(lr.start_at).date()
        # inclusive end: subtract 1 microsecond then take date
        e = _ist(lr.end_at - timedelta(microseconds=1)).date()
    except Exception:
        s = lr.start_at.date()
        e = lr.end_at.date()

    if s == e:
        return s.strftime("%d %b %Y")
    if s.month == e.month and s.year == e.year:
        return f"{s.day}–{e.day} {s.strftime('%b %Y')}"
    return f"{s.strftime('%d %b %Y')} – {e.strftime('%d %b %Y')}"


def _approval_page_url(lr: LeaveRequest) -> str:
    return _abs(reverse("leave:approval_page", args=[lr.id]))


def _token_links_for_recipient(lr: LeaveRequest, recipient_email: str) -> Tuple[str, str]:
    """
    Build one-click token links for the given recipient.
    """
    email_norm = (recipient_email or "").strip().lower()
    base = {"leave_id": int(lr.id), "actor_email": email_norm, "manager_email": email_norm}
    approve = signing.dumps({**base, "action": "approve"}, salt=TOKEN_SALT)
    reject = signing.dumps({**base, "action": "reject"}, salt=TOKEN_SALT)
    return (
        _abs(reverse("leave:token_decide", args=[approve])) + "?a=APPROVED",
        _abs(reverse("leave:token_decide", args=[reject])) + "?a=REJECTED",
    )


def _already_sent_recent(
    leave: LeaveRequest, kind_hint: str | None = None, within_seconds: int = 90
) -> bool:
    try:
        since = timezone.now() - timedelta(seconds=within_seconds)
        qs = LeaveDecisionAudit.objects.filter(
            leave=leave, action=DecisionAction.EMAIL_SENT, decided_at__gte=since
        )
        if kind_hint:
            qs = qs.filter(extra__kind=kind_hint)
        return qs.exists()
    except Exception:
        return False


def _audit_sent(
    leave: LeaveRequest, kind: str, to: Iterable[str], cc: Iterable[str] | None = None
) -> None:
    try:
        LeaveDecisionAudit.objects.create(
            leave=leave,
            action=DecisionAction.EMAIL_SENT,
            decided_by=None if kind != "decision" else leave.approver,
            extra={"kind": kind, "to": list(to), "cc": list(cc or [])},
        )
    except Exception:
        logger.exception(
            "Failed to write EMAIL_SENT audit (kind=%s, leave=%s)",
            kind,
            getattr(leave, "id", "?"),
        )


def _datespan_ist(start_dt, end_dt) -> List[date]:
    """Inclusive list of IST dates between start_dt and end_dt (order agnostic)."""
    if not (start_dt and end_dt):
        return []
    s = _ist(start_dt).date()
    e = _ist(end_dt).date()
    if e < s:
        s, e = e, s
    out: List[date] = []
    cur = s
    while cur <= e:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _duration_days(leave: LeaveRequest) -> float:
    """
    Duration in days using IST, matching how UI/admin calculates:
    - half-day if single-date half-day
    - otherwise number of unique dates in span
    """
    span = _datespan_ist(leave.start_at, leave.end_at)
    if not span:
        return 0.0
    uniq = set(span)
    if leave.is_half_day and len(uniq) == 1:
        return 0.5
    return float(len(uniq))


def _employee_display_name(user) -> str:
    try:
        return (getattr(user, "get_full_name", lambda: "")() or user.username or "").strip()
    except Exception:
        return (getattr(user, "username", "") or "").strip()


def _manager_name_for_email(leave: LeaveRequest, manager_email: str) -> Optional[str]:
    """
    Try to show a pretty manager name for header:
    - If leave.reporting_person matches email, use their full name/username
    - Else look up a User with that email
    """
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


def _resolve_rp_cc_from_args_or_mapping(
    leave: LeaveRequest,
    manager_email: Optional[str],
    cc_list: Optional[Iterable[str]],
) -> Tuple[str, List[str]]:
    """
    Return (manager_email, cc_list) using explicit args > model mapping > routing fallback.
    Emails are normalized to lowercase/trimmed.
    """
    if manager_email:
        return manager_email.strip().lower(), [e.strip().lower() for e in (cc_list or []) if e]

    # Prefer model mapping present on the leave instance
    if getattr(leave, "reporting_person", None) and getattr(leave.reporting_person, "email", None):
        to_addr = leave.reporting_person.email.strip().lower()
        cc = []
        # cc_person is optional on the LeaveRequest snapshot/model; guard it.
        if getattr(leave, "cc_person", None) and getattr(leave.cc_person, "email", None):
            cc.append(leave.cc_person.email.strip().lower())
        return to_addr, cc

    # Fallback to dynamic routing
    try:
        from apps.users.routing import recipients_for_leave

        emp_email = (
            leave.employee_email or getattr(leave.employee, "email", "") or ""
        ).strip().lower()
        mapping = recipients_for_leave(emp_email) or {}
        to_addr = (mapping.get("to") or "").strip().lower()
        cc = [e.strip().lower() for e in (mapping.get("cc") or []) if e]
        return to_addr, cc
    except Exception:
        logger.warning(
            "Could not resolve routing; no recipients for leave id=%s",
            getattr(leave, "id", None),
        )
        return "", []


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
def send_leave_request_email(
    leave: LeaveRequest,
    manager_email: Optional[str] = None,
    cc_list: Optional[Iterable[str]] = None,
    *,
    force: bool = False,
) -> None:
    """
    Notify Reporting Person (and CC) about a new leave request.
    Renders templates/email/leave_applied.{html,txt}
    """
    # allow forced resend (e.g., after editing routing)
    if not force and _already_sent_recent(leave, kind_hint="request"):
        logger.info(
            "Suppressing duplicate request email for leave #%s (recent audit found).",
            leave.id,
        )
        return

    to_addr, cc = _resolve_rp_cc_from_args_or_mapping(leave, manager_email, cc_list)
    if not to_addr:
        logger.warning("Request email suppressed: no RP email for leave #%s.", leave.id)
        return

    # URLs
    token_approve_url, token_reject_url = _token_links_for_recipient(leave, to_addr)
    # Approval-page buttons (login flow). We add a hint param (?a=...) for UX.
    approval_page = _approval_page_url(leave)
    approve_url = f"{approval_page}?a=APPROVED"
    reject_url = f"{approval_page}?a=REJECTED"

    # People
    employee_name = leave.employee_name or _employee_display_name(leave.employee)
    manager_name = _manager_name_for_email(leave, to_addr)

    # Subject
    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Leave Request — {employee_name} ({_format_dates_for_subject(leave)})"

    # Context for templates
    ctx = {
        # employee
        "employee_name": employee_name,
        "employee_email": (leave.employee_email or getattr(leave.employee, "email", "") or "").strip(),
        "employee_id": getattr(leave, "employee_id", "") or "",
        "employee_phone": getattr(leave, "employee_phone", "") or "",
        "employee_designation": getattr(leave, "employee_designation", "") or "",
        # routing
        "manager_email": to_addr,
        "manager_name": manager_name,
        "cc_list": list(cc or []),
        # leave
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _ist(leave.start_at).strftime("%d %b %Y, %I:%M %p"),
        "end_at_ist": _ist(leave.end_at).strftime("%d %b %Y, %I:%M %p"),
        "is_half_day": bool(leave.is_half_day),
        "reason": (leave.reason or "").strip(),
        "duration_days": _duration_days(leave),
        "blocked_days": getattr(leave, "blocked_days", None),
        # actions
        "approve_url": approve_url,
        "reject_url": reject_url,
        # One-click tokens (optional)
        "token_approve_url": token_approve_url,
        "token_reject_url": token_reject_url,
        # site
        "site_url": _site_base_url(),
    }

    # Render templates
    body_txt = render_to_string("email/leave_applied.txt", ctx)
    body_html = render_to_string("email/leave_applied.html", ctx)

    # Build message
    msg = EmailMultiAlternatives(
        subject=subject,
        body=body_txt,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        to=[to_addr],
        cc=list(cc) or None,
        reply_to=[
            e
            for e in [
                (leave.employee_email or getattr(leave.employee, "email", "")),
            ]
            if e
        ],
    )
    msg.attach_alternative(body_html, "text/html")

    try:
        msg.send(fail_silently=False)
        _audit_sent(leave, "request", to=[to_addr], cc=cc)
    except Exception:
        logger.exception("Failed to send leave request email (leave #%s).", leave.id)


def send_leave_decision_email(leave: LeaveRequest) -> None:
    """
    Notify employee about APPROVED/REJECTED decision.
    Renders templates/email/leave_decision.{html,txt}
    """
    if _already_sent_recent(leave, kind_hint="decision"):
        logger.info(
            "Suppressing duplicate decision email for leave #%s (recent audit found).",
            leave.id,
        )
        return

    to_addr: Optional[str] = (
        leave.employee_email or getattr(leave.employee, "email", "") or ""
    ).strip()
    if not to_addr:
        logger.info(
            "Decision email suppressed: employee has no email (leave #%s).",
            leave.id,
        )
        return

    status_label = leave.get_status_display()
    subject_prefix = getattr(settings, "EMAIL_SUBJECT_PREFIX", "[EMS] ")
    subject = f"{subject_prefix}Leave {status_label} — {_format_dates_for_subject(leave)}"

    approver_name = None
    try:
        if getattr(leave, "approver", None):
            approver_name = (
                getattr(leave.approver, "get_full_name", lambda: "")() or leave.approver.username
            ).strip()
    except Exception:
        approver_name = "Manager"

    ctx = {
        # decision
        "status": status_label,
        "approver_name": approver_name or "Manager",
        "decided_at_ist": _ist(getattr(leave, "decided_at", timezone.now())).strftime(
            "%d %b %Y, %I:%M %p"
        ),
        "decision_comment": (getattr(leave, "decision_comment", "") or "").strip(),
        # employee
        "employee_name": leave.employee_name or _employee_display_name(leave.employee),
        # leave
        "leave_id": leave.id,
        "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "start_at_ist": _ist(leave.start_at).strftime("%d %b %Y, %I:%M %p"),
        "end_at_ist": _ist(leave.end_at).strftime("%d %b %Y, %I:%M %p"),
        "is_half_day": bool(leave.is_half_day),
        "reason": (leave.reason or "").strip(),
        "duration_days": _duration_days(leave),
        "blocked_days": getattr(leave, "blocked_days", None),
        # site
        "site_url": _site_base_url(),
    }

    body_txt = render_to_string("email/leave_decision.txt", ctx)
    body_html = render_to_string("email/leave_decision.html", ctx)

    reply_to: List[str] = []
    try:
        if getattr(leave.approver, "email", ""):
            reply_to.append(leave.approver.email)
        elif getattr(leave.reporting_person, "email", ""):
            reply_to.append(leave.reporting_person.email)
    except Exception:
        pass

    msg = EmailMultiAlternatives(
        subject=subject,
        body=body_txt,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        to=[to_addr],
        reply_to=reply_to or None,
    )
    msg.attach_alternative(body_html, "text/html")

    try:
        msg.send(fail_silently=False)
        _audit_sent(leave, "decision", to=[to_addr])
    except Exception:
        logger.exception("Failed to send leave decision email (leave #%s).", leave.id)
