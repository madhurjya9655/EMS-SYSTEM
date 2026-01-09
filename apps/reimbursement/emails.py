# apps/reimbursement/emails.py
from __future__ import annotations

import logging
from typing import List, Optional

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils.http import urlencode

from .models import (
    ReimbursementLine,
    ReimbursementLog,
    ReimbursementRequest,
    ReimbursementSettings,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _site_base() -> str:
    """
    Best-effort absolute base URL. Configure one of:
      - REIMBURSEMENT_SITE_BASE (preferred)
      - SITE_URL
      - APP_BASE_URL
    Fallback to empty string (relative links in emails).
    """
    return (
        getattr(settings, "REIMBURSEMENT_SITE_BASE", None)
        or getattr(settings, "SITE_URL", None)
        or getattr(settings, "APP_BASE_URL", None)
        or ""
    ).rstrip("/")


def _abs_url(path: str) -> str:
    base = _site_base()
    if not base:
        return path
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _render(template_base: str, context: dict) -> tuple[str, str]:
    """
    Returns (html, text) for an email given a template base name.
    Template files expected at:
      - templates/email/{template_base}.html
      - templates/email/{template_base}.txt
    """
    html = render_to_string(f"email/{template_base}.html", context)
    txt = render_to_string(f"email/{template_base}.txt", context)
    return html, txt


def _send(to_list: List[str], subject: str, template_base: str, context: dict, *, cc: Optional[List[str]] = None, bcc: Optional[List[str]] = None) -> None:
    """
    Minimal, fail-silent sender.
    """
    # Deduplicate while preserving order
    def _dedup(seq: Optional[List[str]]) -> Optional[List[str]]:
        if not seq:
            return None
        seen = set()
        out = []
        for s in seq:
            s = (s or "").strip()
            if not s or s.lower() in seen:
                continue
            seen.add(s.lower())
            out.append(s)
        return out or None

    to_list = _dedup(to_list) or []
    if not to_list:
        return

    cc = _dedup(cc)
    bcc = _dedup(bcc)

    html, txt = _render(template_base, context)
    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=txt,
            to=to_list,
            cc=cc,
            bcc=bcc,
            from_email=getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None),
        )
        msg.attach_alternative(html, "text/html")
        msg.send(fail_silently=True)
    except Exception:  # pragma: no cover
        logger.exception("Email send failed: %s", template_base)


def _employee_email(req: ReimbursementRequest) -> List[str]:
    email = (getattr(req.created_by, "email", "") or "").strip()
    return [email] if email else []


def _manager_emails(req: ReimbursementRequest) -> List[str]:
    email = (getattr(req.manager, "email", "") or "").strip()
    return [email] if email else []


def _management_emails(req: ReimbursementRequest) -> List[str]:
    email = (getattr(req.management, "email", "") or "").strip()
    return [email] if email else []


def _finance_emails() -> List[str]:
    return ReimbursementSettings.get_solo().finance_email_list()


def _display_name(user) -> str:
    if not user:
        return ""
    return (getattr(user, "get_full_name", lambda: "")() or getattr(user, "username", "") or str(user)).strip()


# ---------------------------------------------------------------------------
# Bill-specific notifications (used by bill-level workflow)
# ---------------------------------------------------------------------------

def send_bill_rejected_by_finance(req: ReimbursementRequest, line: ReimbursementLine) -> None:
    """
    1️⃣ Finance rejects a single bill — email ONLY the employee with bill details and reason.
    """
    subject = f"Reimbursement #{req.id}: One bill was rejected by Finance"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "rejection_reason": line.finance_rejection_reason or "-",
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])) if hasattr(req, "id") else "",
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }
    _send(_employee_email(req), subject, "reimbursement_bill_rejected_by_finance", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} rejected by finance sent to employee.",
        extra={"line_id": line.id, "template": "reimbursement_bill_rejected_by_finance"},
    )


def send_bill_resubmitted(req: ReimbursementRequest, line: ReimbursementLine, *, actor) -> None:
    """
    2️⃣ Employee edits/replaces a previously rejected bill — email Finance team.
    """
    subject = f"Reimbursement #{req.id}: Employee resubmitted a corrected bill"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "resubmitted_by": _display_name(actor),
        "detail_url": _abs_url(reverse("reimbursement:finance_pending")),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }
    _send(_finance_emails(), subject, "reimbursement_bill_resubmitted", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=actor,
        message=f"Email: bill #{line.id} resubmitted sent to finance.",
        extra={"line_id": line.id, "template": "reimbursement_bill_resubmitted"},
    )


def send_bill_to_manager(req: ReimbursementRequest, line: ReimbursementLine) -> None:
    """
    3️⃣ Finance approved a bill and proceeded it to Manager — email the manager.
    """
    subject = f"Reimbursement #{req.id}: Bill #{line.id} needs your approval"
    # Link to manager bill queue
    queue_url = _abs_url(reverse("reimbursement:manager_bills_pending"))
    ctx = {
        "manager_name": _display_name(req.manager),
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "queue_url": queue_url,
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }
    _send(_manager_emails(req), subject, "reimbursement_bill_to_manager", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} sent to manager.",
        extra={"line_id": line.id, "template": "reimbursement_bill_to_manager"},
    )


def send_bill_rejected_by_manager(req: ReimbursementRequest, line: ReimbursementLine, *, reason: str = "") -> None:
    """
    4️⃣ Manager rejects a bill — notify the employee.
    """
    subject = f"Reimbursement #{req.id}: One bill was rejected by Manager"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "manager_name": _display_name(req.manager),
        "rejection_reason": (reason or "-"),
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    _send(_employee_email(req), subject, "reimbursement_bill_rejected_by_manager", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} rejected by manager sent to employee.",
        extra={"line_id": line.id, "template": "reimbursement_bill_rejected_by_manager"},
    )


def send_bill_paid(req: ReimbursementRequest, line: ReimbursementLine) -> None:
    """
    5️⃣ Finance marked a bill as PAID — notify the employee.
    """
    subject = f"Reimbursement #{req.id}: Bill #{line.id} paid"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "payment_reference": line.payment_reference or "-",
        "paid_at": line.paid_at,
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    _send(_employee_email(req), subject, "reimbursement_bill_paid", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} paid sent to employee.",
        extra={"line_id": line.id, "template": "reimbursement_bill_paid"},
    )


# ---------------------------------------------------------------------------
# Request-level notifications (kept for compatibility with the views)
# ---------------------------------------------------------------------------

def send_reimbursement_finance_verify(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    """
    Triggered when an employee creates/resubmits a request. Notifies Finance.
    """
    subject = f"Reimbursement #{req.id}: Submitted for Finance Verification"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "note": employee_note or "-",
        "queue_url": _abs_url(reverse("reimbursement:finance_pending")),
        "submitted_at": req.submitted_at,
    }
    _send(_finance_emails(), subject, "reimbursement_finance_verify", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: submitted to Finance Verification.",
        extra={"template": "reimbursement_finance_verify"},
    )


def send_reimbursement_finance_verified(req: ReimbursementRequest) -> None:
    """
    Triggered when Finance finalizes and request moves to manager stage.
    """
    subject = f"Reimbursement #{req.id}: Ready for your approval"
    ctx = {
        "manager_name": _display_name(req.manager),
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "queue_url": _abs_url(reverse("reimbursement:manager_bills_pending")),
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    to = _manager_emails(req)
    if not to:
        # If manager isn't mapped (or has no email), notify Finance so they can route.
        to = _finance_emails()
    _send(to, subject, "reimbursement_finance_verified", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: finance verified -> manager.",
        extra={"template": "reimbursement_finance_verified"},
    )


def send_reimbursement_manager_action(req: ReimbursementRequest, *, decision: str) -> None:
    """
    Notify stakeholder(s) after manager decision via UI/email link.
    """
    subject = f"Reimbursement #{req.id}: Manager decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "manager_name": _display_name(req.manager),
        "decision": decision,
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    _send(_employee_email(req), subject, "reimbursement_manager_action", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: manager decision ({decision}) sent to employee.",
        extra={"template": "reimbursement_manager_action"},
    )


def send_reimbursement_management_action(req: ReimbursementRequest, *, decision: str) -> None:
    """
    Notify employee after management decision.
    """
    subject = f"Reimbursement #{req.id}: Management decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "management_name": _display_name(req.management),
        "decision": decision,
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    _send(_employee_email(req), subject, "reimbursement_management_action", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: management decision ({decision}) sent to employee.",
        extra={"template": "reimbursement_management_action"},
    )


def send_reimbursement_paid(req: ReimbursementRequest) -> None:
    """
    Request-level 'Paid' (legacy convenience when all bills paid) — notify employee.
    """
    subject = f"Reimbursement #{req.id}: Claim Settled"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "payment_reference": (req.finance_payment_reference or "-"),
        "paid_at": req.paid_at,
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
    }
    _send(_employee_email(req), subject, "reimbursement_request_paid", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: request marked paid.",
        extra={"template": "reimbursement_request_paid"},
    )
