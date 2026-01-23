# apps/reimbursement/emails.py
from __future__ import annotations

import logging
from typing import List, Optional, Iterable, Dict
from decimal import Decimal
from datetime import datetime

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.template.exceptions import TemplateDoesNotExist
from django.urls import reverse
from django.utils import timezone

from .models import (
    ReimbursementLine,
    ReimbursementLog,
    ReimbursementRequest,
    ReimbursementSettings,
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Central recipient guard (config-driven; no hardcoding)
# If helper isn't available for any reason, fall back to no-op.
# -------------------------------------------------------
try:
    from apps.common.email_guard import filter_recipients_for_category
except Exception:  # pragma: no cover
    def filter_recipients_for_category(*, category: str, to=None, cc=None, bcc=None, **_):
        return list(to or []), list(cc or []), list(bcc or [])


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


def _fmt_amount(value) -> str:
    try:
        if value is None:
            return "0.00"
        if isinstance(value, Decimal):
            value = float(value)
        return f"{value:,.2f}"
    except Exception:
        return str(value or "0.00")


def _fmt_dt(dt) -> str:
    """Format to: 17 Jan 2026, 14:50 IST (local tz name appended)."""
    try:
        if not dt:
            return ""
        loc = timezone.localtime(dt, timezone.get_current_timezone())
        tzname = loc.tzname() or "IST"
        return loc.strftime(f"%d %b %Y, %H:%M {tzname}")
    except Exception:
        # Last-resort plain str
        return str(dt)


def _dedup(seq: Optional[Iterable[str]]) -> Optional[List[str]]:
    if not seq:
        return None
    seen = set()
    out: List[str] = []
    for s in seq:
        s = (s or "").strip()
        if not s:
            continue
        low = s.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(s)
    return out or None


def _render(template_base: str, context: dict) -> tuple[str, str]:
    """
    Returns (html, text) for an email given a template base name.
    Template files expected at:
      - templates/email/{template_base}.html
      - templates/email/{template_base}.txt

    Hardened: if one or both templates are missing, render a safe fallback body
    (no exception bubbles up, so emails never block workflows).
    """
    html = txt = None
    try:
        html = render_to_string(f"email/{template_base}.html", context)
    except TemplateDoesNotExist:
        html = None
    except Exception:  # pragma: no cover
        logger.exception("HTML template render failed for %s", template_base)
        html = None

    try:
        txt = render_to_string(f"email/{template_base}.txt", context)
    except TemplateDoesNotExist:
        txt = None
    except Exception:  # pragma: no cover
        logger.exception("TXT template render failed for %s", template_base)
        txt = None

    # Final safety net: generate synthetic bodies if still missing
    if html is None or txt is None:
        req_id = context.get("request_id", "")
        employee = context.get("employee_name", "")
        total = context.get("total_amount", "")
        detail_url = context.get("detail_url") or "-"
        title = template_base.replace("_", " ").title()
        html_f = f"""<!doctype html>
<html><body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif">
  <h2 style="margin:0 0 8px 0;">{title}</h2>
  <table style="border-collapse:collapse;margin:12px 0;">
    <tr><td style="padding:4px 8px;color:#555;">Request ID</td><td style="padding:4px 8px;"><strong>#{req_id}</strong></td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Employee</td><td style="padding:4px 8px;">{employee}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Total Amount</td><td style="padding:4px 8px;">₹{total}</td></tr>
  </table>
  <p style="margin-top:12px;">Details: {detail_url}</p>
</body></html>"""
        txt_f = (
            f"{title}\n\n"
            f"Request ID : #{req_id}\n"
            f"Employee   : {employee}\n"
            f"Total Amt  : ₹{total}\n"
            f"Details    : {detail_url}\n"
        )
        html = html or html_f
        txt = txt or txt_f

    return html, txt


def _send(
    to_list: List[str],
    subject: str,
    template_base: str,
    context: dict,
    *,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
) -> None:
    """
    Minimal, fail-silent sender with recipient guard.
    """
    to_list = _dedup(to_list) or []
    if not to_list:
        return
    cc = _dedup(cc)
    bcc = _dedup(bcc)

    # Recipient guard (e.g., exclusions/suppression by environment)
    filt_to, filt_cc, filt_bcc = filter_recipients_for_category(
        category="reimbursement",
        to=to_list,
        cc=cc or [],
        bcc=bcc or [],
    )
    if not (filt_to or filt_cc or filt_bcc):
        return

    html, txt = _render(template_base, context)
    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=txt,
            to=filt_to,
            cc=filt_cc or None,
            bcc=filt_bcc or None,
            from_email=getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None)
                       or getattr(settings, "DEFAULT_FROM_EMAIL", None),
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
    return (
        getattr(user, "get_full_name", lambda: "")()
        or getattr(user, "username", "")
        or str(user)
    ).strip()


# ---------- Helpers for final notification ----------

def _final_to_list() -> List[str]:
    """TO recipients for final notification (configurable via settings)."""
    default_to = ["jyothi@gasteels.com", "chetan.shah@gasteels.com"]
    return list(getattr(settings, "REIMBURSEMENT_FINAL_TO", default_to))


def _final_cc_list() -> List[str]:
    """CC recipients for final notification (configurable via settings)."""
    default_cc = [
        "amreen@blueoceansteels.com",
        "vilas@blueoceansteels.com",
        "akshay@blueoceansteels.com",
        "sharyu@blueoceansteels.com",
    ]
    return list(getattr(settings, "REIMBURSEMENT_FINAL_CC", default_cc))


def _line_bill_url(line: ReimbursementLine) -> str:
    try:
        # urlpattern: path("receipt/line/<int:line_id>/", ..., name="receipt_line")
        return _abs_url(reverse("reimbursement:receipt_line", kwargs={"line_id": line.id}))
    except Exception:
        return ""


def _lines_context(req: ReimbursementRequest) -> List[Dict[str, str]]:
    """
    Build the lines dataset for email templates:
    category, date, description, amount (no currency symbol), bill_url.
    Only includes request lines that are part of the reimbursement.
    """
    out: List[Dict[str, str]] = []
    try:
        qs = (
            req.lines.select_related("expense_item")
            .filter(status=ReimbursementLine.Status.INCLUDED)
            .order_by("id")
        )
        for line in qs:
            item = line.expense_item
            # Category
            try:
                category = item.get_category_display()
            except Exception:
                category = getattr(item, "category", "") or ""
            # Date
            try:
                date_str = item.date.strftime("%d %b %Y")
            except Exception:
                date_str = str(getattr(item, "date", "") or "")
            # Description
            description = (line.description or getattr(item, "description", "") or "").strip() or "-"
            # Amount (no currency symbol; template adds "₹")
            amt = line.amount if line.amount is not None else getattr(item, "amount", None)
            amount_str = _fmt_amount(amt)
            # Bill URL
            bill_url = _line_bill_url(line)
            out.append(
                {
                    "category": category,
                    "date": date_str,
                    "description": description,
                    "amount": amount_str,
                    "bill_url": bill_url,
                }
            )
    except Exception:
        logger.exception("Failed building lines for request #%s", getattr(req, "id", "?"))
    return out


# ---------------------------------------------------------------------------
# Bill-specific notifications (used by bill-level workflow)
# (UNCHANGED)
# ---------------------------------------------------------------------------

def send_bill_rejected_by_finance(req: ReimbursementRequest, line: ReimbursementLine) -> None:
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
    subject = f"Reimbursement #{req.id}: Bill #{line.id} needs your approval"
    queue_url = _abs_url(reverse("reimbursement:manager_pending"))
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
# (EXISTING FUNCTIONS UNCHANGED)
# ---------------------------------------------------------------------------

def send_reimbursement_finance_verify(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    subject = f"Reimbursement #{req.id}: Submitted for Finance Verification"
    review_url = _abs_url(reverse("reimbursement:finance_verify", args=[req.id]))
    ctx = {
        "employee_name": _display_name(req.created_by),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "note": employee_note or "-",
        "queue_url": _abs_url(reverse("reimbursement:finance_pending")),
        "review_url": review_url,   # button target
        "approve_url": review_url,  # button target
        "reject_url": review_url,   # button target
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
    subject = f"Reimbursement #{req.id}: Ready for your approval"
    review_url = _abs_url(reverse("reimbursement:manager_review", args=[req.id]))
    ctx = {
        "manager_name": _display_name(req.manager),
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "queue_url": _abs_url(reverse("reimbursement:manager_pending")),
        "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
        "review_url": review_url,   # button target
        "approve_url": review_url,  # button target
        "reject_url": review_url,   # button target
    }
    to = _manager_emails(req)
    if not to:
        to = _finance_emails()  # fallback to finance if manager has no email
    _send(to, subject, "reimbursement_finance_verified", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: finance verified -> manager.",
        extra={"template": "reimbursement_finance_verified"},
    )


def send_reimbursement_manager_action(req: ReimbursementRequest, *, decision: str) -> None:
    subject = f"Reimbursement #{req.id}: Manager decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "manager_name": _display_name(req.manager),
        "decision": decision,
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
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
    # NOTE: we do NOT auto-send final notification here to avoid duplicate emails.
    # Call send_reimbursement_final_notification(req) from your approval handler.


def send_reimbursement_management_action(req: ReimbursementRequest, *, decision: str) -> None:
    subject = f"Reimbursement #{req.id}: Management decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "management_name": _display_name(req.management),
        "decision": decision,
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
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
    subject = f"Reimbursement #{req.id}: Claim Settled"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
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


# ---------------------------------------------------------------------------
# NEW: final after manager approval (wired to your new templates)
# ---------------------------------------------------------------------------

def send_reimbursement_final_notification(req: ReimbursementRequest) -> None:
    """
    Final email after approval.
    TO:  settings.REIMBURSEMENT_FINAL_TO (or defaults)
    CC:  settings.REIMBURSEMENT_FINAL_CC (or defaults)

    Body matches the approved example with full expense table.
    """
    try:
        employee_name = _display_name(req.created_by)
        verified_by = _display_name(getattr(req, "verified_by", None)) or "Finance"
        approved_by = _display_name(getattr(req, "manager", None)) or "Approver"
        approved_at = getattr(req, "manager_decided_at", None) or timezone.now()

        ctx = {
            "employee_name": employee_name,
            "request_id": req.id,
            "total_amount": _fmt_amount(req.total_amount),
            "verified_by": verified_by,
            "approved_by": approved_by,
            "approved_at": _fmt_dt(approved_at),
            "detail_url": _abs_url(reverse("reimbursement:request_detail", args=[req.id])),
            "lines": _lines_context(req),
        }

        subject = f"Approved Reimbursement — {employee_name} — ₹{ctx['total_amount']}"
        to_list = _final_to_list()
        cc_list = _final_cc_list()

        _send(to_list, subject, "reimbursement_final_notification", ctx, cc=cc_list)

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.EMAIL_SENT,
            actor=None,
            message="Email: final notification after approval.",
            extra={"template": "reimbursement_final_notification", "to": to_list, "cc": cc_list},
        )
    except Exception:
        logger.exception("Failed to send final notification for request #%s", getattr(req, "id", "?"))
