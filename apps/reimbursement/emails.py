# apps/reimbursement/emails.py
from __future__ import annotations

import logging
from typing import List, Optional

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.template.exceptions import TemplateDoesNotExist
from django.urls import reverse

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


def _fallback_render(template_base: str, context: dict) -> tuple[str, str]:
    """
    Last-resort bodies when a template is missing.
    Kept intentionally simple to avoid introducing new dependencies.
    """
    # Pull common fields if present
    req_id = context.get("request_id") or getattr(context.get("request", None), "id", None) \
             or getattr(context.get("request_obj", None), "id", None)
    employee = context.get("employee_name") or ""
    total = context.get("total_amount") or ""
    detail_url = context.get("detail_url") or ""
    queue_url = context.get("queue_url") or ""
    note = context.get("note") or context.get("employee_note") or ""

    if template_base == "reimbursement_finance_verify":
        txt = (
            "New reimbursement pending Finance verification\n\n"
            f"Reimbursement ID: #{req_id}\n"
            f"Employee        : {employee}\n"
            f"Total Amount    : {total}\n"
            f"Note            : {note or '-'}\n"
            f"Queue           : {queue_url or '-'}\n"
        )
        html = f"""<!doctype html>
<html><body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif">
  <h2 style="margin:0 0 8px 0;">New reimbursement pending Finance verification</h2>
  <p style="margin:0 0 12px 0;">A new reimbursement has entered the Finance Verification queue.</p>
  <table style="border-collapse:collapse;margin:12px 0;">
    <tr><td style="padding:4px 8px;color:#555;">Reimbursement ID</td><td style="padding:4px 8px;"><strong>#{req_id}</strong></td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Employee</td><td style="padding:4px 8px;">{employee}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Total Amount</td><td style="padding:4px 8px;">{total}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Employee Note</td><td style="padding:4px 8px;">{note or '-'}</td></tr>
  </table>
  <p style="margin-top:12px;">Open EMS → Finance → Verification Queue to review and act.</p>
</body></html>"""
        return html, txt

    if template_base in ("reimbursement_request_paid", "reimbursement_paid"):
        payment_reference = context.get("payment_reference") or "-"
        paid_at = context.get("paid_at") or "-"
        txt = (
            "Reimbursement marked as Paid\n\n"
            f"Reimbursement ID: #{req_id}\n"
            f"Total Amount    : {total}\n"
            f"Reference       : {payment_reference}\n"
            f"Paid At         : {paid_at}\n"
            f"Details         : {detail_url or '-'}\n"
        )
        html = f"""<!doctype html>
<html><body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif">
  <h2 style="margin:0 0 8px 0;">Reimbursement marked as Paid</h2>
  <table style="border-collapse:collapse;margin:12px 0;">
    <tr><td style="padding:4px 8px;color:#555;">Reimbursement ID</td><td style="padding:4px 8px;"><strong>#{req_id}</strong></td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Total Amount</td><td style="padding:4px 8px;">{total}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Reference</td><td style="padding:4px 8px;">{payment_reference}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Paid At</td><td style="padding:4px 8px;">{paid_at}</td></tr>
  </table>
  <p style="margin-top:12px;">Details: {detail_url or '-'}</p>
</body></html>"""
        return html, txt

    # Generic fallback
    txt = (
        f"Notification: {template_base.replace('_', ' ').title()}\n\n"
        f"Reimbursement ID: #{req_id}\n"
        f"Employee        : {employee}\n"
        f"Total Amount    : {total}\n"
        f"Details         : {detail_url or queue_url or '-'}\n"
    )
    html = f"""<!doctype html>
<html><body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif">
  <h2 style="margin:0 0 8px 0;">Notification</h2>
  <p style="margin:0 0 12px 0;">{template_base.replace('_', ' ').title()}</p>
  <table style="border-collapse:collapse;margin:12px 0;">
    <tr><td style="padding:4px 8px;color:#555;">Reimbursement ID</td><td style="padding:4px 8px;"><strong>#{req_id}</strong></td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Employee</td><td style="padding:4px 8px;">{employee}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Total Amount</td><td style="padding:4px 8px;">{total}</td></tr>
  </table>
  <p style="margin-top:12px;">Details: {detail_url or queue_url or '-'}</p>
</body></html>"""
    return html, txt


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
    # Try primary templates
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

    # If either part is missing, try a conservative fallback variant mapping
    if html is None or txt is None:
        # Map a few known alternate filenames found in some deployments
        fallback_map = {
            "reimbursement_finance_verify": "reimbursement_submitted",
            "reimbursement_request_paid": "reimbursement_paid",
        }
        alt = fallback_map.get(template_base)
        if alt:
            try:
                if html is None:
                    html = render_to_string(f"email/{alt}.html", context)
                if txt is None:
                    txt = render_to_string(f"email/{alt}.txt", context)
            except Exception:
                # ignore and proceed to synthetic fallback
                pass

    # Final safety net: generate synthetic bodies if still missing
    if html is None or txt is None:
        html_f, txt_f = _fallback_render(template_base, context)
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
    Minimal, fail-silent sender with dedup, now resilient to missing templates.
    Also applies Pankaj-specific restrictions via central guard (ISSUE 18).
    """
    # Deduplicate while preserving order
    def _dedup(seq: Optional[List[str]]) -> Optional[List[str]]:
        if not seq:
            return None
        seen = set()
        out = []
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

    to_list = _dedup(to_list) or []
    if not to_list:
        return

    cc = _dedup(cc)
    bcc = _dedup(bcc)

    # ---- ISSUE 18: Remove Pankaj from reimbursement emails ---------------
    # One category is sufficient: "reimbursement" (blocked unless explicitly allowed in settings)
    filt_to, filt_cc, filt_bcc = filter_recipients_for_category(
        category="reimbursement",
        to=to_list,
        cc=cc or [],
        bcc=bcc or [],
    )
    if not (filt_to or filt_cc or filt_bcc):
        # Nothing to send after filtering
        return
    # ---------------------------------------------------------------------

    html, txt = _render(template_base, context)
    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=txt,
            to=filt_to,
            cc=filt_cc or None,
            bcc=filt_bcc or None,
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
    return (
        getattr(user, "get_full_name", lambda: "")()
        or getattr(user, "username", "")
        or str(user)
    ).strip()


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
    # FIX: correct manager queue URL name
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
    # FIX: correct manager queue URL name
    ctx = {
        "manager_name": _display_name(req.manager),
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
        "queue_url": _abs_url(reverse("reimbursement:manager_pending")),
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
    # FIX: remove trailing comma that turned dict into a tuple
    ctx = {
        "employee_name": _display_name(req.created_by),
        "manager_name": _display_name(req.manager),
        "decision": decision,
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
    }
    ctx["detail_url"] = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
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
    # FIX: remove trailing comma that turned dict into a tuple
    ctx = {
        "employee_name": _display_name(req.created_by),
        "management_name": _display_name(req.management),
        "decision": decision,
        "request_id": req.id,
        "total_amount": f"{req.total_amount:.2f}",
    }
    ctx["detail_url"] = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
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
