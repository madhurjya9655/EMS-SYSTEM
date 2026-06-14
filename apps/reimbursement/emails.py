# apps/reimbursement/emails.py

from __future__ import annotations

import logging
from typing import List, Optional, Iterable, Dict
from decimal import Decimal

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db.models import Sum
from django.template.loader import render_to_string
from django.template.exceptions import TemplateDoesNotExist
from django.urls import reverse, NoReverseMatch
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
    Returns the absolute base URL for the deployment.

    Priority order:
      1. REIMBURSEMENT_SITE_BASE
      2. SITE_URL
      3. APP_BASE_URL

    If none is set, logs a prominent ERROR so the misconfiguration surfaces
    immediately in Render logs.
    """
    base = (
        getattr(settings, "REIMBURSEMENT_SITE_BASE", None)
        or getattr(settings, "SITE_URL", None)
        or getattr(settings, "APP_BASE_URL", None)
    )
    if base:
        return base.rstrip("/")

    logger.error(
        "REIMBURSEMENT EMAIL LINKS BROKEN: None of REIMBURSEMENT_SITE_BASE / "
        "SITE_URL / APP_BASE_URL is set in settings.py. "
        "Fix: add REIMBURSEMENT_SITE_BASE = 'https://your-app.onrender.com' "
        "to your settings / Render environment variables."
    )
    return ""


def _abs_url(path: str) -> str:
    """
    Convert a Django path to an absolute URL using the configured site base.
    If the path is already absolute, return it unchanged.
    """
    if path.startswith("http://") or path.startswith("https://"):
        return path

    base = _site_base()
    if not path.startswith("/"):
        path = "/" + path

    if not base:
        return path

    return f"{base}{path}"


def _safe_reverse(viewname: str, *args, **kwargs) -> str:
    """
    Wrapper around django.urls.reverse that never raises.
    Returns empty string on failure and logs a warning.
    """
    try:
        return reverse(viewname, *args, **kwargs)
    except NoReverseMatch:
        logger.warning(
            "_safe_reverse: could not reverse '%s' args=%s kwargs=%s",
            viewname,
            args,
            kwargs,
        )
        return ""


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
    """Format to: 17 Jan 2026, 14:50 IST."""
    try:
        if not dt:
            return ""
        loc = timezone.localtime(dt, timezone.get_current_timezone())
        tzname = loc.tzname() or "IST"
        return loc.strftime(f"%d %b %Y, %H:%M {tzname}")
    except Exception:
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

    If templates are missing, render a safe fallback body.
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

    if html is None or txt is None:
        req_id = context.get("request_id", "")
        employee = context.get("employee_name", "")
        total = context.get("total_amount", "")
        detail_url = context.get("detail_url") or context.get("queue_url") or "-"
        title = template_base.replace("_", " ").title()

        html_f = f"""<!doctype html>
<html><body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif">
  <h2 style="margin:0 0 8px 0;">{title}</h2>
  <table style="border-collapse:collapse;margin:12px 0;">
    <tr><td style="padding:4px 8px;color:#555;">Request ID</td><td style="padding:4px 8px;"><strong>#{req_id}</strong></td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Employee</td><td style="padding:4px 8px;">{employee}</td></tr>
    <tr><td style="padding:4px 8px;color:#555;">Total Amount</td><td style="padding:4px 8px;">&#8377;{total}</td></tr>
  </table>
  <p style="margin-top:12px;">Details: <a href="{detail_url}">{detail_url}</a></p>
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
    Fail-silent sender with recipient guard.
    Logs a warning when recipient list is empty.
    """
    to_list = _dedup(to_list) or []
    if not to_list:
        logger.warning(
            "Email '%s' not sent: recipient list is empty.",
            template_base,
        )
        return

    cc = _dedup(cc)
    bcc = _dedup(bcc)

    filt_to, filt_cc, filt_bcc = filter_recipients_for_category(
        category="reimbursement",
        to=to_list,
        cc=cc or [],
        bcc=bcc or [],
    )

    if not (filt_to or filt_cc or filt_bcc):
        logger.warning(
            "Email '%s' suppressed by filter_recipients_for_category. "
            "All recipients were filtered out.",
            template_base,
        )
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
        logger.info(
            "Email '%s' sent to %s (cc=%s)",
            template_base,
            filt_to,
            filt_cc,
        )
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
# URL helpers for email links
# ---------------------------------------------------------------------------

def _manager_queue_url() -> str:
    return _abs_url(_safe_reverse("reimbursement:manager_pending"))


def _manager_review_url(req: ReimbursementRequest) -> str:
    path = _safe_reverse("reimbursement:manager_review", args=[req.id])
    if not path:
        return _manager_queue_url()
    return _abs_url(path)


def _finance_queue_url() -> str:
    return _abs_url(_safe_reverse("reimbursement:finance_pending"))


def _finance_verify_url(req: ReimbursementRequest) -> str:
    path = _safe_reverse("reimbursement:finance_verify", args=[req.id])
    if not path:
        return _finance_queue_url()
    return _abs_url(path)


def _request_detail_url(req: ReimbursementRequest) -> str:
    path = _safe_reverse("reimbursement:request_detail", args=[req.id])
    if not path:
        return _abs_url("/reimbursement/my/")
    return _abs_url(path)


# ---------------------------------------------------------------------------
# Final notification recipient helpers
# ---------------------------------------------------------------------------

def _final_to_list() -> List[str]:
    default_to = ["jyothi@gasteels.com", "chetan.shah@gasteels.com"]
    return list(getattr(settings, "REIMBURSEMENT_FINAL_TO", default_to))


def _final_cc_list() -> List[str]:
    default_cc = [
        "amreen@blueoceansteels.com",
        "vilas@blueoceansteels.com",
        "akshay@blueoceansteels.com",
        "sharyu@blueoceansteels.com",
    ]
    return list(getattr(settings, "REIMBURSEMENT_FINAL_CC", default_cc))


def _line_bill_url(line: ReimbursementLine) -> str:
    try:
        path = _safe_reverse("reimbursement:receipt_line", kwargs={"line_id": line.id})
        return _abs_url(path) if path else ""
    except Exception:
        return ""


def _eligible_final_lines_qs(req: ReimbursementRequest):
    """
    Bills eligible to move beyond Finance.

    Finance-rejected bills are dead for approval/final flow.
    Only Finance-approved included bills should be shown to approvers/final recipients.
    """
    L = ReimbursementLine
    return (
        req.lines.select_related("expense_item")
        .filter(
            status=L.Status.INCLUDED,
            bill_status=L.BillStatus.FINANCE_APPROVED,
        )
        .order_by("id")
    )


def _eligible_final_amount(req: ReimbursementRequest) -> Decimal:
    total = _eligible_final_lines_qs(req).aggregate(total=Sum("amount")).get("total")
    return total or Decimal("0.00")


def _lines_context(
    req: ReimbursementRequest,
    *,
    finance_approved_only: bool = False,
) -> List[Dict[str, str]]:
    """
    Build the lines dataset for email templates:
    category, date, description, amount, bill_url.

    Default behaviour keeps legacy full-included-line context.
    finance_approved_only=True is mandatory for approver/final emails.
    """
    out: List[Dict[str, str]] = []
    try:
        if finance_approved_only:
            qs = _eligible_final_lines_qs(req)
        else:
            qs = (
                req.lines.select_related("expense_item")
                .filter(status=ReimbursementLine.Status.INCLUDED)
                .order_by("id")
            )

        for line in qs:
            item = line.expense_item

            try:
                category = item.get_category_display()
            except Exception:
                category = getattr(item, "category", "") or ""

            try:
                date_str = item.date.strftime("%d %b %Y")
            except Exception:
                date_str = str(getattr(item, "date", "") or "")

            description = (
                line.description or getattr(item, "description", "") or ""
            ).strip() or "-"

            amt = line.amount if line.amount is not None else getattr(item, "amount", None)
            amount_str = _fmt_amount(amt)

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
        logger.exception(
            "Failed building lines for request #%s",
            getattr(req, "id", "?"),
        )
    return out


# ---------------------------------------------------------------------------
# Bill-specific notifications
# ---------------------------------------------------------------------------

def send_bill_rejected_by_finance(
    req: ReimbursementRequest,
    line: ReimbursementLine,
) -> None:
    subject = f"Reimbursement #{req.id}: One bill was rejected by Finance"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "rejection_reason": line.finance_rejection_reason or "-",
        "detail_url": _request_detail_url(req),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_bill_rejected_by_finance",
        ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} rejected by finance sent to employee.",
        extra={
            "line_id": line.id,
            "template": "reimbursement_bill_rejected_by_finance",
        },
    )


def send_bill_resubmitted(
    req: ReimbursementRequest,
    line: ReimbursementLine,
    *,
    actor,
) -> None:
    """
    Corrected-bill resubmission email.

    TO = Finance emails
    CC = Manager email + ReimbursementSettings.approver_cc_list()
    """
    settings_obj = ReimbursementSettings.get_solo()
    cc_raw = _manager_emails(req) + settings_obj.approver_cc_list()

    subject = f"Reimbursement #{req.id}: Employee resubmitted a corrected bill"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "resubmitted_by": _display_name(actor),
        "detail_url": _finance_queue_url(),
        "queue_url": _finance_queue_url(),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }

    to = _finance_emails()
    cc = _dedup(cc_raw) or []

    _send(
        to,
        subject,
        "reimbursement_bill_resubmitted",
        ctx,
        cc=cc,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=actor,
        message=f"Email: bill #{line.id} resubmitted sent to finance (cc={cc}).",
        extra={
            "line_id": line.id,
            "template": "reimbursement_bill_resubmitted",
            "to": to,
            "cc": cc,
        },
    )


def send_bill_rejected_by_manager(
    req: ReimbursementRequest,
    line: ReimbursementLine,
    *,
    reason: str = "",
) -> None:
    subject = f"Reimbursement #{req.id}: One bill was rejected by Manager"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "manager_name": _display_name(req.manager),
        "rejection_reason": reason or "-",
        "detail_url": _request_detail_url(req),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_bill_rejected_by_manager",
        ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} rejected by manager sent to employee.",
        extra={
            "line_id": line.id,
            "template": "reimbursement_bill_rejected_by_manager",
        },
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
        "detail_url": _request_detail_url(req),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_bill_paid",
        ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} paid sent to employee.",
        extra={
            "line_id": line.id,
            "template": "reimbursement_bill_paid",
        },
    )


# ---------------------------------------------------------------------------
# Request-level notifications
# ---------------------------------------------------------------------------

def send_reimbursement_finance_verify(
    req: ReimbursementRequest,
    *,
    employee_note: str = "",
) -> None:
    """
    Sent when an employee submits or resubmits a reimbursement.

    Finance verification email intentionally uses request total / all submitted bills,
    because Finance needs to review all included bills.
    """
    settings_obj = ReimbursementSettings.get_solo()
    verify_url = _finance_verify_url(req)
    queue_url = _finance_queue_url()
    detail_url = _request_detail_url(req)
    status_label = dict(ReimbursementRequest.Status.choices).get(req.status, req.status)

    finance_ctx = {
        "employee_name": _display_name(req.created_by),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "note": employee_note or "-",
        "queue_url": queue_url,
        "review_url": verify_url,
        "approve_url": verify_url,
        "reject_url": verify_url,
        "submitted_at": req.submitted_at,
    }

    _send(
        _finance_emails(),
        f"Reimbursement #{req.id}: Submitted for Finance Verification",
        "reimbursement_finance_verify",
        finance_ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: submitted to Finance Verification.",
        extra={"template": "reimbursement_finance_verify"},
    )

    notify_to = settings_obj.submitted_notify_to_list()
    notify_cc = settings_obj.submitted_notify_cc_list()

    if notify_to:
        submitted_ctx = {
            "employee_name": _display_name(req.created_by),
            "employee_email": getattr(req.created_by, "email", "") or "-",
            "request_id": req.id,
            "total_amount": _fmt_amount(req.total_amount),
            "status_label": status_label,
            "submitted_at": _fmt_dt(req.submitted_at) if req.submitted_at else "-",
            "employee_note": employee_note or "",
            "detail_url": detail_url,
            "queue_url": queue_url,
            "manager_to": ", ".join(notify_to),
            "admin_cc": notify_cc,
        }

        _send(
            notify_to,
            f"New Reimbursement Submitted — #{req.id} by {_display_name(req.created_by)}",
            "reimbursement_submitted",
            submitted_ctx,
            cc=notify_cc or None,
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.EMAIL_SENT,
            actor=None,
            message=f"Email: submission notification sent (to={notify_to}, cc={notify_cc}).",
            extra={
                "template": "reimbursement_submitted",
                "to": notify_to,
                "cc": notify_cc,
            },
        )


def send_reimbursement_finance_verified(req: ReimbursementRequest) -> None:
    """
    Finance-verified email to manager.

    IMPORTANT:
    Finance-rejected bills are dead for approval flow.
    This fallback email must use only Finance-approved bills and eligible amount.
    """
    eligible_lines = _eligible_final_lines_qs(req)
    eligible_count = eligible_lines.count()

    if eligible_count <= 0:
        logger.info(
            "Legacy finance-verified email suppressed: no Finance-approved bills for req #%s.",
            req.id,
        )
        return

    settings_obj = ReimbursementSettings.get_solo()
    cc = _dedup(settings_obj.approver_cc_list()) or []

    eligible_amount = _eligible_final_amount(req)

    subject = f"Reimbursement #{req.id}: Ready for your approval"
    queue_url = _manager_queue_url()
    review_url = _manager_review_url(req)

    ctx = {
        "manager_name": _display_name(req.manager),
        "employee_name": _display_name(req.created_by),
        "request_id": req.id,
        "total_amount": _fmt_amount(eligible_amount),
        "eligible_total_amount": _fmt_amount(eligible_amount),
        "eligible_bill_count": eligible_count,
        "queue_url": queue_url,
        "detail_url": _request_detail_url(req),
        "review_url": review_url,
        "approve_url": review_url,
        "reject_url": review_url,
        "lines": _lines_context(req, finance_approved_only=True),
    }

    to = _manager_emails(req)
    if not to:
        to = _finance_emails()

    _send(
        to,
        subject,
        "reimbursement_finance_verified",
        ctx,
        cc=cc,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: finance verified -> manager (to={to}, cc={cc}).",
        extra={
            "template": "reimbursement_finance_verified",
            "to": to,
            "cc": cc,
            "eligible_bill_count": eligible_count,
            "eligible_total_amount": str(eligible_amount),
        },
    )


def send_reimbursement_manager_action(
    req: ReimbursementRequest,
    *,
    decision: str,
) -> None:
    subject = f"Reimbursement #{req.id}: Manager decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "manager_name": _display_name(req.manager),
        "decision": decision,
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
        "detail_url": _request_detail_url(req),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_manager_action",
        ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: manager decision ({decision}) sent to employee.",
        extra={"template": "reimbursement_manager_action"},
    )


def send_reimbursement_management_action(
    req: ReimbursementRequest,
    *,
    decision: str,
) -> None:
    subject = f"Reimbursement #{req.id}: Management decision — {decision.capitalize()}"
    ctx = {
        "employee_name": _display_name(req.created_by),
        "management_name": _display_name(req.management),
        "decision": decision,
        "request_id": req.id,
        "total_amount": _fmt_amount(req.total_amount),
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
        "detail_url": _request_detail_url(req),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_management_action",
        ctx,
    )

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
        "payment_reference": req.finance_payment_reference or "-",
        "paid_at": req.paid_at,
        "detail_url": _request_detail_url(req),
    }

    _send(
        _employee_email(req),
        subject,
        "reimbursement_request_paid",
        ctx,
    )

    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message="Email: request marked paid.",
        extra={"template": "reimbursement_request_paid"},
    )


# ---------------------------------------------------------------------------
# Final notification after manager approval
# ---------------------------------------------------------------------------

def send_reimbursement_final_notification(req: ReimbursementRequest) -> None:
    """
    Final email after approval.

    TO: settings.REIMBURSEMENT_FINAL_TO
    CC: settings.REIMBURSEMENT_FINAL_CC

    IMPORTANT:
    Finance-rejected bills are dead for final approval flow.
    This fallback email must include only Finance-approved bills and eligible total.
    """
    try:
        eligible_lines = _eligible_final_lines_qs(req)
        eligible_count = eligible_lines.count()

        if eligible_count <= 0:
            logger.info(
                "Legacy final notification suppressed: no Finance-approved bills for req #%s.",
                req.id,
            )
            return

        eligible_amount = _eligible_final_amount(req)

        employee_name = _display_name(req.created_by)
        verified_by = _display_name(getattr(req, "verified_by", None)) or "Finance"
        approved_by = _display_name(getattr(req, "manager", None)) or "Approver"
        approved_at = getattr(req, "manager_decided_at", None) or timezone.now()

        ctx = {
            "employee_name": employee_name,
            "request_id": req.id,
            "total_amount": _fmt_amount(eligible_amount),
            "eligible_total_amount": _fmt_amount(eligible_amount),
            "eligible_bill_count": eligible_count,
            "verified_by": verified_by,
            "approved_by": approved_by,
            "approved_at": _fmt_dt(approved_at),
            "detail_url": _request_detail_url(req),
            "lines": _lines_context(req, finance_approved_only=True),
        }

        subject = f"Approved Reimbursement — {employee_name} — ₹{ctx['total_amount']}"
        to_list = _final_to_list()
        cc_list = _final_cc_list()

        _send(
            to_list,
            subject,
            "reimbursement_final_notification",
            ctx,
            cc=cc_list,
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.EMAIL_SENT,
            actor=None,
            message="Email: final notification after approval.",
            extra={
                "template": "reimbursement_final_notification",
                "to": to_list,
                "cc": cc_list,
                "eligible_bill_count": eligible_count,
                "eligible_total_amount": str(eligible_amount),
            },
        )
    except Exception:
        logger.exception(
            "Failed to send final notification for request #%s",
            getattr(req, "id", "?"),
        )