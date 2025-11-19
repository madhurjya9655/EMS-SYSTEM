from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlencode, urljoin

from django.conf import settings
from django.core import signing
        # NOTE: using EmailMultiAlternatives for HTML + attachments
from django.core.mail import EmailMultiAlternatives, get_connection
from django.urls import reverse
from django.utils import timezone
from django.contrib.auth import get_user_model

from apps.reimbursement.models import (
    ReimbursementRequest,
    ReimbursementSettings,
    ReimbursementLog,
    ReimbursementApproverMapping,
    ReimbursementLine,
)

logger = logging.getLogger(__name__)
User = get_user_model()

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _site_base() -> str:
    base = (
        getattr(settings, "SITE_URL", "")
        or getattr(settings, "SITE_BASE_URL", "")
        or "https://ems-system-d26q.onrender.com"
    ).strip()
    return base.rstrip("/") + "/"

def _abs_url(path: str) -> str:
    return urljoin(_site_base(), path.lstrip("/"))

def _email_enabled() -> bool:
    try:
        return bool(getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        return True

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

def _employee_display_name(user) -> str:
    try:
        return (getattr(user, "get_full_name", lambda: "")() or user.username or "").strip()
    except Exception:
        return (getattr(user, "username", "") or "").strip()

def _format_amount(amount: Decimal | None) -> str:
    try:
        if amount is None:
            return "0.00"
        return f"{amount:.2f}"
    except Exception:
        return str(amount or "0.00")

def _collect_receipt_files(req: ReimbursementRequest) -> List:
    files = []
    try:
        lines = req.lines.select_related("expense_item")
        seen_paths = set()
        for line in lines:
            f = line.receipt_file or getattr(line.expense_item, "receipt_file", None)
            if not f:
                continue
            path = getattr(f, "path", None)
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            files.append(f)
    except Exception:
        logger.exception("Failed to collect receipt files for reimbursement #%s", req.id)
    return files

def _amreen_from_email() -> str:
    """
    Canonical From header for reimbursement emails (Amreen).
    """
    return getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", "")

def _amreen_reply_to() -> List[str]:
    """
    Always provide a stable Reply-To for reimbursement emails (Amreen).
    """
    from_value = _amreen_from_email()
    if "<" in from_value and ">" in from_value:
        email = from_value[from_value.find("<")+1 : from_value.find(">")].strip()
    else:
        email = from_value.strip()
    return [email] if email else []

def _ensure_cc_amreen(cc: Iterable[str] | None) -> List[str]:
    """
    Ensure Amreen is included in CC where business rules require it (final mails).
    """
    cc_list = list(cc or [])
    am = _amreen_reply_to()
    if am:
        cc_list.extend(am)
    return _dedupe_lower(cc_list)

def _as_list(value: Union[str, Iterable[str], None]) -> List[str]:
    """
    Normalize incoming recipient(s) to a lowercase, deduped list.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return _dedupe_lower([value])
    try:
        return _dedupe_lower(list(value))
    except Exception:
        return []

def _send(
    subject: str,
    to_addrs: Union[str, Iterable[str]],
    *,
    cc: Optional[Iterable[str]] = None,
    reply_to: Optional[Iterable[str]] = None,
    html: str,
    txt: str,
    attachments: Optional[Iterable] = None,
    bcc: Optional[Iterable[str]] = None,
) -> bool:
    """
    Send an email and log success/failure with full context.
    Supports multiple recipients in TO/CC/BCC.
    Returns True on success, False on failure.
    """
    to_list = _as_list(to_addrs)
    cc_list = _as_list(cc)
    bcc_list = _as_list(bcc)
    reply_to_list = _as_list(reply_to) or _amreen_reply_to()

    if not to_list and not cc_list and not bcc_list:
        logger.warning(
            "Reimbursement email suppressed: no recipients. subject=%r", subject
        )
        return False

    from_email = _amreen_from_email()

    # Per requirement: do NOT fail silently for reimbursement emails.
    fail_silently = False

    try:
        backend_name = getattr(settings, "EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
        host = getattr(settings, "EMAIL_HOST", None)
        port = getattr(settings, "EMAIL_PORT", None)
        user = getattr(settings, "EMAIL_HOST_USER", None)
        use_tls = getattr(settings, "EMAIL_USE_TLS", None)
        use_ssl = getattr(settings, "EMAIL_USE_SSL", None)
        logger.info(
            "Reimbursement email attempt: backend=%s host=%s port=%s user=%s TLS=%s SSL=%s "
            "from=%s to=%s cc=%s bcc=%s reply_to=%s subject=%r",
            backend_name, host, port, user, use_tls, use_ssl,
            from_email, to_list, cc_list, bcc_list, reply_to_list, subject
        )
    except Exception:
        pass

    try:
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=txt,
                from_email=from_email,
                to=to_list or None,
                cc=cc_list or None,
                bcc=bcc_list or None,
                reply_to=reply_to_list or None,
                connection=conn,
            )
            msg.attach_alternative(html, "text/html")

            for f in attachments or []:
                try:
                    if hasattr(f, "path"):
                        msg.attach_file(f.path)
                    elif isinstance(f, tuple) and len(f) in (2, 3):
                        msg.attach(*f)
                except Exception:
                    logger.exception("Failed to attach file %r to email", getattr(f, "name", f))

            sent = msg.send(fail_silently=fail_silently)

        if sent:
            logger.info(
                "Reimbursement email sent OK: to=%s cc=%s bcc=%s subject=%r",
                to_list, cc_list, bcc_list, subject
            )
            return True

        logger.error(
            "Reimbursement email send returned 0: to=%s cc=%s bcc=%s subject=%r",
            to_list, cc_list, bcc_list, subject
        )
        return False
    except Exception as exc:
        logger.exception(
            "Reimbursement email send FAILED: to=%s cc=%s bcc=%s subject=%r error=%s",
            to_list, cc_list, bcc_list, subject, exc
        )
        return False

def _already_sent_recent(
    req: ReimbursementRequest,
    kind_hint: str,
    within_seconds: int = 90,
) -> bool:
    try:
        since = timezone.now() - timedelta(seconds=within_seconds)
        qs = ReimbursementLog.objects.filter(
            request=req,
            action=ReimbursementLog.Action.EMAIL_SENT,
            created_at__gte=since,
        )
        if kind_hint:
            qs = qs.filter(extra__kind=kind_hint)
        return qs.exists()
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Recipient resolution
# ---------------------------------------------------------------------------

@dataclass
class _Recipients:
    to: List[str]
    cc: List[str]

def _default_settings() -> ReimbursementSettings:
    return ReimbursementSettings.get_solo()

def _admin_emails() -> List[str]:
    try:
        return _default_settings().admin_email_list()
    except Exception:
        return []

def _finance_emails() -> List[str]:
    try:
        return _default_settings().finance_email_list()
    except Exception:
        return []

def _management_emails() -> List[str]:
    try:
        return _default_settings().management_email_list()
    except Exception:
        return []

def _manager_email_candidates(req: ReimbursementRequest) -> List[str]:
    candidates: List[str] = []
    try:
        settings_obj = _default_settings()
        # Global L1 approver first (if set)
        lvl1 = settings_obj.approver_level1()
        if lvl1:
            candidates.append(lvl1)

        # Request's assigned manager
        if req.manager and getattr(req.manager, "email", None):
            candidates.append((req.manager.email or "").strip())

        # Mapping fallback
        mapping = ReimbursementApproverMapping.for_employee(req.created_by)
        if mapping and mapping.manager and mapping.manager.email:
            candidates.append((mapping.manager.email or "").strip())

        # Profile/team_leader fallback (if available)
        profile = getattr(req.created_by, "profile", None)
        if profile and getattr(profile, "team_leader", None) and profile.team_leader.email:
            candidates.append((profile.team_leader.email or "").strip())
    except Exception:
        logger.exception("Failed resolving manager email candidates for reimbursement #%s", req.id)

    return _dedupe_lower(candidates)

def _recipients_for_manager(req: ReimbursementRequest) -> _Recipients:
    admin_cc = _admin_emails()
    mgr_list = _manager_email_candidates(req)
    return _Recipients(
        to=mgr_list,
        cc=_dedupe_lower(admin_cc),
    )

def _recipients_for_management(req: ReimbursementRequest) -> _Recipients:
    admin_cc = _admin_emails()
    mgmt_list: List[str] = []
    try:
        # Direct management user on the request
        if req.management and getattr(req.management, "email", None):
            mgmt_list.append((req.management.email or "").strip())
        # Plus settings' management_emails
        mgmt_list.extend(_management_emails())
    except Exception:
        logger.exception("Failed resolving management recipients for reimbursement #%s", req.id)

    return _Recipients(
        to=_dedupe_lower(mgmt_list),
        cc=_dedupe_lower(admin_cc),
    )

def _recipients_for_finance(req: ReimbursementRequest) -> _Recipients:
    admin_cc = _admin_emails()
    finance_list: List[str] = []
    try:
        mapping = ReimbursementApproverMapping.for_employee(req.created_by)
        if mapping and mapping.finance and mapping.finance.email:
            finance_list.append((mapping.finance.email or "").strip())
        finance_list.extend(_finance_emails())
    except Exception:
        logger.exception("Failed resolving finance recipients for reimbursement #%s", req.id)

    return _Recipients(
        to=_dedupe_lower(finance_list),
        cc=_dedupe_lower(admin_cc),
    )

def _employee_email(req: ReimbursementRequest) -> Optional[str]:
    try:
        return (req.created_by.email or "").strip() or None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Expense line details for emails
# ---------------------------------------------------------------------------

def _lines_for_email(req: ReimbursementRequest) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        lines = (
            req.lines.select_related("expense_item")
            .filter(status=ReimbursementLine.Status.INCLUDED)
            .order_by("id")
        )
        for line in lines:
            item = line.expense_item
            try:
                category = item.get_category_display()
            except Exception:
                category = getattr(item, "category", "") or ""
            try:
                date_str = item.date.strftime("%d %b %Y")
            except Exception:
                date_str = str(getattr(item, "date", "") or "")
            description = (line.description or getattr(item, "description", "") or "").strip() or "-"
            amount = f"₹{_format_amount(line.amount or getattr(item, 'amount', None))}"
            try:
                bill_url = _abs_url(reverse("reimbursement:receipt_line", args=[line.id]))
            except Exception:
                bill_url = ""
            rows.append(
                {
                    "category": category,
                    "date": date_str,
                    "description": description,
                    "amount": amount,
                    "bill_url": bill_url,
                }
            )
    except Exception:
        logger.exception("Failed building line details for reimbursement #%s", req.id)
    return rows

def _build_lines_table_html(req: ReimbursementRequest) -> str:
    rows = _lines_for_email(req)
    if not rows:
        return """
<p style="font-size:13px;color:#6b7280;margin:8px 0;">
  No expense lines attached to this reimbursement.
</p>
        """.strip()

    parts: List[str] = []
    parts.append(
        """
<table style="border-collapse:collapse;width:100%;font-size:13px;margin:8px 0;">
  <thead>
    <tr>
      <th style="border-bottom:1px solid #e5e7eb;padding:6px 4px;text-align:left;">Type of Expenses</th>
      <th style="border-bottom:1px solid #e5e7eb;padding:6px 4px;text-align:left;">Date of Expenses</th>
      <th style="border-bottom:1px solid #e5e7eb;padding:6px 4px;text-align:left;">Description</th>
      <th style="border-bottom:1px solid #e5e7eb;padding:6px 4px;text-align:right;">Amount</th>
      <th style="border-bottom:1px solid #e5e7eb;padding:6px 4px;text-align:left;">Bill Attachment</th>
    </tr>
  </thead>
  <tbody>
        """.strip()
    )
    for r in rows:
        if r["bill_url"]:
            bill_html = (
                f'<a href="{r["bill_url"]}" '
                'style="display:inline-block;padding:6px 10px;border-radius:6px;background:#eef2ff;color:#2563eb;text-decoration:none;">View</a>'
            )
        else:
            bill_html = "-"

        parts.append(
            f"""
    <tr>
      <td style="border-bottom:1px solid #f3f4f6;padding:6px 4px;">{r["category"]}</td>
      <td style="border-bottom:1px solid #f3f4f6;padding:6px 4px;">{r["date"]}</td>
      <td style="border-bottom:1px solid #f3f4f6;padding:6px 4px;">{r["description"]}</td>
      <td style="border-bottom:1px solid #f3f4f6;padding:6px 4px;text-align:right;">{r["amount"]}</td>
      <td style="border-bottom:1px solid #f3f4f6;padding:6px 4px;">{bill_html}</td>
    </tr>
            """.rstrip()
        )
    parts.append(
        """
  </tbody>
</table>
        """.strip()
    )
    return "\n".join(parts)

def _build_lines_table_text(req: ReimbursementRequest) -> str:
    rows = _lines_for_email(req)
    if not rows:
        return "No expense lines attached."

    header = "Type of Expenses | Date of Expenses | Description | Amount | Bill Attachment"
    sep = "-" * len(header)
    lines: List[str] = [header, sep]
    for r in rows:
        bill = f"View: {r['bill_url']}" if r["bill_url"] else "-"
        lines.append(
            f"{r['category']} | {r['date']} | {r['description']} | {r['amount']} | {bill}"
        )
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Email-action links (Approve / Reject from email)
# ---------------------------------------------------------------------------

_ACTION_SALT = "reimbursement-email-action"  # must match views.EMAIL_ACTION_SALT

def _build_action_token(req: ReimbursementRequest, role: str, decision: str) -> str:
    payload: Dict[str, object] = {
        "req_id": req.id,
        "role": role,
        "decision": decision,
    }
    return signing.dumps(payload, salt=_ACTION_SALT)

def _build_action_url(req: ReimbursementRequest, role: str, decision: str) -> str:
    token = _build_action_token(req, role, decision)
    try:
        path = reverse("reimbursement:email_action")
    except Exception:
        path = "/reimbursement/email-action/"
    qs = urlencode({"t": token})
    return _abs_url(f"{path}?{qs}")

def _manager_action_buttons(req: ReimbursementRequest) -> str:
    approve_url = _build_action_url(req, role="manager", decision="approved")
    reject_url = _build_action_url(req, role="manager", decision="rejected")
    return f"""
      <div style="margin:16px 0;">
        <a href="{approve_url}"
           style="display:inline-block;padding:10px 16px;margin-right:8px;
                  background-color:#16a34a;color:#ffffff;text-decoration:none;
                  border-radius:6px;font-weight:600;font-size:14px;">
          Approve
        </a>
        <a href="{reject_url}"
           style="display:inline-block;padding:10px 16px;
                  background-color:#dc2626;color:#ffffff;text-decoration:none;
                  border-radius:6px;font-weight:600;font-size:14px;">
          Reject
        </a>
      </div>
    """.strip()

def _management_action_buttons(req: ReimbursementRequest) -> str:
    approve_url = _build_action_url(req, role="management", decision="approved")
    reject_url = _build_action_url(req, role="management", decision="rejected")
    return f"""
      <div style="margin:16px 0;">
        <a href="{approve_url}"
           style="display:inline-block;padding:10px 16px;margin-right:8px;
                  background-color:#16a34a;color:#ffffff;text-decoration:none;
                  border-radius:6px;font-weight:600;font-size:14px;">
          Approve
        </a>
        <a href="{reject_url}"
           style="display:inline-block;padding:10px 16px;
                  background-color:#dc2626;color:#ffffff;text-decoration:none;
                  border-radius:6px;font-weight:600;font-size:14px;">
          Reject
        </a>
      </div>
    """.strip()

# ---------------------------------------------------------------------------
# Public API – finance-first flow (NEW)
# ---------------------------------------------------------------------------

def send_reimbursement_finance_verify(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    if not _email_enabled():
        logger.info("Emails disabled; skipping finance-verify email for #%s.", req.id)
        return

    kind = "finance_verify"
    if _already_sent_recent(req, kind):
        logger.info("Suppressing duplicate '%s' email for req #%s.", kind, req.id)
        return

    fin_rec = _recipients_for_finance(req)
    if not fin_rec.to:
        logger.warning("Finance verify email suppressed: no finance address for req #%s.", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Verify Reimbursement — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Verify Reimbursement — {emp_name} — ₹{amt_str}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID -</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{submitted_at}</td></tr>
      </table>
    """

    if employee_note:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Employee Note -</strong><br>
        {employee_note.replace("\n","<br>")}
      </p>
    """

    html += f"""
      <h3 style="font-size:15px;margin:16px 0 6px 0;">Expense Details</h3>
      {lines_html}

      <p style="font-size:14px;margin:16px 0 6px 0;">
        Open in BOS Lakshya:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Please verify the expenses and forward to the approver.
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Verify Reimbursement — {emp_name} — ₹{amt_str}",
            "",
            f"Request ID - #{req.id}",
            f"Employee Name - {emp_name}",
            f"Date of Submission - {submitted_at}",
            "",
            ("Employee Note:\n" + employee_note + "\n") if employee_note else "",
            "Expense Details:",
            lines_txt,
            "",
            "Open in BOS Lakshya:",
            detail_url,
        ]
    )

    # Replies go to employee if available; else to Amreen
    reply_to = [_employee_email(req)] if _employee_email(req) else _amreen_reply_to()
    attachments = _collect_receipt_files(req)

    _send(
        subject=subject,
        to_addrs=fin_rec.to,
        cc=fin_rec.cc,
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
    )

def send_reimbursement_finance_verified(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Emails disabled; skipping finance-verified email for #%s.", req.id)
        return

    kind = "finance_verified"
    if _already_sent_recent(req, kind):
        logger.info("Suppressing duplicate '%s' email for req #%s.", kind, req.id)
        return

    mgr_rec = _recipients_for_manager(req)
    if not mgr_rec.to:
        logger.warning("Finance verified email suppressed: no manager/L1 email for req #%s.", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Request For – {emp_name} – ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)
    buttons_html = _manager_action_buttons(req)
    cc_str = ", ".join(mgr_rec.cc) if mgr_rec.cc else ""
    to_str = ", ".join(mgr_rec.to)

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement Request For – {emp_name} – ₹{amt_str}
      </h2>

      <p style="margin:0 0 8px 0;">Finance has verified this request and forwarded it for approval.</p>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID -</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{submitted_at}</td></tr>
      </table>

      <h3 style="font-size:15px;margin:16px 0 6px 0;">Expense Details</h3>
      {lines_html}

      <p style="font-size:14px;margin:16px 0 6px 0;">
        You can also review this request in BOS Lakshya:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:14px;margin:16px 0 6px 0;">Quick actions (no login required):</p>
      {buttons_html}

      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This email was sent to: {to_str}
        {(" | CC: " + cc_str) if cc_str else ""}
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Reimbursement Request For – {emp_name} – ₹{amt_str}",
            "",
            "Finance has verified this request and forwarded it for approval.",
            "",
            f"Request ID - #{req.id}",
            f"Employee Name - {emp_name}",
            f"Date of Submission - {submitted_at}",
            "",
            "Expense Details:",
            lines_txt,
            "",
            "View in BOS Lakshya:",
            detail_url,
            "",
            "Quick actions (secure, no login):",
            f"Approve: {_build_action_url(req, 'manager', 'approved')}",
            f"Reject : {_build_action_url(req, 'manager', 'rejected')}",
            "",
            "Uploaded bills are attached.",
        ]
    )

    attachments = _collect_receipt_files(req)

    _send(
        subject=subject,
        to_addrs=mgr_rec.to,
        cc=mgr_rec.cc,
        reply_to=[],  # defaults to Amreen via _send
        html=html,
        txt=txt,
        attachments=attachments,
    )

def send_reimbursement_finance_rejected(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Emails disabled; skipping finance-rejected email for #%s.", req.id)
        return

    kind = "finance_rejected"
    if _already_sent_recent(req, kind):
        logger.info("Suppressing duplicate '%s' email for req #%s.", kind, req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Finance rejected email suppressed: employee missing email (req #%s).", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Rejected by Finance — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    note_html = (req.finance_note or "").replace("\n", "<br>") if req.finance_note else "-"

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Finance Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>Rejected</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
      </table>

      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Finance Note:</strong><br>{note_html}
      </p>

      <p style="font-size:14px;margin:12px 0;">
        View details:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Your reimbursement #{req.id} was rejected by Finance.",
            "",
            f"Total Amount: ₹{amt_str}",
            f"Reason/Note : {(req.finance_note or '').strip() or '-'}",
            "",
            "View request:",
            detail_url,
        ]
    )

    _send(
        subject=subject,
        to_addrs=[emp_email],
        cc=_admin_emails(),
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )

# ---------------------------------------------------------------------------
# Public API – legacy/compat (manager-first submit email kept)
# ---------------------------------------------------------------------------

def send_reimbursement_submitted(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping submitted email for #%s.", req.id)
        return

    if _already_sent_recent(req, kind_hint="submitted"):
        logger.info("Suppressing duplicate 'submitted' email for reimbursement #%s.", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Request For – {emp_name} – ₹{amt_str}"

    mgr_recip = _recipients_for_manager(req)
    if not mgr_recip.to:
        logger.warning(
            "Reimbursement submitted email suppressed: no manager/Level-1 email for req #%s.",
            req.id,
        )
        return

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    buttons_html = _manager_action_buttons(req)
    cc_str = ", ".join(mgr_recip.cc) if mgr_recip.cc else ""
    to_str = ", ".join(mgr_recip.to)
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement Request For – {emp_name} – ₹{amt_str}
      </h2>

      <p style="margin:0 0 8px 0;">New Reimbursement Request from {emp_name}</p>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID -</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{submitted_at}</td></tr>
      </table>
    """

    if employee_note:
        note_html = employee_note.replace("\n", "<br>")
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Employee Note -</strong><br>
        {note_html}
      </p>
    """

    html += """
      <h3 style="font-size:15px;margin:16px 0 6px 0;">Expense Details</h3>
    """ + lines_html + f"""
      <p style="font-size:14px;margin:16px 0 6px 0;">
        You can also review this request in BOS Lakshya:
        <br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:14px;margin:16px 0 6px 0;">
        Quick actions (no login required):
      </p>

      {buttons_html}

      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This email was sent to: {to_str}
        {(" | CC: " + cc_str) if cc_str else ""}
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Reimbursement Request For – {emp_name} – ₹{amt_str}",
        "",
        f"New Reimbursement Request from {emp_name}",
        "",
        f"Request ID - #{req.id}",
        f"Employee Name - {emp_name}",
        f"Date of Submission - {submitted_at}",
        "",
    ]
    if employee_note:
        txt_lines.extend(
            [
                "Employee Note -",
                employee_note,
                "",
            ]
        )
    lines_txt = _build_lines_table_text(req)
    txt_lines.extend(
        [
            "Expense Details:",
            lines_txt,
            "",
            "View in BOS Lakshya:",
            detail_url,
            "",
            "Quick actions (no login, via secure link):",
            f"Approve: {_build_action_url(req, 'manager', 'approved')}",
            f"Reject : {_build_action_url(req, 'manager', 'rejected')}",
            "",
            "Uploaded bills are attached to this email.",
        ]
    )
    txt = "\n".join(txt_lines)

    reply_to = [_employee_email(req)] if _employee_email(req) else _amreen_reply_to()
    attachments = _collect_receipt_files(req)

    _send(
        subject=subject,
        to_addrs=mgr_recip.to,
        cc=mgr_recip.cc,
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
    )

# ---------------------------------------------------------------------------
# Manager / management / finance follow-ups
# ---------------------------------------------------------------------------

def send_reimbursement_admin_summary(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping admin summary for #%s.", req.id)
        return

    admin_list = _admin_emails()
    if not admin_list:
        logger.info("No admin emails configured; skipping admin summary for #%s.", req.id)
        return

    if _already_sent_recent(req, kind_hint="admin_summary"):
        logger.info("Suppressing duplicate admin_summary email for reimbursement #%s.", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Submitted (Admin Summary) — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement Submitted (Admin Summary)
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Employee:</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Request ID:</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Status:</strong></td><td>{req.get_status_display()}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Submitted On:</strong></td><td>{submitted_at}</td></tr>
      </table>

      <p style="font-size:14px;margin:12px 0;">
        View request:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This summary was sent to: {", ".join(admin_list)}
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            "Reimbursement Submitted (Admin Summary)",
            "",
            f"Employee   : {emp_name}",
            f"Request ID : #{req.id}",
            f"Total Amt  : ₹{amt_str}",
            f"Status     : {req.get_status_display()}",
            f"Submitted  : {submitted_at}",
            "",
            "View request:",
            detail_url,
            "",
            f"Sent to: {', '.join(admin_list)}",
        ]
    )

    _send(
        subject=subject,
        to_addrs=admin_list,
        cc=[],
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )

def _send_level2_notification(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping level2 notification for #%s.", req.id)
        return

    kind = "level2_notify"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate Level-2 notification for reimbursement #%s.", req.id)
        return

    settings_obj = _default_settings()
    to_addr = settings_obj.approver_level2()
    if not to_addr:
        logger.info(
            "Level-2 notification suppressed: no global Level-2 email configured for req #%s.",
            req.id,
        )
        return

    cc_list = settings_obj.approver_cc_list()
    emp_email = _employee_email(req)
    if emp_email:
        cc_list.append(emp_email)
    cc_list = _dedupe_lower(cc_list)
    bcc_list = settings_obj.approver_bcc_list()

    # Ensure Amreen is CC'd on this final approval handoff
    cc_list = _ensure_cc_amreen(cc_list)

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    manager_name = _employee_display_name(req.manager) if req.manager else "Manager"
    subject = f"Approved Reimbursement Request For – {emp_name} – ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    approved_at = req.manager_decided_at or timezone.now()
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)
    manager_comment_html = (req.manager_comment or "").replace("\n", "<br>") if req.manager_comment else "-"

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Approved Reimbursement Request For – {emp_name} – ₹{amt_str}
      </h2>

      <p style="margin:0 0 8px 0;">
        Reimbursement Approved By {manager_name}<br>
        For {emp_name}
      </p>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{submitted_at}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Manager Comment -</strong></td><td>{manager_comment_html}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Approver Name -</strong></td><td>{manager_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date &amp; Time Approved -</strong></td><td>{approved_at}</td></tr>
      </table>

      <h3 style="font-size:15px;margin:16px 0 6px 0;">Expense Details</h3>
      {lines_html}

      <p style="font-size:14px;margin:16px 0 6px 0;">
        View full details in BOS Lakshya:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Please process the above expenses for settlement.
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Approved Reimbursement Request For – {emp_name} – ₹{amt_str}",
            "",
            f"Reimbursement Approved By {manager_name}",
            f"For {emp_name}",
            "",
            f"Employee Name - {emp_name}",
            f"Date of Submission - {submitted_at}",
            f"Manager Comment - {(req.manager_comment or '').strip() or '-'}",
            f"Approver Name - {manager_name}",
            f"Date & Time Approved - {approved_at}",
            "",
            "Expense Details:",
            lines_txt,
            "",
            "View details:",
            detail_url,
            "",
            "Please process the above expenses for settlement.",
        ]
    )

    attachments = _collect_receipt_files(req)

    _send(
        subject=subject,
        to_addrs=[to_addr],
        cc=cc_list,
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        bcc=_dedupe_lower(bcc_list),
        attachments=attachments,
    )

def send_reimbursement_manager_action(req: ReimbursementRequest, *, decision: str) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping manager-action email for #%s.", req.id)
        return

    kind = f"manager_{decision}"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate manager '%s' email for reimbursement #%s.", decision, req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Manager action email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    status_label = req.get_status_display()

    if decision == "approved":
        subject = f"Reimbursement Approved by Manager — {emp_name} — ₹{amt_str}"
        decision_label = "Approved"
    elif decision == "rejected":
        subject = f"Reimbursement Rejected by Manager — {emp_name} — ₹{amt_str}"
        decision_label = "Rejected"
    else:
        subject = f"Clarification Requested by Manager — {emp_name} — ₹{amt_str}"
        decision_label = "Clarification Required"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    manager_comment_html = (req.manager_comment or "").replace("\n", "<br>") if req.manager_comment else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Manager Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>{decision_label}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{status_label}</td></tr>
      </table>
    """

    if req.manager_comment:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Manager Comment:</strong><br>
        {manager_comment_html}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0;">
        View full details:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Manager decision for reimbursement #{req.id}",
        "",
        f"Decision      : {decision_label}",
        f"Total Amount  : ₹{amt_str}",
        f"Current Status: {status_label}",
        "",
    ]
    if req.manager_comment:
        txt_lines.extend(["Manager Comment:", req.manager_comment, ""])
    txt_lines.extend(["View details:", detail_url])
    txt = "\n".join(txt_lines)

    cc_list: List[str] = [] if decision == "rejected" else _admin_emails()

    _send(
        subject=subject,
        to_addrs=[emp_email],
        cc=cc_list,
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )

    if decision == "approved":
        _send_level2_notification(req)

def send_reimbursement_management_action(req: ReimbursementRequest, *, decision: str) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping management-action email for #%s.", req.id)
        return

    kind = f"management_{decision}"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info(
            "Suppressing duplicate management '%s' email for reimbursement #%s.",
            decision,
            req.id,
        )
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Management action email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    status_label = req.get_status_display()

    if decision == "approved":
        subject = f"Reimbursement Approved by Management — {emp_name} — ₹{amt_str}"
        decision_label = "Approved"
    elif decision == "rejected":
        subject = f"Reimbursement Rejected by Management — {emp_name} — ₹{amt_str}"
        decision_label = "Rejected"
    else:
        subject = f"Clarification Requested by Management — {emp_name} — ₹{amt_str}"
        decision_label = "Clarification Required"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    management_comment_html = (req.management_comment or "").replace("\n", "<br>") if req.management_comment else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Management Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>{decision_label}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{status_label}</td></tr>
      </table>
    """

    if req.management_comment:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Management Comment:</strong><br>
        {management_comment_html}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0;">
        View full details:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Management decision for reimbursement #{req.id}",
        "",
        f"Decision      : {decision_label}",
        f"Total Amount  : ₹{amt_str}",
        f"Current Status: {status_label}",
        "",
    ]
    if req.management_comment:
        txt_lines.extend(["Management Comment:", req.management_comment, ""])
    txt_lines.extend(["View details:", detail_url])
    txt = "\n".join(txt_lines)

    finance_rec = _recipients_for_finance(req)
    cc_raw: List[str] = []
    cc_raw.extend(finance_rec.to or [])
    cc_raw.extend(finance_rec.cc or [])
    cc_raw.extend(_admin_emails())
    cc = _dedupe_lower(cc_raw)

    _send(
        subject=subject,
        to_addrs=[emp_email],
        cc=cc,
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )

def send_reimbursement_paid(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping paid email for #%s.", req.id)
        return

    if _already_sent_recent(req, kind_hint="paid"):
        logger.info("Suppressing duplicate paid email for reimbursement #%s.", req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Paid email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Paid — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))
    paid_at = req.paid_at or timezone.now()

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement #{req.id} Paid
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Status:</strong></td><td>{req.get_status_display()}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Paid On:</strong></td><td>{paid_at}</td></tr>
    """
    if req.finance_payment_reference:
        html += f"""
        <tr><td style="padding-right:8px;"><strong>Payment Ref:</strong></td>
            <td>{req.finance_payment_reference}</td></tr>
    """
    html += f"""
      </table>

      <p style="font-size:14px;margin:12px 0;">
        View request:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Reimbursement #{req.id} has been marked Paid.",
        "",
        f"Total Amount: ₹{amt_str}",
        f"Status      : {req.get_status_display()}",
        f"Paid On     : {paid_at}",
    ]
    if req.finance_payment_reference:
        txt_lines.append(f"Payment Ref : {req.finance_payment_reference}")
    txt_lines.extend(["", "View request:", detail_url])
    txt = "\n".join(txt_lines)

    cc_list = _admin_emails()
    mgr_to = _manager_email_candidates(req)
    cc_list.extend(mgr_to)
    # Ensure Amreen is CC'd on the final paid email
    cc = _ensure_cc_amreen(cc_list)

    _send(
        subject=subject,
        to_addrs=[emp_email],
        cc=cc,
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )

def send_reimbursement_clarification(req: ReimbursementRequest, *, actor=None) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping clarification email for #%s.", req.id)
        return

    if _already_sent_recent(req, kind_hint="clarification"):
        logger.info("Suppressing duplicate clarification email for reimbursement #%s.", req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Clarification email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Clarification Needed — Reimbursement #{req.id} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:reimbursement_detail", args=[req.id]))

    try:
        if actor:
            who = _employee_display_name(actor)
        elif req.manager_decision == "clarification":
            who = "Manager"
        elif req.management_decision == "clarification":
            who = "Management"
        else:
            who = "Finance"
    except Exception:
        who = "Approver"

    clarification_msg = (
        (req.finance_note or "").strip()
        or (req.management_comment or "").strip()
        or (req.manager_comment or "").strip()
    )

    clarification_msg_html = clarification_msg.replace("\n", "<br>") if clarification_msg else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Clarification Needed – Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Requested By:</strong></td><td>{who}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{req.get_status_display()}</td></tr>
      </table>
    """

    if clarification_msg:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Clarification Details:</strong><br>
        {clarification_msg_html}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0;">
        Please review and update your request:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Clarification is required for your reimbursement #{req.id}.",
        "",
        f"Requested By : {who}",
        f"Total Amount : ₹{amt_str}",
        f"Current Status: {req.get_status_display()}",
        "",
    ]
    if clarification_msg:
        txt_lines.extend(
            [
                "Clarification Details:",
                clarification_msg,
                "",
            ]
        )
    txt_lines.extend(["Review / update:", detail_url])
    txt = "\n".join(txt_lines)

    _send(
        subject=subject,
        to_addrs=[emp_email],
        cc=_admin_emails(),
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
    )
