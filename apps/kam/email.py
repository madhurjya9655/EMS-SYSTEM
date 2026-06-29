# FILE: apps/kam/email.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.template.loader import render_to_string
from django.urls import NoReverseMatch, reverse
from django.utils.html import strip_tags

User = get_user_model()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApprovalLinkPayload:
    batch_id: int
    action: str  # APPROVE | REJECT


class VisitBatchApprovalSigner:
    """
    Timestamped signer for secure batch approval links.

    This signer:
    - prevents token tampering
    - stores action inside signed payload
    - supports expiry through TimestampSigner max_age
    """

    def __init__(
        self,
        *,
        salt: str = "kam.visitbatch.approval.v1",
        max_age_seconds: int = 60 * 60 * 24 * 7,
    ) -> None:
        self._signer = TimestampSigner(salt=salt)
        self.max_age_seconds = int(max_age_seconds)

    def make_token(self, batch_id: int, action: str) -> str:
        action_clean = (action or "").strip().upper()

        if action_clean not in {"APPROVE", "REJECT"}:
            raise ValueError("Invalid action for approval token")

        raw = f"{int(batch_id)}:{action_clean}"
        return self._signer.sign(raw)

    def parse_token(self, token: str) -> ApprovalLinkPayload:
        value = self._signer.unsign(
            token,
            max_age=self.max_age_seconds,
        )

        parts = (value or "").split(":", 1)

        if len(parts) != 2:
            raise BadSignature("Invalid token payload")

        batch_id = int(parts[0])
        action = (parts[1] or "").strip().upper()

        if action not in {"APPROVE", "REJECT"}:
            raise BadSignature("Invalid token action")

        return ApprovalLinkPayload(
            batch_id=batch_id,
            action=action,
        )


# ---------------------------------------------------------------------------
# Safe display helpers
# ---------------------------------------------------------------------------
def _safe_str(value, default: str = "-") -> str:
    if value is None:
        return default

    try:
        text = str(value).strip()
    except Exception:
        return default

    return text or default


def _display_user(user) -> str:
    if not user:
        return "-"

    try:
        full_name = (user.get_full_name() or "").strip()
        if full_name:
            return full_name
    except Exception:
        pass

    username = (getattr(user, "username", "") or "").strip()
    if username:
        return username

    email = (getattr(user, "email", "") or "").strip()
    if email:
        return email

    return "-"


def _display_email(user) -> str:
    if not user:
        return "-"

    email = (getattr(user, "email", "") or "").strip()
    return email or "-"


def _dedupe_email_list(emails: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen = set()

    for email in emails or []:
        clean = (email or "").strip()

        if not clean:
            continue

        key = clean.lower()

        if key in seen:
            continue

        seen.add(key)
        output.append(clean)

    return output


def _safe_email_list(users: Sequence[User] | Iterable[User]) -> list[str]:
    emails: list[str] = []

    for user in users or []:
        email = (getattr(user, "email", "") or "").strip()

        if email:
            emails.append(email)

    return _dedupe_email_list(emails)


def _display_date_range(start_value, end_value=None) -> str:
    start_text = _safe_str(start_value)
    end_text = _safe_str(end_value)

    if end_text != "-" and end_text != start_text:
        return f"{start_text} to {end_text}"

    return start_text


def _html_to_plain_text(html: str) -> str:
    text = strip_tags(html or "")
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines) or "BOS Lakshya ERP notification."


# ---------------------------------------------------------------------------
# Mail sender
# ---------------------------------------------------------------------------
def _send_email_message(
    *,
    subject: str,
    body: str,
    to: list[str],
    cc: Optional[list[str]] = None,
) -> bool:
    """
    Production-safe sender.

    Sends:
    - plain text body
    - HTML alternative when body is HTML

    This prevents raw HTML rendering issues and gives email clients a fallback.
    """
    to_emails = _dedupe_email_list(to or [])
    cc_emails = _dedupe_email_list(cc or [])

    to_keys = {email.lower() for email in to_emails}
    cc_emails = [
        email
        for email in cc_emails
        if email.lower() not in to_keys
    ]

    if not to_emails and not cc_emails:
        logger.warning(
            "KAM email skipped because no recipients were found. subject=%r",
            subject,
        )
        return False

    final_to = to_emails or cc_emails
    final_cc = cc_emails if to_emails else []

    from_email = (
        getattr(settings, "DEFAULT_FROM_EMAIL", None)
        or getattr(settings, "EMAIL_HOST_USER", None)
    )

    is_html = "<html" in (body or "").lower()
    plain_body = _html_to_plain_text(body) if is_html else (body or "BOS Lakshya ERP notification.")

    try:
        email = EmailMultiAlternatives(
            subject=subject,
            body=plain_body,
            from_email=from_email,
            to=final_to,
            cc=final_cc,
        )

        if is_html:
            email.attach_alternative(body, "text/html")

        sent_count = email.send(fail_silently=False)

        logger.info(
            "KAM email sent. subject=%r to=%s cc=%s sent_count=%s",
            subject,
            final_to,
            final_cc,
            sent_count,
        )

        return bool(sent_count)

    except Exception:
        logger.exception(
            "KAM email failed. subject=%r to=%s cc=%s",
            subject,
            final_to,
            final_cc,
        )
        return False


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------
def build_batch_approval_urls(
    *,
    request,
    signer: VisitBatchApprovalSigner,
    batch_id: int,
    prefer_direct_links: bool = True,
) -> Tuple[str, str]:
    """
    Returns absolute approve/reject URLs.

    Preferred:
    - kam:direct_batch_approve
    - kam:direct_batch_reject

    Fallback:
    - kam:visit_batch_approve_link
    - kam:visit_batch_reject_link
    """
    approve_token = signer.make_token(batch_id, "APPROVE")
    reject_token = signer.make_token(batch_id, "REJECT")

    if prefer_direct_links:
        try:
            approve_path = reverse("kam:direct_batch_approve", args=[approve_token])
            reject_path = reverse("kam:direct_batch_reject", args=[reject_token])

            return (
                request.build_absolute_uri(approve_path),
                request.build_absolute_uri(reject_path),
            )

        except NoReverseMatch:
            logger.warning(
                "Direct batch approval URLs not found. Falling back to login-required batch approval URLs."
            )

    approve_path = reverse("kam:visit_batch_approve_link", args=[approve_token])
    reject_path = reverse("kam:visit_batch_reject_link", args=[reject_token])

    return (
        request.build_absolute_uri(approve_path),
        request.build_absolute_uri(reject_path),
    )


# ---------------------------------------------------------------------------
# Email context builder
# ---------------------------------------------------------------------------
def _build_batch_line_rows(*, batch, customers: Sequence | None = None) -> list[dict]:
    """
    Builds visit line rows for the batch template.

    It first tries to read VisitPlan rows from DB.
    If unavailable, falls back to the customers sequence.
    """
    line_rows: list[dict] = []

    try:
        from apps.kam.models import VisitPlan

        plans = (
            VisitPlan.objects
            .select_related("customer")
            .filter(batch=batch)
            .order_by("id")
        )

        for plan in plans:
            if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
                entity = _safe_str(getattr(plan.customer, "name", None))
                location = _safe_str(
                    getattr(plan, "location", None)
                    or getattr(plan.customer, "address", None)
                )
            else:
                entity = _safe_str(getattr(plan, "counterparty_name", None))
                location = _safe_str(getattr(plan, "location", None))

            line_rows.append({
                "index": len(line_rows) + 1,
                "entity": entity,
                "date": _display_date_range(
                    getattr(plan, "visit_date", None),
                    getattr(plan, "visit_date_to", None),
                ),
                "location": location,
                "category": _safe_str(getattr(plan, "visit_category", None)),
                "purpose": _safe_str(getattr(plan, "purpose", None)),
            })

    except Exception:
        logger.exception(
            "Failed to build batch line rows from VisitPlan. batch_id=%s",
            getattr(batch, "id", None),
        )

    if not line_rows and customers:
        for customer in customers:
            line_rows.append({
                "index": len(line_rows) + 1,
                "entity": _safe_str(getattr(customer, "name", None)),
                "date": _display_date_range(
                    getattr(batch, "from_date", None),
                    getattr(batch, "to_date", None),
                ),
                "location": _safe_str(getattr(customer, "address", None)),
                "category": _safe_str(getattr(batch, "visit_category", None)),
                "purpose": _safe_str(getattr(batch, "purpose", None)),
            })

    return line_rows


def _build_safe_fallback_batch_html(
    *,
    batch,
    kam_user,
    manager_user,
    visit_category_label: str,
    date_range: str,
    remarks: str,
    approve_url: str,
    reject_url: str,
) -> str:
    """
    Structured fallback HTML.

    This avoids sending raw URL dump if the template render fails.
    """
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Batch Approval Required</title>
</head>
<body style="margin:0;padding:0;background:#f6f7f9;font-family:Arial,Helvetica,sans-serif;color:#111111;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f6f7f9;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background:#ffffff;border:1px solid #e6e8ec;border-radius:8px;overflow:hidden;">
          <tr>
            <td style="background:#0b1f3a;color:#ffffff;padding:18px 22px;font-size:18px;font-weight:700;">
              Batch Approval Required
            </td>
          </tr>
          <tr>
            <td style="padding:22px;font-size:14px;line-height:1.6;">
              <p style="margin:0 0 12px 0;">Hello <strong>{_display_user(manager_user)}</strong>,</p>
              <p style="margin:0 0 16px 0;">A visit batch has been submitted and requires your approval.</p>

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;border:1px solid #e6e8ec;">
                <tr>
                  <td style="width:35%;padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Batch ID</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">#{_safe_str(getattr(batch, "id", None))}</td>
                </tr>
                <tr>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Employee Name</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">{_display_user(kam_user)}</td>
                </tr>
                <tr>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Employee Email</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">{_display_email(kam_user)}</td>
                </tr>
                <tr>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Date</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">{date_range}</td>
                </tr>
                <tr>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Category</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">{visit_category_label}</td>
                </tr>
                <tr>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:700;">Remarks</td>
                  <td style="padding:9px 10px;border:1px solid #e6e8ec;">{remarks or "-"}</td>
                </tr>
              </table>

              <p style="margin:18px 0 0 0;">
                <a href="{approve_url}" style="display:inline-block;background:#0b5cab;color:#ffffff;text-decoration:none;padding:10px 16px;border-radius:4px;font-size:13px;font-weight:700;margin-right:8px;">Approve</a>
                <a href="{reject_url}" style="display:inline-block;background:#b42318;color:#ffffff;text-decoration:none;padding:10px 16px;border-radius:4px;font-size:13px;font-weight:700;">Reject</a>
              </p>

              <p style="margin:16px 0 0 0;font-size:12px;color:#667085;">
                System generated message. Please do not reply.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Public send function: visit batch approval
# ---------------------------------------------------------------------------
def send_visit_batch_approval_email(
    *,
    request,
    batch,
    kam_user: User,
    manager_user: User,
    customers: Sequence | None,
    remarks: str,
    visit_category_label: str,
    signer: Optional[VisitBatchApprovalSigner] = None,
    cc_users: Optional[Sequence[User]] = None,
    template_name: str = "kam/emails/visit_batch_approval.html",
    prefer_direct_links: bool = True,
) -> bool:
    """
    Sends one professional batch approval email.

    Email includes:
    - Batch ID
    - Employee Name
    - Employee Email
    - KAM Name
    - Date
    - Customer/entity summary
    - Location summary
    - Category
    - Visit lines
    - Approve / Reject buttons
    """
    signer = signer or VisitBatchApprovalSigner()

    approve_url, reject_url = build_batch_approval_urls(
        request=request,
        signer=signer,
        batch_id=int(batch.id),
        prefer_direct_links=prefer_direct_links,
    )

    customers = list(customers or [])
    cc_users = list(cc_users or [])

    cc_emails = _safe_email_list(cc_users)
    line_rows = _build_batch_line_rows(batch=batch, customers=customers)
    line_count = len(line_rows)

    if line_count == 1:
        customer_summary = line_rows[0].get("entity") or "-"
        location_summary = line_rows[0].get("location") or "-"
    elif line_count > 1:
        customer_summary = f"{line_count} customers/entities"
        location_summary = "Multiple - see visit lines"
    else:
        customer_summary = "-"
        location_summary = "-"

    date_range = _display_date_range(
        getattr(batch, "from_date", None),
        getattr(batch, "to_date", None),
    )

    subject = (
        f"[KAM] Approval Required: Batch #{batch.id} "
        f"({getattr(batch, 'from_date', '-') }..{getattr(batch, 'to_date', '-')}) - "
        f"{getattr(kam_user, 'username', '-')}"
    )

    context = {
        "batch": batch,
        "kam_user": kam_user,
        "manager_user": manager_user,
        "recipient_name": _display_user(manager_user),

        "employee_name": _display_user(kam_user),
        "employee_email": _display_email(kam_user),
        "kam_name": _display_user(kam_user),

        "visit_category_label": _safe_str(visit_category_label),
        "date_range": date_range,
        "remarks": _safe_str(remarks),
        "customers": customers,

        "customer_summary": customer_summary,
        "location_summary": location_summary,
        "line_rows": line_rows,
        "line_count": line_count,

        "approve_url": approve_url,
        "reject_url": reject_url,

        "cc_users": cc_users,
        "cc_emails": cc_emails,
    }

    try:
        html_body = render_to_string(template_name, context)

    except Exception:
        logger.exception(
            "Failed to render batch approval email template=%s batch_id=%s",
            template_name,
            getattr(batch, "id", None),
        )

        html_body = _build_safe_fallback_batch_html(
            batch=batch,
            kam_user=kam_user,
            manager_user=manager_user,
            visit_category_label=_safe_str(visit_category_label),
            date_range=date_range,
            remarks=_safe_str(remarks),
            approve_url=approve_url,
            reject_url=reject_url,
        )

    to_emails = _safe_email_list([manager_user])
    cc_email_list = _safe_email_list(cc_users)

    return _send_email_message(
        subject=subject,
        body=html_body,
        to=to_emails,
        cc=cc_email_list,
    )


# ---------------------------------------------------------------------------
# Public send function: monthly KAM performance report
# ---------------------------------------------------------------------------
def send_monthly_kam_performance_report_email(
    *,
    reporting_period: str,
    kam_sections: list[dict],
    summary_table: list[dict],
    management_summary: dict,
    chart_attachments: list[dict] | None = None,
    template_name: str = "kam/emails/monthly_kam_performance_report.html",
) -> bool:
    """
    Sends one consolidated monthly KAM Performance Report.

    Production requirements:
    - One email only.
    - Every KAM included.
    - TO: pankaj@blueoceansteels.com
    - CC: amreen@blueoceansteels.com
    - Professional HTML.
    - Company branding.
    - Embedded chart support via Content-ID.
    - No duplicate calculation logic here.

    chart_attachments expected format:
    [
        {
            "cid": "chart_akshay_sales",
            "filename": "chart_akshay_sales.png",
            "content": b"...png bytes...",
            "mimetype": "image/png",
        }
    ]
    """
    subject = f"BOS Lakshya Monthly KAM Performance Report - {reporting_period}"

    to_emails = ["pankaj@blueoceansteels.com"]
    cc_emails = ["amreen@blueoceansteels.com"]

    context = {
        "reporting_period": reporting_period,
        "kam_sections": kam_sections or [],
        "summary_table": summary_table or [],
        "management_summary": management_summary or {},
    }

    try:
        html_body = render_to_string(template_name, context)

    except Exception:
        logger.exception(
            "Failed to render monthly KAM performance email template=%s period=%s",
            template_name,
            reporting_period,
        )

        html_body = _build_safe_fallback_monthly_kam_report_html(
            reporting_period=reporting_period,
            kam_sections=kam_sections or [],
            summary_table=summary_table or [],
            management_summary=management_summary or {},
        )

    plain_body = _html_to_plain_text(html_body)

    from_email = (
        getattr(settings, "DEFAULT_FROM_EMAIL", None)
        or getattr(settings, "EMAIL_HOST_USER", None)
    )

    final_to = _dedupe_email_list(to_emails)
    final_cc = _dedupe_email_list(cc_emails)

    to_keys = {email.lower() for email in final_to}
    final_cc = [
        email
        for email in final_cc
        if email.lower() not in to_keys
    ]

    try:
        email = EmailMultiAlternatives(
            subject=subject,
            body=plain_body,
            from_email=from_email,
            to=final_to,
            cc=final_cc,
        )

        email.attach_alternative(html_body, "text/html")

        for attachment in chart_attachments or []:
            cid = (attachment.get("cid") or "").strip()
            filename = (attachment.get("filename") or "").strip()
            content = attachment.get("content")
            mimetype = (attachment.get("mimetype") or "image/png").strip()

            if not cid or not filename or not content:
                continue

            try:
                part = email.attach(filename, content, mimetype)
                part.add_header("Content-ID", f"<{cid}>")
                part.add_header("Content-Disposition", "inline", filename=filename)

            except Exception:
                logger.exception(
                    "Failed to attach inline monthly KAM chart image. cid=%s filename=%s",
                    cid,
                    filename,
                )

        sent_count = email.send(fail_silently=False)

        logger.info(
            "Monthly KAM performance report sent. period=%s to=%s cc=%s sent_count=%s kam_count=%s",
            reporting_period,
            final_to,
            final_cc,
            sent_count,
            len(kam_sections or []),
        )

        return bool(sent_count)

    except Exception:
        logger.exception(
            "Monthly KAM performance report email failed. period=%s to=%s cc=%s",
            reporting_period,
            final_to,
            final_cc,
        )
        return False


def _build_safe_fallback_monthly_kam_report_html(
    *,
    reporting_period: str,
    kam_sections: list[dict],
    summary_table: list[dict],
    management_summary: dict,
) -> str:
    """
    Minimal fallback HTML for the monthly KAM report.

    Used only when the main template fails to render.
    """
    summary_rows = ""

    for row in summary_table or []:
        summary_rows += f"""
        <tr>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;">{_safe_str(row.get("rank"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;">{_safe_str(row.get("kam"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;text-align:right;">{_safe_str(row.get("sales"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;text-align:right;">{_safe_str(row.get("visits"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;text-align:right;">{_safe_str(row.get("collections"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;text-align:right;">{_safe_str(row.get("target_pct"))}</td>
          <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;text-align:right;">{_safe_str(row.get("performance_pct"))}</td>
        </tr>
        """

    if not summary_rows:
        summary_rows = """
        <tr>
          <td colspan="7" style="border:1px solid #e5e7eb;padding:12px;font-size:12px;text-align:center;color:#6b7280;">
            No KAM performance records available.
          </td>
        </tr>
        """

    kam_blocks = ""

    for item in kam_sections or []:
        kam_blocks += f"""
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin:0 0 18px 0;border:1px solid #d1d5db;">
          <tr>
            <td style="background:#f9fafb;padding:12px 14px;border-bottom:1px solid #d1d5db;">
              <div style="font-size:15px;font-weight:700;color:#111827;">{_safe_str(item.get("name"))}</div>
              <div style="font-size:12px;color:#4b5563;margin-top:3px;">
                Designation: {_safe_str(item.get("designation"))} |
                Manager: {_safe_str(item.get("manager"))} |
                Reporting Period: {reporting_period}
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding:14px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Sales</strong><br>{_safe_str(item.get("sales"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Visits</strong><br>{_safe_str(item.get("visits"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Calls</strong><br>{_safe_str(item.get("calls"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Collections</strong><br>{_safe_str(item.get("collections"))}</td>
                </tr>
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Leads</strong><br>{_safe_str(item.get("leads"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Conversion</strong><br>{_safe_str(item.get("conversion"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Targets</strong><br>{_safe_str(item.get("targets"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Achievement</strong><br>{_safe_str(item.get("achievement_pct"))}</td>
                </tr>
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Overdues</strong><br>{_safe_str(item.get("overdues"))}</td>
                  <td style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Risk</strong><br>{_safe_str(item.get("risk"))}</td>
                  <td colspan="2" style="border:1px solid #e5e7eb;padding:8px;font-size:12px;"><strong>Performance</strong><br>{_safe_str(item.get("performance_pct"))}</td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
        """

    if not kam_blocks:
        kam_blocks = """
        <p style="font-size:13px;color:#6b7280;margin:0 0 18px 0;">
          No individual KAM sections were available for this reporting period.
        </p>
        """

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BOS Lakshya Monthly KAM Performance Report</title>
</head>
<body style="margin:0;padding:0;background:#f5f6f8;font-family:Arial,Helvetica,sans-serif;color:#111827;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f5f6f8;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:980px;background:#ffffff;border:1px solid #e5e7eb;border-collapse:collapse;">
          <tr>
            <td style="background:#0b1f3a;color:#ffffff;padding:20px 24px;">
              <div style="font-size:20px;font-weight:700;">Blue Ocean Steels</div>
              <div style="font-size:13px;margin-top:4px;">BOS Lakshya ERP System</div>
              <div style="font-size:16px;font-weight:700;margin-top:14px;">Monthly KAM Performance Report</div>
              <div style="font-size:13px;margin-top:4px;">Reporting Period: {reporting_period}</div>
            </td>
          </tr>
          <tr>
            <td style="padding:22px 24px;">
              <h2 style="font-size:16px;margin:0 0 10px 0;color:#111827;">Executive Summary</h2>
              <p style="font-size:14px;line-height:1.6;margin:0 0 16px 0;color:#374151;">
                This consolidated report presents monthly sales, visits, calls, collections,
                leads, target achievement, overdue risk, and overall KAM performance.
              </p>

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin-bottom:22px;">
                <thead>
                  <tr>
                    <th align="left" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Rank</th>
                    <th align="left" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">KAM</th>
                    <th align="right" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Sales</th>
                    <th align="right" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Visits</th>
                    <th align="right" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Collections</th>
                    <th align="right" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Target %</th>
                    <th align="right" style="border:1px solid #d1d5db;background:#f3f4f6;padding:8px;font-size:12px;">Performance %</th>
                  </tr>
                </thead>
                <tbody>
                  {summary_rows}
                </tbody>
              </table>

              {kam_blocks}

              <h2 style="font-size:16px;margin:0 0 10px 0;color:#111827;">Overall Management Summary</h2>

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin-bottom:16px;">
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;width:28%;background:#f9fafb;"><strong>Top Performer</strong></td>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;">{_safe_str(management_summary.get("top_performer"))}</td>
                </tr>
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;background:#f9fafb;"><strong>Needs Improvement</strong></td>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;">{_safe_str(management_summary.get("needs_improvement"))}</td>
                </tr>
                <tr>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;background:#f9fafb;"><strong>Recommendations</strong></td>
                  <td style="border:1px solid #e5e7eb;padding:10px;font-size:13px;line-height:1.55;">{_safe_str(management_summary.get("recommendations"))}</td>
                </tr>
              </table>

              <p style="font-size:11px;color:#6b7280;margin:18px 0 0 0;">
                This is a system generated report from BOS Lakshya ERP. Please do not reply to this email.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Token validation helper
# ---------------------------------------------------------------------------
def validate_and_parse_token(
    *,
    token: str,
    signer: Optional[VisitBatchApprovalSigner] = None,
) -> Tuple[Optional[ApprovalLinkPayload], Optional[str]]:
    """
    Parses approval token without raising.

    Returns:
    - payload, None on success
    - None, error_message on failure
    """
    signer = signer or VisitBatchApprovalSigner()

    try:
        payload = signer.parse_token(token)
        return payload, None

    except SignatureExpired:
        return None, "Approval link expired."

    except BadSignature:
        return None, "Invalid approval link."

    except Exception:
        logger.exception("Unexpected error while validating batch approval token")
        return None, "Invalid approval link."