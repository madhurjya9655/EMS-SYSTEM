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
            "KAM batch approval email skipped because no recipients were found. subject=%r",
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
            "KAM batch approval email sent. subject=%r to=%s cc=%s sent_count=%s",
            subject,
            final_to,
            final_cc,
            sent_count,
        )

        return bool(sent_count)

    except Exception:
        logger.exception(
            "KAM batch approval email failed. subject=%r to=%s cc=%s",
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
# Public send function
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