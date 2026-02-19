# FILE: apps/kam/email.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.template.loader import render_to_string
from django.urls import reverse

User = get_user_model()


@dataclass(frozen=True)
class ApprovalLinkPayload:
    batch_id: int
    action: str  # "APPROVE" | "REJECT"


class VisitBatchApprovalSigner:
    """
    Timestamped signer for secure approval links.
    - Uses Django's TimestampSigner
    - Prevents tampering
    - Expires based on max_age_seconds
    """

    def __init__(
        self,
        *,
        salt: str = "kam.visitbatch.approval.v1",
        max_age_seconds: int = 60 * 60 * 24 * 7,  # 7 days
    ) -> None:
        self._signer = TimestampSigner(salt=salt)
        self.max_age_seconds = int(max_age_seconds)

    def make_token(self, batch_id: int, action: str) -> str:
        act = (action or "").strip().upper()
        if act not in {"APPROVE", "REJECT"}:
            raise ValueError("Invalid action for token")
        raw = f"{int(batch_id)}:{act}"
        return self._signer.sign(raw)

    def parse_token(self, token: str) -> ApprovalLinkPayload:
        value = self._signer.unsign(token, max_age=self.max_age_seconds)
        parts = (value or "").split(":", 1)
        if len(parts) != 2:
            raise BadSignature("Invalid token payload")
        batch_id = int(parts[0])
        action = (parts[1] or "").strip().upper()
        if action not in {"APPROVE", "REJECT"}:
            raise BadSignature("Invalid token action")
        return ApprovalLinkPayload(batch_id=batch_id, action=action)


def _safe_email_list(users: Sequence[User] | Iterable[User]) -> list[str]:
    emails: list[str] = []
    for u in users:
        e = (getattr(u, "email", "") or "").strip()
        if e:
            emails.append(e)
    return emails


def _send_email_message(*, subject: str, body: str, to: list[str], cc: Optional[list[str]] = None) -> None:
    """
    Best-effort mail sender.
    - HTML if body contains <html (simple heuristic)
    - fail_silently=True to avoid breaking workflow on email failure
    """
    if not to and not (cc or []):
        return

    msg = EmailMessage(
        subject=subject,
        body=body,
        to=to,
        cc=cc or [],
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or None,
    )
    if "<html" in (body or "").lower():
        msg.content_subtype = "html"

    try:
        msg.send(fail_silently=True)
    except Exception:
        # Never block production workflows on mail transport issues
        return


def build_batch_approval_urls(*, request, signer: VisitBatchApprovalSigner, batch_id: int) -> Tuple[str, str]:
    """
    Returns absolute URLs for:
      - approve link endpoint
      - reject link endpoint

    NOTE: URL names must exist in apps/kam/urls.py:
      - kam:visit_batch_approve_link
      - kam:visit_batch_reject_link
    """
    approve_token = signer.make_token(batch_id, "APPROVE")
    reject_token = signer.make_token(batch_id, "REJECT")

    approve_path = reverse("kam:visit_batch_approve_link", args=[approve_token])
    reject_path = reverse("kam:visit_batch_reject_link", args=[reject_token])

    approve_url = request.build_absolute_uri(approve_path)
    reject_url = request.build_absolute_uri(reject_path)
    return approve_url, reject_url


def send_visit_batch_approval_email(
    *,
    request,
    batch,
    kam_user: User,
    manager_user: User,
    customers: Sequence,
    remarks: str,
    visit_category_label: str,
    signer: Optional[VisitBatchApprovalSigner] = None,
    cc_users: Optional[Sequence[User]] = None,
    template_name: str = "kam/emails/visit_batch_approval.html",
) -> None:
    """
    Sends ONE consolidated approval email for a batch.

    Email includes:
      - Batch ID
      - KAM Name
      - Visit Category
      - Date Range
      - Remarks
      - Customers list
      - Secure Approve / Reject buttons (signed token links)
    """
    signer = signer or VisitBatchApprovalSigner()
    approve_url, reject_url = build_batch_approval_urls(request=request, signer=signer, batch_id=int(batch.id))

    subject = f"[KAM] Approval Required: Batch #{batch.id} ({batch.from_date}..{batch.to_date}) - {kam_user.username}"

    # Try HTML template first; fallback to plain text if template missing.
    html_body = ""
    try:
        html_body = render_to_string(
            template_name,
            {
                "batch": batch,
                "kam_user": kam_user,
                "manager_user": manager_user,
                "visit_category_label": visit_category_label,
                "date_range": f"{batch.from_date} → {batch.to_date}",
                "remarks": remarks,
                "customers": customers,
                "approve_url": approve_url,
                "reject_url": reject_url,
            },
        )
    except Exception:
        html_body = ""

    if html_body:
        body = html_body
    else:
        # Plain-text fallback (safe)
        cust_lines = []
        for i, c in enumerate(customers, start=1):
            name = getattr(c, "name", None) or str(c)
            cust_lines.append(f"{i}. {name}")

        body = (
            f"Batch ID: {batch.id}\n"
            f"KAM: {kam_user.get_full_name() or kam_user.username}\n"
            f"Category: {visit_category_label}\n"
            f"Date Range: {batch.from_date} to {batch.to_date}\n"
            f"Remarks:\n{remarks}\n\n"
            f"Customers:\n" + "\n".join(cust_lines) + "\n\n"
            f"Approve: {approve_url}\n"
            f"Reject:  {reject_url}\n"
        )

    to_emails = _safe_email_list([manager_user])
    cc_emails = _safe_email_list(cc_users or [])

    _send_email_message(subject=subject, body=body, to=to_emails, cc=cc_emails)


def validate_and_parse_token(
    *,
    token: str,
    signer: Optional[VisitBatchApprovalSigner] = None,
) -> Tuple[Optional[ApprovalLinkPayload], Optional[str]]:
    """
    Helper for views:
      - returns (payload, error_message)
      - does NOT raise; safe for direct use in GET endpoints
    """
    signer = signer or VisitBatchApprovalSigner()
    try:
        payload = signer.parse_token(token)
        return payload, None
    except SignatureExpired:
        return None, "Approval link expired."
    except (BadSignature, Exception):
        return None, "Invalid approval link."
