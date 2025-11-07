from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlencode, urljoin

from django.conf import settings
from django.core import signing
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
    """
    Resolve the public base URL for links in emails.
    Priority:
      1) settings.SITE_URL
      2) settings.SITE_BASE_URL
      3) hard-coded production URL for this project
    """
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
    """
    Collect FieldFile objects for all receipts attached to this request.
    Used for attaching uploaded bills to emails.

    NOTE:
    Files are stored in the system via the model FileField; this function
    only collects their references for attaching to outgoing emails.
    """
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


def _send(
    subject: str,
    to_addr: str,
    cc: List[str],
    reply_to: List[str],
    html: str,
    txt: str,
    *,
    attachments: Optional[Iterable] = None,
    bcc: Optional[Iterable[str]] = None,
) -> bool:
    """
    Send an email and log success/failure with full context.
    Optionally attach uploaded bill files.
    Returns True on success, False on any failure.
    """
    if not to_addr:
        logger.warning(
            "Reimbursement email suppressed: empty TO address. subject=%r cc=%s bcc=%s",
            subject,
            cc,
            list(bcc or []),
        )
        return False

    from_email = (
        getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None)
        or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    )
    fail_silently = getattr(settings, "EMAIL_FAIL_SILENTLY", True)
    backend_name = getattr(
        settings,
        "EMAIL_BACKEND",
        "django.core.mail.backends.smtp.EmailBackend",
    )

    bcc_list = list(bcc or [])

    try:
        host = getattr(settings, "EMAIL_HOST", None)
        port = getattr(settings, "EMAIL_PORT", None)
        user = getattr(settings, "EMAIL_HOST_USER", None)
        use_tls = getattr(settings, "EMAIL_USE_TLS", None)
        use_ssl = getattr(settings, "EMAIL_USE_SSL", None)
        logger.info(
            "Reimbursement email attempt: backend=%s host=%s port=%s user=%s "
            "TLS=%s SSL=%s from=%s to=%s cc=%s bcc=%s reply_to=%s subject=%r "
            "fail_silently=%s",
            backend_name,
            host,
            port,
            user,
            use_tls,
            use_ssl,
            from_email,
            to_addr,
            cc,
            bcc_list,
            reply_to,
            subject,
            fail_silently,
        )
    except Exception:
        pass

    try:
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=txt,
                from_email=from_email,
                to=[to_addr],
                cc=cc or None,
                bcc=bcc_list or None,
                reply_to=reply_to or None,
                connection=conn,
            )
            msg.attach_alternative(html, "text/html")

            # Attach uploaded bills if provided
            for f in attachments or []:
                try:
                    if hasattr(f, "path"):
                        msg.attach_file(f.path)
                    elif isinstance(f, tuple) and len(f) in (2, 3):
                        # (filename, content [, mimetype])
                        msg.attach(*f)
                except Exception:
                    logger.exception("Failed to attach file %r to email", getattr(f, "name", f))

            sent = msg.send(fail_silently=fail_silently)

        if sent:
            logger.info(
                "Reimbursement email sent OK: to=%s cc=%s bcc=%s subject=%r",
                to_addr,
                cc,
                bcc_list,
                subject,
            )
            return True

        logger.error(
            "Reimbursement email send returned 0: to=%s cc=%s bcc=%s subject=%r",
            to_addr,
            cc,
            bcc_list,
            subject,
        )
        return False
    except Exception as exc:
        logger.exception(
            "Reimbursement email send FAILED: to=%s cc=%s bcc=%s subject=%r error=%s",
            to_addr,
            cc,
            bcc_list,
            subject,
            exc,
        )
        return False


def _already_sent_recent(
    req: ReimbursementRequest,
    kind_hint: str,
    within_seconds: int = 90,
) -> bool:
    """
    Light duplicate suppression using EMAIL_SENT logs.
    """
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
    to: Optional[str]
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


def _recipients_for_manager(req: ReimbursementRequest) -> _Recipients:
    """
    TO (priority):
      1) Global Level-1 approver from ReimbursementSettings (approver_level1_email)
      2) req.manager.email
      3) mapping.manager.email
      4) profile.team_leader.email

    CC:
      - Admin emails (if any)
    """
    admin_cc = _admin_emails()
    mgr_email = None

    try:
        settings_obj = _default_settings()
        # 1) Global Level-1 approver (admin-configurable)
        lvl1 = settings_obj.approver_level1()
        if lvl1:
            mgr_email = lvl1
        elif req.manager and req.manager.email:
            # 2) Manager on the request
            mgr_email = req.manager.email
        else:
            # 3) Mapping
            mapping = ReimbursementApproverMapping.for_employee(req.created_by)
            if mapping and mapping.manager and mapping.manager.email:
                mgr_email = mapping.manager.email
            else:
                # 4) Fallback to profile.team_leader
                profile = getattr(req.created_by, "profile", None)
                if profile and getattr(profile, "team_leader", None) and profile.team_leader.email:
                    mgr_email = profile.team_leader.email
    except Exception:
        logger.exception("Failed resolving manager recipients for reimbursement #%s", req.id)

    return _Recipients(
        to=(mgr_email or "").strip() or None,
        cc=_dedupe_lower(admin_cc),
    )


def _recipients_for_management(req: ReimbursementRequest) -> _Recipients:
    """
    TO:   management_emails[0] or req.management.email
    CC:   admin_emails
    """
    admin_cc = _admin_emails()
    mgmt_email = None

    try:
        if req.management and getattr(req.management, "email", None):
            mgmt_email = req.management.email
        else:
            mgmt_list = _management_emails()
            if mgmt_list:
                mgmt_email = mgmt_list[0]
    except Exception:
        logger.exception("Failed resolving management recipients for reimbursement #%s", req.id)

    return _Recipients(
        to=(mgmt_email or "").strip() or None,
        cc=_dedupe_lower(admin_cc),
    )


def _recipients_for_finance(req: ReimbursementRequest) -> _Recipients:
    """
    TO:   dedicated finance user (from mapping) or first finance email from settings
    CC:   admin_emails
    """
    admin_cc = _admin_emails()
    finance_email = None

    try:
        mapping = ReimbursementApproverMapping.for_employee(req.created_by)
        if mapping and mapping.finance and mapping.finance.email:
            finance_email = mapping.finance.email
        else:
            fin_list = _finance_emails()
            if fin_list:
                finance_email = fin_list[0]
    except Exception:
        logger.exception("Failed resolving finance recipients for reimbursement #%s", req.id)

    return _Recipients(
        to=(finance_email or "").strip() or None,
        cc=_dedupe_lower(admin_cc),
    )


def _employee_email(req: ReimbursementRequest) -> Optional[str]:
    try:
        return (req.created_by.email or "").strip() or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Email-action links (Approve / Reject from email)
# ---------------------------------------------------------------------------

_ACTION_SALT = "reimbursement-email-action"


def _build_action_token(req: ReimbursementRequest, role: str, decision: str) -> str:
    """
    Returns a signed token that we'll later validate in a dedicated view.
    role: 'manager' | 'management'
    decision: 'approved' | 'rejected'
    """
    payload: Dict[str, object] = {
        "req_id": req.id,
        "role": role,
        "decision": decision,
    }
    return signing.dumps(payload, salt=_ACTION_SALT)


def _build_action_url(req: ReimbursementRequest, role: str, decision: str) -> str:
    """
    URL that, when hit from the email button, will perform the action
    without requiring the user to log in (magic-link style).
    """
    token = _build_action_token(req, role, decision)
    try:
        path = reverse("reimbursement:email_action")
    except Exception:
        # fallback path if URL name not wired yet
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
      <div style="margin:16px 0%;">
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
# Public API – individual email flows
# ---------------------------------------------------------------------------

def send_reimbursement_submitted(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    """
    On submit:
      - Notify manager / Level-1 approver (TO; prefers global approver email).
      - CC admin summary.
      - Email includes Approve / Reject buttons for the manager.
      - Uploaded bills are attached to this email.
    """
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping submitted email for #%s.", req.id)
        return

    if _already_sent_recent(req, kind_hint="submitted"):
        logger.info("Suppressing duplicate 'submitted' email for reimbursement #%s.", req.id)
        return

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Request Submitted — {emp_name} — ₹{amt_str}"

    mgr_recip = _recipients_for_manager(req)
    if not mgr_recip.to:
        logger.warning(
            "Reimbursement submitted email suppressed: no manager/Level-1 email for req #%s.",
            req.id,
        )
        return

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )
    submitted_at = req.submitted_at or req.created_at
    buttons_html = _manager_action_buttons(req)
    cc_str = ", ".join(mgr_recip.cc) if mgr_recip.cc else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        New Reimbursement Request from {emp_name}
      </h2>

      <p style="margin:0 0 8px 0;">A new reimbursement request has been submitted.</p>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID:</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Status:</strong></td><td>{req.get_status_display()}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Submitted On:</strong></td><td>{submitted_at}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Email:</strong></td><td>{_employee_email(req) or "-"}</td></tr>
      </table>
    """

    if employee_note:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Employee Note:</strong><br>
        {employee_note.replace('\n', '<br>')}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0;">
        You can review full details in BOS Lakshya:
        <br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:14px;margin:16px 0 6px 0;">
        Or use the quick action buttons below (no login required):
      </p>

      {buttons_html}

      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This email was sent to: {mgr_recip.to}
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
        f"New reimbursement request from {emp_name}",
        "",
        f"Request ID: #{req.id}",
        f"Total Amount: ₹{amt_str}",
        f"Status: {req.get_status_display()}",
        f"Submitted On: {submitted_at}",
        f"Employee Email: {_employee_email(req) or '-'}",
        "",
    ]
    if employee_note:
        txt_lines.extend(
            [
                "Employee Note:",
                employee_note,
                "",
            ]
        )
    txt_lines.extend(
        [
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

    reply_to = [_employee_email(req)] if _employee_email(req) else []
    attachments = _collect_receipt_files(req)

    ok = _send(
        subject=subject,
        to_addr=mgr_recip.to,
        cc=mgr_recip.cc,
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                actor=req.created_by,
                message="Submitted notification sent.",
                extra={"kind": "submitted"},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (submitted) for reimbursement #%s", req.id)


def send_reimbursement_admin_summary(req: ReimbursementRequest) -> None:
    """
    Simple immediate summary to Admin emails when a request is submitted.
    (Separate from any daily digest.)
    """
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

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )
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

    to_addr = admin_list[0]
    cc = _dedupe_lower(admin_list[1:])

    ok = _send(
        subject=subject,
        to_addr=to_addr,
        cc=cc,
        reply_to=[],
        html=html,
        txt=txt,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                message="Admin summary email sent.",
                extra={"kind": "admin_summary"},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (admin_summary) for reimbursement #%s", req.id)


def _send_level2_notification(req: ReimbursementRequest) -> None:
    """
    After Level-1 (manager) approval:
      - Send mail to global Level-2 approver (approver_level2_email),
      - CC approver_cc_emails (plus the employee),
      - BCC approver_bcc_emails.

    All actual email addresses are configured in ReimbursementSettings.
    """
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
    # Always CC the employee who raised the reimbursement
    emp_email = _employee_email(req)
    if emp_email:
        cc_list.append(emp_email)
    cc_list = _dedupe_lower(cc_list)

    bcc_list = settings_obj.approver_bcc_list()

    emp_name = _employee_display_name(req.created_by)
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Approved (Level-2) — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement Approved by Level-1
      </h2>

      <p style="margin:0 0 8px 0;">
        A reimbursement request has been approved by the first approver and now requires your attention.
      </p>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID:</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee:</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{req.get_status_display()}</td></tr>
      </table>

      <p style="font-size:14px;margin:12px 0;">
        View full details in BOS Lakshya:<br>
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
            "Reimbursement approved by Level-1 approver.",
            "",
            f"Request ID : #{req.id}",
            f"Employee   : {emp_name}",
            f"Total Amt  : ₹{amt_str}",
            f"Status     : {req.get_status_display()}",
            "",
            "View details:",
            detail_url,
        ]
    )

    ok = _send(
        subject=subject,
        to_addr=to_addr,
        cc=cc_list,
        reply_to=[],
        html=html,
        txt=txt,
        bcc=_dedupe_lower(bcc_list),
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                message="Level-2 notification email sent.",
                extra={"kind": kind},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (level2_notify) for reimbursement #%s", req.id)


def send_reimbursement_manager_action(req: ReimbursementRequest, *, decision: str) -> None:
    """
    Notify employee after manager acts.
    decision: 'approved', 'rejected', 'clarification'

    Additionally:
      - If decision == 'approved', trigger Level-2 notification email using
        global approver_level2_email / approver_cc_emails / approver_bcc_emails.
    """
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

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )

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
        {req.manager_comment.replace('\n', '<br>')}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0%;">
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

    ok = _send(
        subject=subject,
        to_addr=emp_email,
        cc=_admin_emails(),
        reply_to=[],
        html=html,
        txt=txt,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                message=f"Manager decision email ({decision}) sent.",
                extra={"kind": kind},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (manager_%s) for reimbursement #%s", decision, req.id)

    # If manager approved, also notify Level-2 approver
    if decision == "approved":
        _send_level2_notification(req)


def send_reimbursement_management_action(req: ReimbursementRequest, *, decision: str) -> None:
    """
    Notify employee + finance/admin after management acts.
    decision: 'approved', 'rejected', 'clarification'
    """
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

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Management Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0%;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>{decision_label}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{status_label}</td></tr>
      </table>
    """

    if req.management_comment:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Management Comment:</strong><br>
        {req.management_comment.replace('\n', '<br>')}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0;">
        View full details:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563%;">
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
    cc = _dedupe_lower([*(finance_rec.cc or []), *_admin_emails()])

    ok = _send(
        subject=subject,
        to_addr=emp_email,
        cc=cc,
        reply_to=[],
        html=html,
        txt=txt,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                message=f"Management decision email ({decision}) sent.",
                extra={"kind": kind},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (management_%s) for reimbursement #%s", decision, req.id)


def send_reimbursement_paid(req: ReimbursementRequest) -> None:
    """
    Notify employee + admin that reimbursement has been marked Paid.
    """
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

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )

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
        <tr><td style="padding-right:8px;"><strong>Paid On:</strong></td><td>{req.paid_at or timezone.now()}</td></tr>
    """

    if req.finance_payment_reference:
        html += f"""
        <tr><td style="padding-right:8px;"><strong>Payment Ref:</strong></td>
            <td>{req.finance_payment_reference}</td></tr>
    """

    html += """
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
    """.format(detail_url=detail_url)

    txt_lines = [
        f"Reimbursement #{req.id} has been marked Paid.",
        "",
        f"Total Amount: ₹{amt_str}",
        f"Status      : {req.get_status_display()}",
        f"Paid On     : {req.paid_at or timezone.now()}",
    ]
    if req.finance_payment_reference:
        txt_lines.append(f"Payment Ref : {req.finance_payment_reference}")
    txt_lines.extend(["", "View request:", detail_url])
    txt = "\n".join(txt_lines)

    cc = _admin_emails()

    ok = _send(
        subject=subject,
        to_addr=emp_email,
        cc=cc,
        reply_to=[],
        html=html,
        txt=txt,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                message="Paid email sent.",
                extra={"kind": "paid"},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (paid) for reimbursement #%s", req.id)


def send_reimbursement_clarification(req: ReimbursementRequest, *, actor=None) -> None:
    """
    Notify employee that clarification is required (by manager/management/finance).
    """
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

    detail_url = _abs_url(
        reverse("reimbursement:reimbursement_detail", args=[req.id])
    )

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

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Clarification Needed – Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0%;">
        <tr><td style="padding-right:8px;"><strong>Requested By:</strong></td><td>{who}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{req.get_status_display()}</td></tr>
      </table>
    """

    if clarification_msg:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Clarification Details:</strong><br>
        {clarification_msg.replace('\n', '<br>')}
      </p>
    """

    html += f"""
      <p style="font-size:14px;margin:12px 0%;">
        Please review and update your request:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563%;">
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

    ok = _send(
        subject=subject,
        to_addr=emp_email,
        cc=_admin_emails(),
        reply_to=[],
        html=html,
        txt=txt,
    )
    if ok:
        try:
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.EMAIL_SENT,
                actor=actor,
                message="Clarification email sent.",
                extra={"kind": "clarification"},
            )
        except Exception:
            logger.exception("Failed to log EMAIL_SENT (clarification) for reimbursement #%s", req.id)
