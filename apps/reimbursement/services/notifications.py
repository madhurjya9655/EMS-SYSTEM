from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlencode, urljoin

from django.conf import settings
from django.core import signing
from django.core.mail import EmailMultiAlternatives, get_connection
from django.urls import reverse
from django.utils import timezone
from django.utils.html import escape

from apps.reimbursement.models import (
    ReimbursementApproverMapping,
    ReimbursementLine,
    ReimbursementLog,
    ReimbursementRequest,
    ReimbursementSettings,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Central recipient guard (config-driven; no hardcoding)
# If helper isn't available for any reason, fall back to no-op.
# ---------------------------------------------------------------------------
try:
    from apps.common.email_guard import filter_recipients_for_category  # type: ignore
except Exception:  # pragma: no cover
    def filter_recipients_for_category(*, category: str, to=None, cc=None, bcc=None, **_):
        return list(to or []), list(cc or []), list(bcc or [])


# ---------------------------------------------------------------------------
# Recipients – pulled from settings with safe fallbacks
# ---------------------------------------------------------------------------

def _as_cfg_list(value) -> List[str]:
    """
    Accept list/tuple or comma-separated string from settings and normalize to a list.
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(x).strip() for x in value if str(x).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]

# Finance verification stage — ONLY these (settings first; fallback to spec)
_FINANCE_TEAM = _as_cfg_list(getattr(settings, "REIMBURSEMENT_FINANCE_TEAM", None)) or [
    "akshay@blueoceansteels.com",
    "sharyu@blueoceansteels.com",
]

# Final (after approval) — TO + CC (settings first; fallback to spec)
_FINAL_TO = _as_cfg_list(getattr(settings, "REIMBURSEMENT_FINAL_TO", None)) or [
    "jyothi@gasteels.com",
    "chetan.shah@gasteels.com",
]
_FINAL_CC = _as_cfg_list(getattr(settings, "REIMBURSEMENT_FINAL_CC", None)) or [
    "amreen@blueoceansteels.com",
    "vilas@blueoceansteels.com",
    "akshay@blueoceansteels.com",
    "sharyu@blueoceansteels.com",
]

# Attachment cap (bytes) for outbound emails (default: 20 MB)
_MAX_ATTACH_TOTAL_BYTES = int(getattr(settings, "REIMBURSEMENT_EMAIL_ATTACHMENTS_MAX_BYTES", 20 * 1024 * 1024))

# De-duplication window for notification spam control (seconds)
_DUP_WINDOW_SECONDS = int(getattr(settings, "REIMBURSEMENT_EMAIL_DUP_WINDOW_SECONDS", 60))

# Should we attach receipts to manager-facing emails?
_ATTACH_TO_MANAGER = bool(getattr(settings, "REIMBURSEMENT_ATTACH_TO_MANAGER", False))

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _site_base() -> str:
    """
    Best-effort absolute base URL. Configure one of:
      - REIMBURSEMENT_SITE_BASE (preferred)
      - SITE_URL
      - APP_BASE_URL
      - SITE_BASE_URL
    Fallback to relative links in emails.
    """
    base = (
        getattr(settings, "REIMBURSEMENT_SITE_BASE", None)
        or getattr(settings, "SITE_URL", None)
        or getattr(settings, "APP_BASE_URL", None)
        or getattr(settings, "SITE_BASE_URL", None)
        or ""
    )
    base = (base or "").strip().rstrip("/")
    return (base + "/") if base else ""


def _abs_url(path: str) -> str:
    base = _site_base()
    if not base:
        # Ensure root-relative instead of relative-to-current
        return path if path.startswith("/") else ("/" + path)
    return urljoin(base, path.lstrip("/"))


def _email_enabled() -> bool:
    try:
        return bool(getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        return True


def _dedupe_preserve(emails: Iterable[str]) -> List[str]:
    """
    Dedupe emails case-insensitively but preserve original casing and order.
    """
    seen = set()
    out: List[str] = []
    for e in emails or []:
        if not e:
            continue
        raw = (e or "").strip()
        if not raw:
            continue
        key = raw.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def _employee_display_name(user) -> str:
    try:
        return (getattr(user, "get_full_name", lambda: "")() or getattr(user, "username", "") or "").strip()
    except Exception:
        return (getattr(user, "username", "") or "").strip()


def _format_amount(amount: Decimal | None) -> str:
    try:
        if amount is None:
            return "0.00"
        return f"{amount:,.2f}"
    except Exception:
        return str(amount or "0.00")


def _format_dt(dt) -> str:
    try:
        if not dt:
            return ""
        z = timezone.get_current_timezone()
        loc = timezone.localtime(dt, z)
        tzname = loc.tzname() or "IST"
        return loc.strftime(f"%d %b %Y, %H:%M {tzname}")
    except Exception:
        return str(dt)


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


def _collect_receipt_files_limited(req: ReimbursementRequest) -> List:
    selected = []
    total = 0
    for f in _collect_receipt_files(req):
        try:
            size = getattr(f, "size", None)
            if size is None and hasattr(f, "path"):
                import os
                size = os.path.getsize(f.path)
            size = int(size or 0)
        except Exception:
            size = 0

        if _MAX_ATTACH_TOTAL_BYTES and size > _MAX_ATTACH_TOTAL_BYTES:
            logger.warning("Skipping oversized attachment %r (size=%s)", getattr(f, "name", f), size)
            continue

        if _MAX_ATTACH_TOTAL_BYTES and (total + size) > _MAX_ATTACH_TOTAL_BYTES:
            logger.info("Attachment cap reached for req #%s (cap=%s bytes)", req.id, _MAX_ATTACH_TOTAL_BYTES)
            break

        selected.append(f)
        total += size
    return selected


def _amreen_from_email() -> str:
    return getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", "")


def _amreen_reply_to() -> List[str]:
    from_value = _amreen_from_email()
    if "<" in from_value and ">" in from_value:
        email = from_value[from_value.find("<")+1 : from_value.find(">")].strip()
    else:
        email = from_value.strip()
    return [email] if email else []


def _ensure_cc_amreen(cc: Iterable[str] | None) -> List[str]:
    cc_list = list(cc or [])
    am = _amreen_reply_to()
    if am:
        cc_list.extend(am)
    return _dedupe_preserve(cc_list)


def _as_list(value: Union[str, Iterable[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _dedupe_preserve([value])
    try:
        return _dedupe_preserve(list(value))
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
    extra_headers: Optional[Dict[str, str]] = None,
) -> bool:
    # Normalize lists
    to_list = _as_list(to_addrs)
    cc_list = _as_list(cc)
    bcc_list = _as_list(bcc)
    reply_to_list = _as_list(reply_to) or _amreen_reply_to()

    # Apply central recipient guard (category = reimbursement)
    try:
        filt_to, filt_cc, filt_bcc = filter_recipients_for_category(
            category="reimbursement",
            to=to_list,
            cc=cc_list,
            bcc=bcc_list,
        )
        to_list = _dedupe_preserve(filt_to)
        cc_list = _dedupe_preserve(filt_cc)
        bcc_list = _dedupe_preserve(filt_bcc)
    except Exception:
        # Fail open if guard is unavailable; we already have normalized lists
        pass

    if not to_list and not cc_list and not bcc_list:
        logger.warning("Reimbursement email suppressed: no recipients after guard. subject=%r", subject)
        return False

    from_email = _amreen_from_email()
    fail_silently = False

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
                headers=extra_headers or None,
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
            logger.info("Reimbursement email sent OK: to=%s cc=%s subject=%r", to_list, cc_list, subject)
            return True

        logger.error("Reimbursement email send returned 0: to=%s cc=%s subject=%r", to_list, cc_list, subject)
        return False
    except Exception as exc:
        logger.exception(
            "Reimbursement email send FAILED: to=%s cc=%s subject=%r error=%s",
            to_list, cc_list, subject, exc
        )
        return False


def _send_and_log(
    req: ReimbursementRequest,
    *,
    kind: str,
    subject: str,
    to_addrs: Union[str, Iterable[str]],
    cc: Optional[Iterable[str]] = None,
    reply_to: Optional[Iterable[str]] = None,
    html: str,
    txt: str,
    attachments: Optional[Iterable] = None,
    bcc: Optional[Iterable[str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> None:
    ok = _send(
        subject=subject,
        to_addrs=to_addrs,
        cc=cc,
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
        bcc=bcc,
        extra_headers=extra_headers,
    )
    try:
        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.EMAIL_SENT,
            actor=None,
            message="Email sent" if ok else "Email attempt",
            from_status=req.status,
            to_status=req.status,
            extra={
                "kind": kind,
                "subject": subject,
                "to": _as_list(to_addrs),
                "cc": _as_list(cc),
                "bcc": _as_list(bcc),
                "ok": bool(ok),
            },
        )
    except Exception:
        logger.exception("Failed to write EMAIL_SENT log for req #%s kind=%s", req.id, kind)


def _already_sent_recent(
    req: ReimbursementRequest,
    kind_hint: str,
    within_seconds: Optional[int] = None,
) -> bool:
    """
    Prevents duplicate emails in a short window.
    The window defaults to REIMBURSEMENT_EMAIL_DUP_WINDOW_SECONDS (60s) if not provided.
    """
    try:
        window = int(within_seconds if within_seconds is not None else _DUP_WINDOW_SECONDS)
    except Exception:
        window = _DUP_WINDOW_SECONDS
    try:
        since = timezone.now() - timedelta(seconds=window)
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
    # Kept for compatibility, but we will NOT use admins in any CC lists per new policy.
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
        lvl1 = settings_obj.approver_level1()
        if lvl1:
            candidates.append(lvl1)

        if req.manager and getattr(req.manager, "email", None):
            candidates.append((req.manager.email or "").strip())

        mapping = ReimbursementApproverMapping.for_employee(req.created_by)
        if mapping and mapping.manager and mapping.manager.email:
            candidates.append((mapping.manager.email or "").strip())

        profile = getattr(req.created_by, "profile", None)
        if profile and getattr(profile, "team_leader", None) and profile.team_leader.email:
            candidates.append((profile.team_leader.email or "").strip())
    except Exception:
        logger.exception("Failed resolving manager email candidates for reimbursement #%s", req.id)

    return _dedupe_preserve(candidates)


def _recipients_for_manager(req: ReimbursementRequest) -> _Recipients:
    # NEW POLICY: Do not include admins here.
    mgr_list = _manager_email_candidates(req)
    return _Recipients(to=mgr_list, cc=[])


def _recipients_for_management(req: ReimbursementRequest) -> _Recipients:
    # NEW POLICY: Do not include admins here.
    mgmt_list: List[str] = []
    try:
        if req.management and getattr(req.management, "email", None):
            mgmt_list.append((req.management.email or "").strip())
        mgmt_list.extend(_management_emails())
    except Exception:
        logger.exception("Failed resolving management recipients for reimbursement #%s", req.id)

    return _Recipients(to=_dedupe_preserve(mgmt_list), cc=[])


def _recipients_for_finance(req: ReimbursementRequest) -> _Recipients:
    # NEW POLICY: Do not include admins here.
    finance_list: List[str] = []
    try:
        mapping = ReimbursementApproverMapping.for_employee(req.created_by)
        if mapping and mapping.finance and mapping.finance.email:
            finance_list.append((mapping.finance.email or "").strip())
        finance_list.extend(_finance_emails())
    except Exception:
        logger.exception("Failed resolving finance recipients for reimbursement #%s", req.id)

    return _Recipients(to=_dedupe_preserve(finance_list), cc=[])


def _recipients_for_finance_enforced() -> _Recipients:
    # Verified “concerned” recipients only
    return _Recipients(to=_dedupe_preserve(_FINANCE_TEAM), cc=[])


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
                    "category": escape(category),
                    "date": escape(date_str),
                    "description": escape(description),
                    "amount": escape(amount),
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
        lines.append(f"{r['category']} | {r['date']} | {r['description']} | {r['amount']} | {bill}")
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
        "ts": timezone.now().timestamp(),  # consumer should enforce TTL
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
# Public API – finance-first flow (NEW)
# ---------------------------------------------------------------------------

def send_reimbursement_finance_verify(req: ReimbursementRequest, *, employee_note: str = "") -> None:
    """
    STEP 1 — Send ONLY to Finance team (Akshay & Sharyu). No CC.
    """
    if not _email_enabled():
        logger.info("Emails disabled; skipping finance-verify email for #%s.", req.id)
        return

    kind = "finance_verify"
    if _already_sent_recent(req, kind):
        logger.info("Suppressing duplicate '%s' email for req #%s.", kind, req.id)
        return

    fin_rec = _recipients_for_finance_enforced()
    if not fin_rec.to:
        logger.warning("Finance verify email suppressed: no finance address for req #%s.", req.id)
        return

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Verify Reimbursement — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)
    emp_note_html = escape(employee_note).replace("\n", "<br>") if employee_note else ""

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
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{_format_dt(submitted_at)}</td></tr>
      </table>
    """

    if employee_note:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Employee Note -</strong><br>
        {emp_note_html}
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
            f"Verify Reimbursement — {_employee_display_name(req.created_by)} — ₹{amt_str}",
            "",
            f"Request ID - #{req.id}",
            f"Employee Name - {_employee_display_name(req.created_by)}",
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

    reply_to = [_employee_email(req)] if _employee_email(req) else _amreen_reply_to()
    attachments = _collect_receipt_files_limited(req)

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=fin_rec.to,
        cc=[],  # no admins
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "finance_verify"},
    )


def send_reimbursement_finance_verified(req: ReimbursementRequest) -> None:
    """
    STEP 2 — Finance verified → email Approver(s).
    CC MUST include ONLY the verifier (Akshay or Sharyu), not both.
    Include verification timestamp, verified-by, and approver names.
    """
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

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement #{req.id} Verified – Pending Manager Approval"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    verified_at = req.verified_at or timezone.now()
    verifier_name = _employee_display_name(req.verified_by) if req.verified_by else "Finance"
    approver_list_str = ", ".join(mgr_rec.to)

    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)
    buttons_html = _manager_action_buttons(req)

    verifier_email = ((req.verified_by.email or "").strip()
                      if (req.verified_by and getattr(req.verified_by, "email", None)) else "")
    cc_list = _dedupe_preserve([verifier_email] if verifier_email else [])

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
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{_format_dt(submitted_at)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Verified By -</strong></td><td>{escape(verifier_name)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Verification Timestamp -</strong></td><td>{_format_dt(verified_at)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Approver(s) -</strong></td><td>{escape(approver_list_str)}</td></tr>
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
        This email was sent to: {escape(approver_list_str)}
        {(" | CC: " + ", ".join(cc_list)) if cc_list else ""}
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
            f"Reimbursement Request For – {_employee_display_name(req.created_by)} – ₹{amt_str}",
            "",
            "Finance has verified this request and forwarded it for approval.",
            "",
            f"Request ID - #{req.id}",
            f"Employee Name - {_employee_display_name(req.created_by)}",
            f"Date of Submission - {submitted_at}",
            f"Verified By - {verifier_name}",
            f"Verification Timestamp - {verified_at}",
            f"Approver(s) - {approver_list_str}",
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
        ]
    )

    # *** PERMANENT FIX ***
    # Manager-facing email: do NOT attach receipts unless explicitly enabled via settings.
    attachments = _collect_receipt_files_limited(req) if _ATTACH_TO_MANAGER else []

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=mgr_rec.to,
        cc=cc_list,          # ONLY verifier (concerned)
        reply_to=[],         # defaults to Amreen via _send
        html=html,
        txt=txt,
        attachments=attachments,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "finance_verified"},
    )


def send_reimbursement_finance_rejected(req: ReimbursementRequest) -> None:
    if not _email_enabled():
        logger.info("Emails disabled; skipping finance-rejected email for #%s.", req.id)
        return

    kind = "finance_rejected"
    if _already_sent_recent(req, kind):
        logger.info("Suppressing duplicate '%s' email for req #%s.", req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Finance rejected email suppressed: employee missing email (req #%s).", req.id)
        return

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Rejected by Finance — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    note_html = escape((req.finance_note or "")).replace("\n", "<br>") if req.finance_note else "-"

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

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=[emp_email],
        cc=[],  # remove admins
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "finance_rejected"},
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

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Request For – {emp_name} – ₹{amt_str}"

    mgr_recip = _recipients_for_manager(req)
    if not mgr_recip.to:
        logger.warning("Reimbursement submitted email suppressed: no manager/Level-1 email for req #%s.", req.id)
        return

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    submitted_at = req.submitted_at or req.created_at
    buttons_html = _manager_action_buttons(req)
    to_str = ", ".join(mgr_recip.to)
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)
    note_html = escape(employee_note).replace("\n", "<br>") if employee_note else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Reimbursement Request For – {emp_name} – ₹{amt_str}
      </h2>

      <p style="margin:0 0 8px 0;">New Reimbursement Request from {emp_name}</p>

      <table style="font-size:14px;margin:8px 0 16px 0%;">
        <tr><td style="padding-right:8px;"><strong>Request ID -</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Date of Submission -</strong></td><td>{_format_dt(submitted_at)}</td></tr>
      </table>
    """

    if employee_note:
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
        This email was sent to: {escape(to_str)}
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Regards,<br>BOS Lakshya
      </p>
    </div>
  </body>
</html>
    """

    txt_lines = [
        f"Reimbursement Request For – {_employee_display_name(req.created_by)} – ₹{amt_str}",
        "",
        f"New Reimbursement Request from {_employee_display_name(req.created_by)}",
        "",
        f"Request ID - #{req.id}",
        f"Employee Name - {_employee_display_name(req.created_by)}",
        f"Date of Submission - {submitted_at}",
        "",
    ]
    if employee_note:
        txt_lines.extend(["Employee Note -", employee_note, ""])
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
    # KEEP attachments in the "submitted" email (historical workflow ok)
    attachments = _collect_receipt_files_limited(req)

    _send_and_log(
        req,
        kind="submitted",
        subject=subject,
        to_addrs=mgr_recip.to,
        cc=[],  # DO NOT CC admins anymore
        reply_to=reply_to,
        html=html,
        txt=txt,
        attachments=attachments,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "submitted"},
    )

# ---------------------------------------------------------------------------
# Manager / management / finance follow-ups
# ---------------------------------------------------------------------------

def send_reimbursement_admin_summary(req: ReimbursementRequest) -> None:
    # Keeping this function as-is for optional admin reporting, but it is not used
    # by the main workflow. If you want it completely disabled, remove its calls.
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

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Submitted (Admin Summary) — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
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
        <tr><td style="padding-right:8px;"><strong>Status:</strong></td><td>{escape(req.get_status_display())}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Submitted On:</strong></td><td>{_format_dt(submitted_at)}</td></tr>
      </table>

      <p style="font-size:14px;margin:12px 0%;">
        View request:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This summary was sent to: {", ".join([escape(x) for x in admin_list])}
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563%;">
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
            f"Employee   : {_employee_display_name(req.created_by)}",
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

    _send_and_log(
        req,
        kind="admin_summary",
        subject=subject,
        to_addrs=admin_list,
        cc=[],  # never leak admins to other flows
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "admin_summary"},
    )


def send_reimbursement_final_notification(req: ReimbursementRequest) -> None:
    """
    STEP 3 — Final email after approval.
    TO:  Jyothi & Chetan
    CC:  Amreen, Vilas, Akshay, Sharyu
    Body includes: Bill Amount, Employee Name, Verified By, Approved By, Approval Timestamp.
    """
    if not _email_enabled():
        logger.info("Emails disabled; skipping final-notification email for #%s.", req.id)
        return

    kind = "final_notification"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate final notification for reimbursement #%s.", req.id)
        return

    to_list = _dedupe_preserve(_FINAL_TO)
    if not to_list:
        logger.warning("Final notification suppressed: no TO list configured for req #%s.", req.id)
        return
    cc_list = _ensure_cc_amreen(_FINAL_CC)

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    approved_by = _employee_display_name(req.manager) if req.manager else "Approver"
    approved_at = req.manager_decided_at or timezone.now()
    verified_by = _employee_display_name(req.verified_by) if req.verified_by else "Finance"

    subject = f"Approved Reimbursement — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    lines_html = _build_lines_table_html(req)
    lines_txt = _build_lines_table_text(req)

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Approved Reimbursement — {emp_name} — ₹{amt_str}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID -</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Name -</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Verified By -</strong></td><td>{escape(verified_by)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Approved By -</strong></td><td>{escape(approved_by)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Approval Timestamp -</strong></td><td>{_format_dt(approved_at)}</td></tr>
      </table>

      <h3 style="font-size:15px;margin:16px 0 6px 0;">Expense Details</h3>
      {lines_html}

      <p style="font-size:14px;margin:16px 0 6px 0;">
        View full details in BOS Lakshya:<br>
        <a href="{detail_url}" style="color:#2563eb;text-decoration:none;">{detail_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        This is an automated confirmation after approval.
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Approved Reimbursement — {_employee_display_name(req.created_by)} — ₹{amt_str}",
            "",
            f"Request ID        : #{req.id}",
            f"Employee Name     : {_employee_display_name(req.created_by)}",
            f"Verified By       : {verified_by}",
            f"Approved By       : {approved_by}",
            f"Approval Timestamp: {approved_at}",
            "",
            "Expense Details:",
            lines_txt,
            "",
            "View details:",
            detail_url,
        ]
    )

    attachments = _collect_receipt_files_limited(req)

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=to_list,
        cc=cc_list,
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        attachments=attachments,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "final_notification"},
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

    emp_name = escape(_employee_display_name(req.created_by))
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

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    manager_comment_html = escape(req.manager_comment or "").replace("\n", "<br>") if req.manager_comment else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Manager Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>{escape(decision_label)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{escape(status_label)}</td></tr>
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
        txt_lines.extend(["Manager Comment:", (req.manager_comment or ""), ""])
    txt_lines.extend(["View details:", detail_url])
    txt = "\n".join(txt_lines)

    # NEW POLICY: No admin CCs here; only the employee is notified.
    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=[emp_email],
        cc=[],  # remove admins
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "manager_action"},
    )

    if decision == "approved":
        send_reimbursement_final_notification(req)


def send_reimbursement_management_action(req: ReimbursementRequest, *, decision: str) -> None:
    if not _email_enabled():
        logger.info("Reimbursement emails disabled; skipping management-action email for #%s.", req.id)
        return

    kind = f"management_{decision}"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate management '%s' email for reimbursement #%s.", decision, req.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Management action email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = escape(_employee_display_name(req.created_by))
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

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
    management_comment_html = escape(req.management_comment or "").replace("\n", "<br>") if req.management_comment else ""

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;margin-bottom:12px;color:#111827;">
        Management Decision for Reimbursement #{req.id}
      </h2>

      <table style="font-size:14px;margin:8px 0 16px 0%;">
        <tr><td style="padding-right:8px;"><strong>Decision:</strong></td><td>{escape(decision_label)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Total Amount:</strong></td><td>₹{amt_str}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Current Status:</strong></td><td>{escape(status_label)}</td></tr>
      </table>
    """

    if req.management_comment:
        html += f"""
      <p style="font-size:14px;margin:0 0 12px 0%;">
        <strong>Management Comment:</strong><br>
        {management_comment_html}
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
        f"Management decision for reimbursement #{req.id}",
        "",
        f"Decision      : {decision_label}",
        f"Total Amount  : ₹{amt_str}",
        f"Current Status: {status_label}",
        "",
    ]
    if req.management_comment:
        txt_lines.extend(["Management Comment:", (req.management_comment or ""), ""])
    txt_lines.extend(["View details:", detail_url])
    txt = "\n".join(txt_lines)

    # NEW POLICY: No admin CCs. If you consider Finance “concerned,” you can CC finance mapping here.
    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=[emp_email],
        cc=[],  # remove admins
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "management_action"},
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

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(req.total_amount)
    subject = f"Reimbursement Paid — {emp_name} — ₹{amt_str}"

    detail_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))
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
        <tr><td style="padding-right:8px;"><strong>Status:</strong></td><td>{escape(req.get_status_display())}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Paid On:</strong></td><td>{_format_dt(paid_at)}</td></tr>
    """
    if req.finance_payment_reference:
        html += f"""
        <tr><td style="padding-right:8px;"><strong>Payment Ref:</strong></td>
            <td>{escape(req.finance_payment_reference)}</td></tr>
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

    _send_and_log(
        req,
        kind="paid",
        subject=subject,
        to_addrs=[emp_email],
        cc=[],  # DO NOT CC managers/admins anymore
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "paid"},
    )

# ---------------------------------------------------------------------------
# NEW: Bill-level notifications wired for finance-first flow
# ---------------------------------------------------------------------------

def _bill_view_url(line: ReimbursementLine) -> str:
    try:
        return _abs_url(reverse("reimbursement:receipt_line", args=[line.id]))
    except Exception:
        return ""


def send_bill_rejected_by_finance(req: ReimbursementRequest, line: ReimbursementLine) -> None:
    """
    Finance rejects a single bill — email ONLY the employee with bill details and reason.
    Keeps the overall request in Partial Hold until all bills are FINANCE_APPROVED.
    """
    if not _email_enabled():
        logger.info("Emails disabled; skipping bill-rejected email for req #%s line #%s.", req.id, line.id)
        return

    kind = f"bill_rejected_by_finance_{line.id}"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate '%s' email for req #%s line #%s.", kind, req.id, line.id)
        return

    emp_email = _employee_email(req)
    if not emp_email:
        logger.info("Bill rejected email suppressed: employee has no email (req #%s).", req.id)
        return

    emp_name = escape(_employee_display_name(req.created_by))
    amt_str = _format_amount(line.amount or Decimal("0.00"))
    reason_html = escape(line.finance_rejection_reason or "-").replace("\n", "<br>")
    bill_url = _bill_view_url(line)
    req_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))

    subject = f"Reimbursement #{req.id}: One bill was rejected by Finance"

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin:0 0 12px 0;color:#111827;">Bill Rejected by Finance</h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID:</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee:</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Bill ID:</strong></td><td>#{line.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Bill Amount:</strong></td><td>₹{amt_str}</td></tr>
      </table>

      <p style="font-size:14px;margin:0 0 12px 0;">
        <strong>Reason:</strong><br>{reason_html}
      </p>

      <p style="font-size:14px;margin:12px 0;">
        {('Bill file: <a href="'+bill_url+'" style="color:#2563eb;text-decoration:none;">View</a><br>' if bill_url else '')}
        Full request: <a href="{req_url}" style="color:#2563eb;text-decoration:none;">{req_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Edit this rejected bill or add a replacement, then resubmit for Finance review.
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Bill rejected by Finance (request #{req.id})",
            "",
            f"Employee : {_employee_display_name(req.created_by)}",
            f"Bill ID  : #{line.id}",
            f"Amount   : ₹{amt_str}",
            f"Reason   : {(line.finance_rejection_reason or '-').strip()}",
            "",
            f"Bill file : {bill_url or '-'}",
            f"Request   : {req_url}",
        ]
    )

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=[emp_email],
        cc=[],  # remove admins
        reply_to=[],  # defaults to Amreen
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "bill_rejected"},
    )


def send_bill_resubmitted(req: ReimbursementRequest, line: ReimbursementLine, *, actor=None) -> None:
    """
    Employee edits/replaces a previously rejected bill — email the Finance team (Akshay & Sharyu).
    """
    if not _email_enabled():
        logger.info("Emails disabled; skipping bill-resubmitted email for req #%s line #%s.", req.id, line.id)
        return

    kind = f"bill_resubmitted_{line.id}"
    if _already_sent_recent(req, kind_hint=kind):
        logger.info("Suppressing duplicate '%s' email for req #%s line #%s.", kind, req.id, line.id)
        return

    fin_rec = _recipients_for_finance_enforced()
    if not fin_rec.to:
        logger.warning("Bill resubmitted email suppressed: no finance recipients for req #%s.", req.id)
        return

    emp_name = escape(_employee_display_name(req.created_by))
    emp_email = (getattr(req.created_by, "email", "") or "").strip() or "-"
    amt_str = _format_amount(line.amount or Decimal("0.00"))
    resubmitter = _employee_display_name(actor) if actor else emp_name
    bill_url = _bill_view_url(line)
    req_url = _abs_url(reverse("reimbursement:request_detail", args=[req.id]))

    subject = f"Reimbursement #{req.id}: Employee resubmitted a corrected bill"

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:700px;margin:0 auto;background:#ffffff;border-radius:10px;
                padding:20px;border:1px solid #e5e7eb;">
      <h2 style="margin:0 0 12px 0;color:#111827;">Corrected Bill Resubmitted</h2>

      <table style="font-size:14px;margin:8px 0 16px 0;">
        <tr><td style="padding-right:8px;"><strong>Request ID:</strong></td><td>#{req.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee:</strong></td><td>{emp_name}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Employee Email:</strong></td><td>{escape(emp_email)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Resubmitted By:</strong></td><td>{escape(resubmitter)}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Bill ID:</strong></td><td>#{line.id}</td></tr>
        <tr><td style="padding-right:8px;"><strong>Bill Amount:</strong></td><td>₹{amt_str}</td></tr>
      </table>

      <p style="font-size:14px;margin:12px 0;">
        {('Bill file: <a href="'+bill_url+'" style="color:#2563eb;text-decoration:none;">View</a><br>' if bill_url else '')}
        Full request: <a href="{req_url}" style="color:#2563eb;text-decoration:none;">{req_url}</a>
      </p>

      <p style="font-size:13px;margin-top:16px;color:#4b5563;">
        Please re-verify only the corrected bill and proceed with the request.
      </p>
    </div>
  </body>
</html>
    """

    txt = "\n".join(
        [
            f"Corrected bill resubmitted (request #{req.id})",
            "",
            f"Employee        : {_employee_display_name(req.created_by)}",
            f"Employee Email  : {emp_email}",
            f"Resubmitted By  : {resubmitter}",
            f"Bill ID         : #{line.id}",
            f"Bill Amount     : ₹{amt_str}",
            "",
            f"Bill file : {bill_url or '-'}",
            f"Request   : {req_url}",
        ]
    )

    _send_and_log(
        req,
        kind=kind,
        subject=subject,
        to_addrs=fin_rec.to,
        cc=[],  # enforced: only finance team
        reply_to=[emp_email] if emp_email and emp_email != "-" else _amreen_reply_to(),
        html=html,
        txt=txt,
        extra_headers={"X-BOS-Flow": "reimbursement", "X-BOS-Stage": "bill_resubmitted"},
    )
