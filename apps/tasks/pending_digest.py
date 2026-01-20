from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any, Optional

import pytz
from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from .utils import (
    send_html_email,
    _fmt_dt_date,
    _safe_console_text,
    _dedupe_emails,
)

# ---- ISSUE 18: central guards (config-driven; no hardcoded IDs) ------------
try:
    from apps.common.email_guard import (
        filter_recipients_for_category,
        strip_rows_to_delegations_only_if_pankaj_target,
    )
except Exception:  # graceful fallbacks
    def filter_recipients_for_category(*, category: str, to=None, cc=None, bcc=None, **_):
        return list(to or []), list(cc or []), list(bcc or [])
    def strip_rows_to_delegations_only_if_pankaj_target(rows: List[dict], *, target_email: str) -> List[dict]:
        return rows
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")


@dataclass
class Row:
    task_id: str
    task_title: str
    assigned_to: str
    assigned_by: str
    due_date: str
    task_type: str
    status: str = "Pending"


def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


def _display_user(u) -> str:
    if not u:
        return "-"
    full = u.get_full_name()
    if full:
        return full
    return getattr(u, "username", None) or getattr(u, "email", "-") or "-"


# --- central leave-aware guard for “suppress mails during leave” ------------
def _is_on_leave_today(user) -> bool:
    """
    Returns True if the user is blocked by leave for *today* (IST),
    using the same helper used elsewhere so half-days respect the 10:00 gate.
    Falls back to LeaveRequest.is_user_blocked_on if blocking util is unavailable.
    """
    today = timezone.localdate()
    # Preferred: shared blocking util (date-level, 10:00 IST anchor)
    try:
        from apps.tasks.utils.blocking import is_user_blocked  # type: ignore
        return bool(is_user_blocked(user, today))
    except Exception:
        pass
    # Fallback: model helper if present
    try:
        from apps.leave.models import LeaveRequest  # type: ignore
        fn = getattr(LeaveRequest, "is_user_blocked_on", None)
        if callable(fn):
            return bool(fn(user, today))
    except Exception:
        pass
    return False
# -----------------------------------------------------------------------------


def _rows_for_user(user) -> List[Dict[str, Any]]:
    rows: List[Row] = []

    # Checklist (Pending for this user)
    try:
        qs = (
            Checklist.objects.filter(status="Pending", assign_to=user)
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.task_name or ""
            desc = (obj.message or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] Checklist fetch failed for user {getattr(user,'id','?')}: {e}"))

    # Delegation (Pending for this user)
    try:
        qs = (
            Delegation.objects.filter(status="Pending", assign_to=user)
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip() or (getattr(obj, "description", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"DL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Delegation",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] Delegation fetch failed for user {getattr(user,'id','?')}: {e}"))

    # Help Ticket (not Closed for this user)
    try:
        qs = (
            HelpTicket.objects.exclude(status="Closed")
            .filter(assign_to=user)
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.title or ""
            desc = (obj.description or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"HT-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Help Ticket",
                    status=getattr(obj, "status", "Pending") or "Pending",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] HelpTicket fetch failed for user {getattr(user,'id','?')}: {e}"))

    try:
        rows.sort(key=lambda r: (r.get("due_date") or "9999-12-31", r.get("task_type") or "", r.get("task_id") or ""))
    except Exception:
        pass

    return rows


def _rows_for_all_users() -> List[Dict[str, Any]]:
    """
    Build one big table containing ALL employees’ pending tasks (across all three types),
    including 'Assigned To' and 'Assigned By', suitable for the admin consolidated mail.
    """
    rows: List[Dict[str, Any]] = []

    # Checklist
    try:
        qs = (
            Checklist.objects.filter(status="Pending")
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.task_name or ""
            desc = (obj.message or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] Checklist fetch failed: {e}"))

    # Delegation
    try:
        qs = (
            Delegation.objects.filter(status="Pending")
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip() or (getattr(obj, "description", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"DL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Delegation",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] Delegation fetch failed: {e}"))

    # Help Ticket (not Closed)
    try:
        qs = (
            HelpTicket.objects.exclude(status="Closed")
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.title or ""
            desc = (obj.description or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"HT-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Help Ticket",
                    status=getattr(obj, "status", "Pending") or "Pending",
                ).__dict__
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] HelpTicket fetch failed: {e}"))

    try:
        rows.sort(key=lambda r: (r.get("due_date") or "9999-12-31", r.get("assigned_to") or "", r.get("task_type") or "", r.get("task_id") or ""))
    except Exception:
        pass

    return rows


def _email_notifications_enabled() -> bool:
    try:
        feats = getattr(settings, "FEATURES", {})
        if isinstance(feats, dict):
            return bool(feats.get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        pass
    return True


@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_daily_employee_pending_digest(
    self,
    force: bool = False,
    send_even_if_empty: bool = False,
    username: Optional[str] = None,
    to_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Sends ONE email per employee containing ALL of THEIR pending tasks.
    - If 'username' is provided, only that user is processed.
    - 'to_override' lets you redirect that employee's email to a tester (safe for dry runs).

    ISSUE 18: If the recipient is Pankaj (configured), send ONLY delegation rows,
              and pass category 'delegation.pending_digest' through the guard.
    """
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[PENDING DIGEST] Skipped: email notifications disabled"))
        return {"ok": True, "skipped": True, "reason": "email_notifications_disabled"}

    User = get_user_model()
    base_qs = User.objects.filter(is_active=True)
    if username:
        base_qs = base_qs.filter(Q(username=username) | Q(email__iexact=username))
    users = base_qs.order_by("id")

    sent = 0
    skipped = 0
    total_candidates = 0

    for user in users:
        total_candidates += 1

        # Suppress digest if the user is on leave today (IST), unless forced
        try:
            if _is_on_leave_today(user) and not force:
                skipped += 1
                logger.info(_safe_console_text(f"[PENDING DIGEST] Suppressed for user_id={getattr(user,'id','?')} (on leave today)"))
                continue
        except Exception as e:
            logger.warning(_safe_console_text(f"[PENDING DIGEST] Leave-check failed for user_id={getattr(user,'id','?')}: {e}"))

        rows = _rows_for_user(user)

        # recipients
        recips = [to_override] if to_override else _dedupe_emails([getattr(user, "email", "") or ""])
        recips = [r for r in recips if r]
        if not recips:
            skipped += 1
            logger.info(_safe_console_text(f"[PENDING DIGEST] Skip user id={getattr(user,'id','?')} – no email"))
            continue

        # ISSUE 18: If recipient is Pankaj, keep ONLY Delegation rows
        # (helper inspects configured identity and filters accordingly)
        rows = strip_rows_to_delegations_only_if_pankaj_target(rows, target_email=recips[0])

        # After possible filtering, if empty and not forced, skip (matches prior behavior)
        if not rows and not send_even_if_empty and not force:
            skipped += 1
            continue

        # Apply guard with the allowed category for Pankaj (harmless no-op for others)
        filt_to, _, _ = filter_recipients_for_category(
            category="delegation.pending_digest",
            to=recips,
        )
        if not filt_to:
            skipped += 1
            logger.info(_safe_console_text(f"[PENDING DIGEST] Guard filtered recipients; skip user id={getattr(user,'id','?')}"))
            continue

        day_iso = _now_ist().date().isoformat()
        subject = f"Your Pending Tasks – {day_iso}"
        title = f"Your Pending Tasks ({day_iso})"

        try:
            send_html_email(
                subject=subject,
                template_name="email/daily_pending_tasks_summary.html",
                context={
                    "title": title,
                    "report_date": day_iso,
                    "total_pending": len(rows),
                    "has_rows": bool(rows),
                    "items_table": rows,
                    "site_url": SITE_URL,
                },
                to=filt_to,
                fail_silently=False,
            )
            logger.info(_safe_console_text(f"[PENDING DIGEST] Sent to {filt_to[0]} (user_id={getattr(user,'id','?')}, items={len(rows)})"))
            sent += 1
        except Exception as e:
            skipped += 1
            logger.error(_safe_console_text(f"[PENDING DIGEST] Email failure for user_id={getattr(user,'id','?')}: {e}"))

    logger.info(
        _safe_console_text(
            f"[PENDING DIGEST] Completed at {_now_ist():%Y-%m-%d %H:%M IST}: "
            f"sent={sent}, skipped={skipped}, candidates={total_candidates}, "
            f"username_filter={'yes' if username else 'no'}, to_override={'yes' if to_override else 'no'}"
        )
    )
    return {"ok": True, "sent": sent, "skipped": skipped, "candidates": total_candidates}


@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_admin_all_pending_digest(
    self,
    to: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Sends ONE consolidated email containing ALL employees' pending tasks to the admin email.
    Default recipient: settings.ADMIN_PENDING_DIGEST_TO (if set).

    ISSUE 18: If the target is Pankaj, include ONLY Delegation rows and
              send under category 'delegation.pending_digest' (allowed for him).
    """
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[ADMIN DIGEST] Skipped: email notifications disabled"))
        return {"ok": True, "skipped": True, "reason": "email_notifications_disabled"}

    # Fully config-driven recipient (no hardcoded fallback here).
    default_admin = getattr(settings, "ADMIN_PENDING_DIGEST_TO", "")
    target = (to or default_admin or "").strip()
    if not target:
        logger.warning(_safe_console_text("[ADMIN DIGEST] No admin recipient; aborting"))
        return {"ok": False, "skipped": True, "reason": "no_admin_recipient"}

    rows = _rows_for_all_users()

    # If target is Pankaj, keep ONLY Delegation rows (others unchanged)
    rows = strip_rows_to_delegations_only_if_pankaj_target(rows, target_email=target)

    if not rows and not force:
        logger.info(_safe_console_text("[ADMIN DIGEST] No pending rows; skipped send"))
        return {"ok": True, "skipped": True, "reason": "empty"}

    # Apply guard for this category (will allow Pankaj only for delegation digests)
    filt_to, _, _ = filter_recipients_for_category(
        category="delegation.pending_digest",
        to=[target],
    )
    if not filt_to:
        logger.info(_safe_console_text("[ADMIN DIGEST] Guard filtered recipient list; no email sent"))
        return {"ok": True, "skipped": True, "reason": "filtered_by_guard"}

    day_iso = _now_ist().date().isoformat()
    subject = f"All Employees – Pending Tasks – {day_iso}"
    title = f"All Employees – Pending Tasks ({day_iso})"

    try:
        send_html_email(
            subject=subject,
            template_name="email/daily_pending_tasks_summary.html",
            context={
                "title": title,
                "report_date": day_iso,
                "total_pending": len(rows),
                "has_rows": bool(rows),
                "items_table": rows,
                "site_url": SITE_URL,
            },
            to=filt_to,
            fail_silently=False,
        )
        logger.info(_safe_console_text(f"[ADMIN DIGEST] Sent consolidated pending to {filt_to[0]} items={len(rows)}"))
        return {"ok": True, "sent": 1, "items": len(rows), "to": filt_to[0]}
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] Email failure: {e}"))
        return {"ok": False, "sent": 0, "error": str(e)}
