# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\pending_digest.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple

import pytz
from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.utils import timezone
from django.core.cache import cache

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
    def filter_recipients_for_category(*, category: str, to=None, cc=None, bcc=None, **_) -> Tuple[list, list, list]:
        return list(to or []), list(cc or []), list(bcc or [])

    def strip_rows_to_delegations_only_if_pankaj_target(rows: List[dict], *, target_email: str) -> List[dict]:
        return rows
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
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


# ---------------- IST helpers ----------------
def _now_ist_dt() -> datetime:
    return timezone.now().astimezone(IST)


def _today_ist() -> date:
    return _now_ist_dt().date()


def _ttl_until_next_3am_ist(now_ist: Optional[datetime] = None) -> int:
    n = now_ist or _now_ist_dt()
    next3 = (n + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    return max(int((next3 - n).total_seconds()), 60)
# ---------------------------------------------------------------------


# ---------------- Working-day / holiday helpers (IST) ----------------
def _is_sunday_ist(d: date) -> bool:
    return d.weekday() == 6  # Sunday == 6


def _is_holiday_ist(d: date) -> bool:
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday.objects.filter(date=d).exists()
    except Exception:
        # If Holiday model not available, treat only Sundays as non-working
        return False


def _is_working_day_ist(d: date) -> bool:
    return (not _is_sunday_ist(d)) and (not _is_holiday_ist(d))
# ---------------------------------------------------------------------


def _display_user(u) -> str:
    if not u:
        return "-"
    try:
        full = u.get_full_name()
        if full:
            return full
    except Exception:
        pass
    return getattr(u, "username", None) or getattr(u, "email", "-") or "-"


# --- central leave-aware guard for “suppress mails during leave” ------------
def _is_on_leave_today(user) -> bool:
    """
    True if user is blocked by leave for *today* (IST).
    Uses shared date-level guard (10:00 IST anchor), with model fallback.
    """
    today = _today_ist()

    try:
        from apps.tasks.utils.blocking import is_user_blocked  # type: ignore
        return bool(is_user_blocked(user, today))
    except Exception:
        pass

    try:
        from apps.leave.models import LeaveRequest  # type: ignore
        fn = getattr(LeaveRequest, "is_user_blocked_on", None)
        if callable(fn):
            return bool(fn(user, today))
    except Exception:
        pass

    return False
# -----------------------------------------------------------------------------


# ---------------- Helpers to include ONLY past/today items -------------------
def _planned_date_to_ist_date(pd) -> Optional[date]:
    """
    Convert model planned_date (date or datetime or None) to an IST calendar date.
    Returns None if cannot parse.
    """
    if not pd:
        return None
    try:
        if isinstance(pd, datetime):
            # timezone.localtime requires aware dt; tolerate naive by forcing current TZ
            if timezone.is_naive(pd):
                pd = timezone.make_aware(pd, timezone.get_current_timezone())
            return timezone.localtime(pd, IST).date()
        return pd  # already a date
    except Exception:
        try:
            if isinstance(pd, datetime):
                return pd.date()
        except Exception:
            return None
    return None


def _include_only_past_and_today(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep rows where planned_date <= today IST.
    """
    today = _today_ist()
    out: List[Dict[str, Any]] = []
    for r in rows:
        pd = r.get("_planned_date_raw")
        d = _planned_date_to_ist_date(pd)
        if d is None:
            # If no due date, be conservative: exclude from pending digest
            continue
        if d <= today:
            out.append(r)
    return out
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Idempotency keys (cache-backed)
# -----------------------------------------------------------------------------
def _emp_digest_key(day_iso: str, user_id: int) -> str:
    return f"pending_digest:emp:{user_id}:{day_iso}"


def _admin_digest_key(day_iso: str, target_email: str) -> str:
    safe = (target_email or "").strip().lower() or "unknown"
    return f"pending_digest:admin:{safe}:{day_iso}"


def _try_claim(key: str, ttl: int) -> bool:
    """
    Atomic claim using cache.add. If cache is unavailable, allow best-effort send.
    """
    try:
        return bool(cache.add(key, True, ttl))
    except Exception:
        logger.warning(_safe_console_text(f"[PENDING DIGEST] Cache claim unavailable for key={key}; continuing best-effort"))
        return True
# -----------------------------------------------------------------------------


def _rows_for_user(user) -> List[Dict[str, Any]]:
    """
    Build rows for a single user.
    Filters to planned_date <= today IST only.
    Excludes voided rows (is_skipped_due_to_leave=True) if that field exists.
    """
    rows: List[Dict[str, Any]] = []

    # Checklist (Pending for this user)
    try:
        qs = Checklist.objects.filter(status="Pending", assign_to=user)
        if hasattr(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] Checklist fetch failed for user {getattr(user,'id','?')}: {e}"))

    # Delegation (Pending for this user)
    try:
        qs = Delegation.objects.filter(status="Pending", assign_to=user)
        if hasattr(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

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
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] Delegation fetch failed for user {getattr(user,'id','?')}: {e}"))

    # Help Ticket (not Closed for this user)
    try:
        qs = HelpTicket.objects.exclude(status="Closed").filter(assign_to=user)
        if hasattr(HelpTicket, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            title = getattr(obj, "title", "") or ""
            desc = (getattr(obj, "description", "") or "").strip()
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
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING DIGEST] HelpTicket fetch failed for user {getattr(user,'id','?')}: {e}"))

    filtered = _include_only_past_and_today(rows)

    try:
        filtered.sort(key=lambda r: (r.get("due_date") or "9999-12-31", r.get("task_type") or "", r.get("task_id") or ""))
    except Exception:
        pass

    for r in filtered:
        r.pop("_planned_date_raw", None)

    return filtered


def _rows_for_all_users() -> List[Dict[str, Any]]:
    """
    Build one big table containing ALL employees’ pending tasks.
    Filters to planned_date <= today IST only.
    Excludes voided rows (is_skipped_due_to_leave=True) if that field exists.
    """
    rows: List[Dict[str, Any]] = []

    # Checklist
    try:
        qs = Checklist.objects.filter(status="Pending")
        if hasattr(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")
        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_desc,
                    assigned_to=_display_user(obj.assign_to),
                    assigned_by=_display_user(obj.assign_by),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] Checklist fetch failed: {e}"))

    # Delegation
    try:
        qs = Delegation.objects.filter(status="Pending")
        if hasattr(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")
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
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] Delegation fetch failed: {e}"))

    # Help Ticket (not Closed)
    try:
        qs = HelpTicket.objects.exclude(status="Closed")
        if hasattr(HelpTicket, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")
        for obj in qs:
            title = getattr(obj, "title", "") or ""
            desc = (getattr(obj, "description", "") or "").strip()
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
                ).__dict__ | {"_planned_date_raw": getattr(obj, "planned_date", None)}
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[ADMIN DIGEST] HelpTicket fetch failed: {e}"))

    filtered = _include_only_past_and_today(rows)

    try:
        filtered.sort(
            key=lambda r: (
                r.get("due_date") or "9999-12-31",
                r.get("assigned_to") or "",
                r.get("task_type") or "",
                r.get("task_id") or "",
            )
        )
    except Exception:
        pass

    for r in filtered:
        r.pop("_planned_date_raw", None)

    return filtered


def _email_notifications_enabled() -> bool:
    try:
        feats = getattr(settings, "FEATURES", {})
        if isinstance(feats, dict) and "EMAIL_NOTIFICATIONS" in feats:
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
    Sends ONE email per employee containing ALL of THEIR pending tasks (planned_date <= today IST).
    Skips if user is on leave today (IST), unless force=True.

    Idempotency:
      - Per user/day cache claim, unless force=True.
    """
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[PENDING DIGEST] Skipped: email notifications disabled"))
        return {"ok": True, "skipped": True, "reason": "email_notifications_disabled"}

    today = _today_ist()
    if not force and not _is_working_day_ist(today):
        logger.info(_safe_console_text(f"[PENDING DIGEST] Skipped (non-working day {today})"))
        return {"ok": True, "skipped": True, "reason": "non_working_day"}

    User = get_user_model()
    base_qs = User.objects.filter(is_active=True)
    if username:
        base_qs = base_qs.filter(Q(username=username) | Q(email__iexact=username))
    users = base_qs.order_by("id")

    sent = 0
    skipped = 0
    total_candidates = 0
    day_iso = today.isoformat()
    ttl = _ttl_until_next_3am_ist(_now_ist_dt())

    for user in users:
        total_candidates += 1

        if _is_on_leave_today(user) and not force:
            skipped += 1
            logger.info(_safe_console_text(f"[PENDING DIGEST] Suppressed for user_id={getattr(user,'id','?')} (on leave today)"))
            continue

        recips = [to_override] if to_override else _dedupe_emails([getattr(user, "email", "") or ""])
        recips = [r for r in recips if r]
        if not recips:
            skipped += 1
            logger.info(_safe_console_text(f"[PENDING DIGEST] Skip user id={getattr(user,'id','?')} – no email"))
            continue

        # Per-user/day claim to prevent duplicates across celery schedules/retries
        if not force:
            key = _emp_digest_key(day_iso, int(getattr(user, "id", 0) or 0))
            if not _try_claim(key, ttl):
                skipped += 1
                logger.info(_safe_console_text(f"[PENDING DIGEST] Already sent/claimed user_id={getattr(user,'id','?')} day={day_iso}"))
                continue

        rows = _rows_for_user(user)
        rows = strip_rows_to_delegations_only_if_pankaj_target(rows, target_email=recips[0])

        if not rows and not send_even_if_empty and not force:
            skipped += 1
            continue

        filt_to, _, _ = filter_recipients_for_category(
            category="delegation.pending_digest",
            to=recips,
        )
        if not filt_to:
            skipped += 1
            logger.info(_safe_console_text(f"[PENDING DIGEST] Guard filtered recipients; skip user id={getattr(user,'id','?')}"))
            continue

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
            logger.info(_safe_console_text(
                f"[PENDING DIGEST] Sent to {filt_to[0]} (user_id={getattr(user,'id','?')}, items={len(rows)})"
            ))
            sent += 1
        except Exception as e:
            skipped += 1
            logger.error(_safe_console_text(f"[PENDING DIGEST] Email failure for user_id={getattr(user,'id','?')}: {e}"))

    logger.info(
        _safe_console_text(
            f"[PENDING DIGEST] Completed at {_now_ist_dt():%Y-%m-%d %H:%M IST}: "
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
    Includes ONLY items with planned_date <= today IST.
    Skips on non-working days unless force=True.

    Idempotency:
      - Per target email/day cache claim, unless force=True.
    """
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[ADMIN DIGEST] Skipped: email notifications disabled"))
        return {"ok": True, "skipped": True, "reason": "email_notifications_disabled"}

    today = _today_ist()
    if not force and not _is_working_day_ist(today):
        logger.info(_safe_console_text(f"[ADMIN DIGEST] Skipped (non-working day {today})"))
        return {"ok": True, "skipped": True, "reason": "non_working_day"}

    default_admin = getattr(settings, "ADMIN_PENDING_DIGEST_TO", "")
    target = (to or default_admin or "").strip()
    if not target:
        logger.warning(_safe_console_text("[ADMIN DIGEST] No admin recipient; aborting"))
        return {"ok": False, "skipped": True, "reason": "no_admin_recipient"}

    day_iso = today.isoformat()
    ttl = _ttl_until_next_3am_ist(_now_ist_dt())

    if not force:
        key = _admin_digest_key(day_iso, target)
        if not _try_claim(key, ttl):
            logger.info(_safe_console_text(f"[ADMIN DIGEST] Already sent/claimed for {target} day={day_iso}"))
            return {"ok": True, "skipped": True, "reason": "already_sent", "day": day_iso}

    rows = _rows_for_all_users()
    rows = strip_rows_to_delegations_only_if_pankaj_target(rows, target_email=target)

    if not rows and not force:
        logger.info(_safe_console_text("[ADMIN DIGEST] No pending rows; skipped send"))
        return {"ok": True, "skipped": True, "reason": "empty"}

    filt_to, _, _ = filter_recipients_for_category(
        category="delegation.pending_digest",
        to=[target],
    )
    if not filt_to:
        logger.info(_safe_console_text("[ADMIN DIGEST] Guard filtered recipient list; no email sent"))
        return {"ok": True, "skipped": True, "reason": "filtered_by_guard"}

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
