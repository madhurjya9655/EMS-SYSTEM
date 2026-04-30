# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\pending_digest.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone

from .models import Checklist, Delegation, HelpTicket
from .utils import (
    send_html_email,
    _fmt_dt_date,
    _safe_console_text,
    _dedupe_emails,
)

# ---------------------------------------------------------------------------
# Central email guards.
# These are config-driven guards. If unavailable, fallback keeps mail working.
# ---------------------------------------------------------------------------
try:
    from apps.common.email_guard import (
        filter_recipients_for_category,
        strip_rows_to_delegations_only_if_pankaj_target,
    )
except Exception:
    def filter_recipients_for_category(
        *,
        category: str,
        to=None,
        cc=None,
        bcc=None,
        **_,
    ) -> Tuple[list, list, list]:
        return list(to or []), list(cc or []), list(bcc or [])

    def strip_rows_to_delegations_only_if_pankaj_target(
        rows: List[dict],
        *,
        target_email: str,
    ) -> List[dict]:
        return rows


logger = logging.getLogger(__name__)

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

EMPLOYEE_DIGEST_TEMPLATE = "email/user_pending_tasks_digest.html"
ADMIN_DIGEST_TEMPLATE = "email/daily_pending_tasks_summary.html"
EMAIL_CATEGORY = "delegation.pending_digest"


@dataclass
class Row:
    task_id: str
    task_title: str
    assigned_to: str
    assigned_by: str
    due_date: str
    task_type: str
    status: str = "Pending"


# ---------------------------------------------------------------------------
# Date / time helpers
# ---------------------------------------------------------------------------
def _now_ist_dt() -> datetime:
    return timezone.now().astimezone(IST)


def _today_ist() -> date:
    return _now_ist_dt().date()


def _ttl_until_next_3am_ist(now_ist: Optional[datetime] = None) -> int:
    """
    Cache TTL until next 3:00 AM IST.

    This keeps idempotency keys alive long enough to avoid duplicate digest mails
    from Celery retries, duplicate schedules, or worker restarts.
    """
    current = now_ist or _now_ist_dt()
    next_3am = (current + timedelta(days=1)).replace(
        hour=3,
        minute=0,
        second=0,
        microsecond=0,
    )
    return max(int((next_3am - current).total_seconds()), 60)


# ---------------------------------------------------------------------------
# Working-day helpers
# ---------------------------------------------------------------------------
def _is_sunday_ist(value: date) -> bool:
    return value.weekday() == 6


def _is_holiday_ist(value: date) -> bool:
    """
    Uses Holiday model if available. If unavailable, holidays are ignored.
    Sundays are handled separately.
    """
    try:
        from apps.settings.models import Holiday  # type: ignore
        return Holiday.objects.filter(date=value).exists()
    except Exception:
        return False


def _is_working_day_ist(value: date) -> bool:
    return (not _is_sunday_ist(value)) and (not _is_holiday_ist(value))


# ---------------------------------------------------------------------------
# Display / formatting helpers
# ---------------------------------------------------------------------------
def _safe_str(value: Any, default: str = "-") -> str:
    if value is None:
        return default

    try:
        text = str(value).strip()
    except Exception:
        return default

    return text or default


def _display_user(user) -> str:
    """
    Returns the best readable display value for a user.
    """
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


def _display_user_email(user) -> str:
    if not user:
        return ""
    return (getattr(user, "email", "") or "").strip()


def _model_has_field(model, field_name: str) -> bool:
    try:
        return any(field.name == field_name for field in model._meta.get_fields())
    except Exception:
        return hasattr(model, field_name)


def _normalise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keeps email templates stable by ensuring every row has all expected keys.
    """
    return {
        "task_id": _safe_str(row.get("task_id")),
        "task_title": _safe_str(row.get("task_title")),
        "assigned_to": _safe_str(row.get("assigned_to")),
        "assigned_by": _safe_str(row.get("assigned_by")),
        "due_date": _safe_str(row.get("due_date")),
        "task_type": _safe_str(row.get("task_type")),
        "status": _safe_str(row.get("status"), "Pending"),
    }


def _sort_rows_for_employee(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        return sorted(
            rows,
            key=lambda row: (
                row.get("due_date") or "9999-12-31",
                row.get("task_type") or "",
                row.get("task_id") or "",
            ),
        )
    except Exception:
        return rows


def _sort_rows_for_admin(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        return sorted(
            rows,
            key=lambda row: (
                row.get("due_date") or "9999-12-31",
                row.get("assigned_to") or "",
                row.get("task_type") or "",
                row.get("task_id") or "",
            ),
        )
    except Exception:
        return rows


# ---------------------------------------------------------------------------
# Leave-aware guard
# ---------------------------------------------------------------------------
def _is_on_leave_today(user) -> bool:
    """
    Returns True if the user is blocked/on leave today in IST.

    The function tries the central blocking utility first, then falls back to
    LeaveRequest.is_user_blocked_on if available.
    """
    today = _today_ist()

    try:
        from apps.tasks.utils.blocking import is_user_blocked  # type: ignore
        return bool(is_user_blocked(user, today))
    except Exception:
        pass

    try:
        from apps.leave.models import LeaveRequest  # type: ignore
        checker = getattr(LeaveRequest, "is_user_blocked_on", None)
        if callable(checker):
            return bool(checker(user, today))
    except Exception:
        pass

    return False


# ---------------------------------------------------------------------------
# Pending-date filtering
# ---------------------------------------------------------------------------
def _planned_date_to_ist_date(value) -> Optional[date]:
    """
    Converts a planned_date value to an IST date.

    Supported values:
    - datetime
    - date
    - None

    Unknown values return None.
    """
    if not value:
        return None

    try:
        if isinstance(value, datetime):
            dt = value
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, timezone.get_current_timezone())
            return timezone.localtime(dt, IST).date()

        if isinstance(value, date):
            return value

    except Exception:
        logger.exception(
            _safe_console_text(
                f"[PENDING DIGEST] Failed converting planned_date={value!r} to IST date"
            )
        )

    return None


def _include_only_past_and_today(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keeps only rows where planned_date <= today IST.

    Rows with no planned date are excluded to prevent future/unknown items from
    appearing in the pending digest.
    """
    today = _today_ist()
    output: List[Dict[str, Any]] = []

    for row in rows:
        planned_date = _planned_date_to_ist_date(row.get("_planned_date_raw"))

        if planned_date is None:
            continue

        if planned_date <= today:
            output.append(row)

    return output


# ---------------------------------------------------------------------------
# Idempotency keys
# ---------------------------------------------------------------------------
def _emp_digest_key(day_iso: str, user_id: int) -> str:
    return f"pending_digest:emp:{user_id}:{day_iso}"


def _admin_digest_key(day_iso: str, target_email: str) -> str:
    safe_email = (target_email or "").strip().lower() or "unknown"
    return f"pending_digest:admin:{safe_email}:{day_iso}"


def _try_claim(key: str, ttl: int) -> bool:
    """
    Atomic cache claim. If cache is unavailable, allows best-effort send.
    """
    try:
        return bool(cache.add(key, True, ttl))
    except Exception:
        logger.warning(
            _safe_console_text(
                f"[PENDING DIGEST] Cache claim unavailable for key={key}; continuing best-effort"
            )
        )
        return True


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------
def _rows_for_user(user) -> List[Dict[str, Any]]:
    """
    Builds pending task rows for one employee.

    Includes:
    - Checklist with status Pending
    - Delegation with status Pending
    - HelpTicket where status is not Closed

    Excludes:
    - future planned_date rows
    - rows skipped due to leave if model has is_skipped_due_to_leave
    """
    rows: List[Dict[str, Any]] = []

    # Checklist
    try:
        qs = Checklist.objects.filter(status="Pending", assign_to=user)

        if _model_has_field(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "task_name", ""), "")
            description = _safe_str(getattr(obj, "message", ""), "")
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                    status="Pending",
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(
                f"[PENDING DIGEST] Checklist fetch failed for user_id={getattr(user, 'id', '?')}: {exc}"
            )
        )

    # Delegation
    try:
        qs = Delegation.objects.filter(status="Pending", assign_to=user)

        if _model_has_field(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "task_name", ""), "")
            description = (
                _safe_str(getattr(obj, "message", ""), "")
                or _safe_str(getattr(obj, "description", ""), "")
            )
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"DL-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Delegation",
                    status="Pending",
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(
                f"[PENDING DIGEST] Delegation fetch failed for user_id={getattr(user, 'id', '?')}: {exc}"
            )
        )

    # Help Ticket
    try:
        qs = HelpTicket.objects.exclude(status="Closed").filter(assign_to=user)

        if _model_has_field(HelpTicket, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "title", ""), "")
            description = _safe_str(getattr(obj, "description", ""), "")
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"HT-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Help Ticket",
                    status=_safe_str(getattr(obj, "status", "Pending"), "Pending"),
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(
                f"[PENDING DIGEST] HelpTicket fetch failed for user_id={getattr(user, 'id', '?')}: {exc}"
            )
        )

    filtered_rows = _include_only_past_and_today(rows)

    for row in filtered_rows:
        row.pop("_planned_date_raw", None)

    filtered_rows = [_normalise_row(row) for row in filtered_rows]
    return _sort_rows_for_employee(filtered_rows)


def _rows_for_all_users() -> List[Dict[str, Any]]:
    """
    Builds pending task rows for all employees.

    Used by admin consolidated digest.
    """
    rows: List[Dict[str, Any]] = []

    # Checklist
    try:
        qs = Checklist.objects.filter(status="Pending")

        if _model_has_field(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "task_name", ""), "")
            description = _safe_str(getattr(obj, "message", ""), "")
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"CL-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Checklist",
                    status="Pending",
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(f"[ADMIN DIGEST] Checklist fetch failed: {exc}")
        )

    # Delegation
    try:
        qs = Delegation.objects.filter(status="Pending")

        if _model_has_field(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "task_name", ""), "")
            description = (
                _safe_str(getattr(obj, "message", ""), "")
                or _safe_str(getattr(obj, "description", ""), "")
            )
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"DL-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Delegation",
                    status="Pending",
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(f"[ADMIN DIGEST] Delegation fetch failed: {exc}")
        )

    # Help Ticket
    try:
        qs = HelpTicket.objects.exclude(status="Closed")

        if _model_has_field(HelpTicket, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)

        qs = qs.select_related("assign_to", "assign_by").order_by(
            "planned_date",
            "id",
        )

        for obj in qs:
            title = _safe_str(getattr(obj, "title", ""), "")
            description = _safe_str(getattr(obj, "description", ""), "")
            title_description = title if not description else f"{title} - {description}"

            rows.append(
                Row(
                    task_id=f"HT-{obj.id}",
                    task_title=title_description,
                    assigned_to=_display_user(getattr(obj, "assign_to", None)),
                    assigned_by=_display_user(getattr(obj, "assign_by", None)),
                    due_date=_fmt_dt_date(getattr(obj, "planned_date", None)),
                    task_type="Help Ticket",
                    status=_safe_str(getattr(obj, "status", "Pending"), "Pending"),
                ).__dict__ | {
                    "_planned_date_raw": getattr(obj, "planned_date", None),
                }
            )

    except Exception as exc:
        logger.error(
            _safe_console_text(f"[ADMIN DIGEST] HelpTicket fetch failed: {exc}")
        )

    filtered_rows = _include_only_past_and_today(rows)

    for row in filtered_rows:
        row.pop("_planned_date_raw", None)

    filtered_rows = [_normalise_row(row) for row in filtered_rows]
    return _sort_rows_for_admin(filtered_rows)


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------
def _email_notifications_enabled() -> bool:
    try:
        features = getattr(settings, "FEATURES", {})
        if isinstance(features, dict) and "EMAIL_NOTIFICATIONS" in features:
            return bool(features.get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        pass

    return True


# ---------------------------------------------------------------------------
# Employee digest task
# ---------------------------------------------------------------------------
@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_daily_employee_pending_digest(
    self,
    force: bool = False,
    send_even_if_empty: bool = False,
    username: Optional[str] = None,
    to_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Sends one pending task email per employee.

    Rules:
    - Only planned_date <= today IST is included.
    - User is skipped if on leave today, unless force=True.
    - Sundays and holidays are skipped unless force=True.
    - Idempotency is enforced per user per day unless force=True.
    """
    if not _email_notifications_enabled():
        logger.info(
            _safe_console_text(
                "[PENDING DIGEST] Skipped: email notifications disabled"
            )
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "email_notifications_disabled",
        }

    today = _today_ist()
    day_iso = today.isoformat()

    if not force and not _is_working_day_ist(today):
        logger.info(
            _safe_console_text(
                f"[PENDING DIGEST] Skipped: non-working day {day_iso}"
            )
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "non_working_day",
            "day": day_iso,
        }

    User = get_user_model()

    users_qs = User.objects.filter(is_active=True)

    if username:
        username_clean = username.strip()
        users_qs = users_qs.filter(
            Q(username=username_clean)
            | Q(email__iexact=username_clean)
        )

    users_qs = users_qs.order_by("id")

    sent = 0
    skipped = 0
    candidates = 0
    ttl = _ttl_until_next_3am_ist(_now_ist_dt())

    for user in users_qs:
        candidates += 1

        if _is_on_leave_today(user) and not force:
            skipped += 1
            logger.info(
                _safe_console_text(
                    f"[PENDING DIGEST] Suppressed for user_id={getattr(user, 'id', '?')} because user is on leave today"
                )
            )
            continue

        if to_override:
            recipients = _dedupe_emails([to_override])
        else:
            recipients = _dedupe_emails([_display_user_email(user)])

        recipients = [email for email in recipients if email]

        if not recipients:
            skipped += 1
            logger.info(
                _safe_console_text(
                    f"[PENDING DIGEST] Skipped user_id={getattr(user, 'id', '?')} because no email exists"
                )
            )
            continue

        if not force:
            key = _emp_digest_key(day_iso, int(getattr(user, "id", 0) or 0))
            if not _try_claim(key, ttl):
                skipped += 1
                logger.info(
                    _safe_console_text(
                        f"[PENDING DIGEST] Already sent or claimed user_id={getattr(user, 'id', '?')} day={day_iso}"
                    )
                )
                continue

        rows = _rows_for_user(user)

        try:
            rows = strip_rows_to_delegations_only_if_pankaj_target(
                rows,
                target_email=recipients[0],
            )
        except Exception:
            logger.exception(
                _safe_console_text(
                    f"[PENDING DIGEST] Guard row-strip failed for user_id={getattr(user, 'id', '?')}"
                )
            )

        if not rows and not send_even_if_empty and not force:
            skipped += 1
            logger.info(
                _safe_console_text(
                    f"[PENDING DIGEST] Skipped user_id={getattr(user, 'id', '?')} because no pending rows"
                )
            )
            continue

        try:
            filtered_to, _, _ = filter_recipients_for_category(
                category=EMAIL_CATEGORY,
                to=recipients,
            )
        except Exception:
            logger.exception(
                _safe_console_text(
                    f"[PENDING DIGEST] Recipient guard failed for user_id={getattr(user, 'id', '?')}"
                )
            )
            filtered_to = recipients

        if not filtered_to:
            skipped += 1
            logger.info(
                _safe_console_text(
                    f"[PENDING DIGEST] Guard filtered all recipients for user_id={getattr(user, 'id', '?')}"
                )
            )
            continue

        subject = f"Your Pending Tasks - {day_iso}"
        title = f"Your Pending Tasks ({day_iso})"

        try:
            send_html_email(
                subject=subject,
                template_name=EMPLOYEE_DIGEST_TEMPLATE,
                context={
                    "title": title,
                    "report_date": day_iso,
                    "total_pending": len(rows),
                    "has_rows": bool(rows),
                    "items_table": rows,
                    "site_url": SITE_URL,
                    "recipient_name": _display_user(user),
                    "employee_name": _display_user(user),
                    "employee_email": _display_user_email(user),
                },
                to=filtered_to,
                fail_silently=False,
            )

            logger.info(
                _safe_console_text(
                    f"[PENDING DIGEST] Sent employee digest to {filtered_to[0]} "
                    f"user_id={getattr(user, 'id', '?')} items={len(rows)}"
                )
            )
            sent += 1

        except Exception as exc:
            skipped += 1
            logger.error(
                _safe_console_text(
                    f"[PENDING DIGEST] Email failure for user_id={getattr(user, 'id', '?')}: {exc}"
                )
            )

    logger.info(
        _safe_console_text(
            f"[PENDING DIGEST] Completed at {_now_ist_dt():%Y-%m-%d %H:%M IST}: "
            f"sent={sent}, skipped={skipped}, candidates={candidates}, "
            f"username_filter={'yes' if username else 'no'}, "
            f"to_override={'yes' if to_override else 'no'}"
        )
    )

    return {
        "ok": True,
        "sent": sent,
        "skipped": skipped,
        "candidates": candidates,
        "day": day_iso,
    }


# ---------------------------------------------------------------------------
# Admin digest task
# ---------------------------------------------------------------------------
@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_admin_all_pending_digest(
    self,
    to: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Sends one consolidated pending task email to admin.

    Rules:
    - Only planned_date <= today IST is included.
    - Sundays and holidays are skipped unless force=True.
    - Idempotency is enforced per target email per day unless force=True.
    """
    if not _email_notifications_enabled():
        logger.info(
            _safe_console_text(
                "[ADMIN DIGEST] Skipped: email notifications disabled"
            )
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "email_notifications_disabled",
        }

    today = _today_ist()
    day_iso = today.isoformat()

    if not force and not _is_working_day_ist(today):
        logger.info(
            _safe_console_text(
                f"[ADMIN DIGEST] Skipped: non-working day {day_iso}"
            )
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "non_working_day",
            "day": day_iso,
        }

    default_admin = getattr(settings, "ADMIN_PENDING_DIGEST_TO", "")
    target = (to or default_admin or "").strip()

    if not target:
        logger.warning(
            _safe_console_text("[ADMIN DIGEST] No admin recipient configured")
        )
        return {
            "ok": False,
            "skipped": True,
            "reason": "no_admin_recipient",
        }

    ttl = _ttl_until_next_3am_ist(_now_ist_dt())

    if not force:
        key = _admin_digest_key(day_iso, target)
        if not _try_claim(key, ttl):
            logger.info(
                _safe_console_text(
                    f"[ADMIN DIGEST] Already sent or claimed for {target} day={day_iso}"
                )
            )
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_sent",
                "day": day_iso,
            }

    rows = _rows_for_all_users()

    try:
        rows = strip_rows_to_delegations_only_if_pankaj_target(
            rows,
            target_email=target,
        )
    except Exception:
        logger.exception(
            _safe_console_text(
                f"[ADMIN DIGEST] Guard row-strip failed for target={target}"
            )
        )

    if not rows and not force:
        logger.info(
            _safe_console_text("[ADMIN DIGEST] No pending rows; skipped send")
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "empty",
            "day": day_iso,
        }

    try:
        filtered_to, _, _ = filter_recipients_for_category(
            category=EMAIL_CATEGORY,
            to=[target],
        )
    except Exception:
        logger.exception(
            _safe_console_text(
                f"[ADMIN DIGEST] Recipient guard failed for target={target}"
            )
        )
        filtered_to = [target]

    if not filtered_to:
        logger.info(
            _safe_console_text(
                "[ADMIN DIGEST] Guard filtered all recipients; no email sent"
            )
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "filtered_by_guard",
            "day": day_iso,
        }

    subject = f"All Employees - Pending Tasks - {day_iso}"
    title = f"All Employees - Pending Tasks ({day_iso})"

    try:
        send_html_email(
            subject=subject,
            template_name=ADMIN_DIGEST_TEMPLATE,
            context={
                "title": title,
                "report_date": day_iso,
                "total_pending": len(rows),
                "has_rows": bool(rows),
                "items_table": rows,
                "site_url": SITE_URL,
                "recipient_name": "Admin",
            },
            to=filtered_to,
            fail_silently=False,
        )

        logger.info(
            _safe_console_text(
                f"[ADMIN DIGEST] Sent consolidated pending digest to {filtered_to[0]} items={len(rows)}"
            )
        )

        return {
            "ok": True,
            "sent": 1,
            "items": len(rows),
            "to": filtered_to[0],
            "day": day_iso,
        }

    except Exception as exc:
        logger.error(
            _safe_console_text(f"[ADMIN DIGEST] Email failure: {exc}")
        )
        return {
            "ok": False,
            "sent": 0,
            "error": str(exc),
            "day": day_iso,
        }