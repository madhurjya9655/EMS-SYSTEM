# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\tasks.py
from __future__ import annotations

import logging
from datetime import timedelta, datetime, time as dt_time, date as dt_date
from typing import Tuple, List, Dict, Any, Optional

import pytz
from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.db import transaction, connection
from django.db.models import Q
from django.db.utils import OperationalError, ProgrammingError
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation, FMS, HelpTicket
from .recurrence_utils import (
    RECURRING_MODES,
    normalize_mode,
    get_next_planned_date,  # pins 19:00 IST, NO working-day shift here
)
from .utils import (
    _safe_console_text,
    send_checklist_assignment_to_user,
    send_html_email,
    get_admin_emails,
    _dedupe_emails,
    _fmt_dt_date,
)
from apps.tasks.services.blocking import guard_assign

logger = logging.getLogger(__name__)

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


# -----------------------------------------------------------------------------
# IST helpers
# -----------------------------------------------------------------------------
def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


def _today_ist_iso(now_ist: Optional[datetime] = None) -> str:
    n = now_ist or _now_ist()
    return n.date().isoformat()


def _ttl_until_next_3am_ist(now_ist: Optional[datetime] = None) -> int:
    n = now_ist or _now_ist()
    next3 = (n + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    return max(int((next3 - n).total_seconds()), 60)


def _ist_day_bounds(for_dt_ist: datetime) -> Tuple[datetime, datetime]:
    """
    Return (start_aware, end_aware) in PROJECT TZ for the IST day containing for_dt_ist.
    """
    start_ist = IST.localize(datetime.combine(for_dt_ist.date(), dt_time(0, 0)))
    end_ist = IST.localize(datetime.combine(for_dt_ist.date(), dt_time(23, 59, 59, 999999)))
    return (
        start_ist.astimezone(timezone.get_current_timezone()),
        end_ist.astimezone(timezone.get_current_timezone()),
    )


def _end_of_today_ist_in_project_tz() -> datetime:
    now_ist = _now_ist()
    end_ist = IST.localize(datetime.combine(now_ist.date(), dt_time(23, 59, 59, 999999)))
    return end_ist.astimezone(timezone.get_current_timezone())


def _assignment_anchor_for_today_10am_ist(now_ist: Optional[datetime] = None) -> datetime:
    d = (now_ist or _now_ist()).date()
    return IST.localize(datetime.combine(d, dt_time(10, 0)))


def _is_after_10am_ist(now_ist: Optional[datetime] = None) -> bool:
    n = now_ist or _now_ist()
    return n.time() >= dt_time(10, 0)


# -----------------------------------------------------------------------------
# Shared day idempotency (MATCH cron_views.py / views_cron.py)
# -----------------------------------------------------------------------------
def _fanout_done_key(day_iso: str) -> str:
    return f"due10_fanout_done:{day_iso}"


def _fanout_lock_key(day_iso: str) -> str:
    return f"due10_fanout_lock:{day_iso}"


def _acquire_fanout_lock(day_iso: str, seconds: int = 180) -> bool:
    """
    Short lock: prevents concurrent duplicate runs (web + celery + retries).
    """
    try:
        return bool(cache.add(_fanout_lock_key(day_iso), True, seconds))
    except Exception:
        # If cache is down, we cannot guarantee cross-process idempotency.
        # We still run (best-effort), but item-level guards below still reduce damage.
        logger.warning(_safe_console_text("[DUE@10] Cache lock unavailable; continuing best-effort"))
        return True


def _release_fanout_lock(day_iso: str) -> None:
    try:
        cache.delete(_fanout_lock_key(day_iso))
    except Exception:
        pass


def _fanout_already_done(day_iso: str) -> bool:
    try:
        return bool(cache.get(_fanout_done_key(day_iso), False))
    except Exception:
        return False


def _mark_fanout_done(day_iso: str, now_ist: Optional[datetime] = None) -> None:
    try:
        cache.set(_fanout_done_key(day_iso), True, _ttl_until_next_3am_ist(now_ist))
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Feature flag
# -----------------------------------------------------------------------------
def _email_notifications_enabled() -> bool:
    try:
        v = getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", None)
        if v is not None:
            return bool(v)
    except Exception:
        pass
    try:
        feats = getattr(settings, "FEATURES", None)
        if isinstance(feats, dict) and "EMAIL_NOTIFICATIONS" in feats:
            return bool(feats.get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        pass
    return True


# -----------------------------------------------------------------------------
# DB/table safety helper
# -----------------------------------------------------------------------------
_TABLE_EXISTS_CACHE: Dict[str, bool] = {}


def _table_exists_for_model(model) -> bool:
    """
    Introspection is expensive; cache in-process.
    """
    try:
        db_table = model._meta.db_table
    except Exception:
        return False

    if db_table in _TABLE_EXISTS_CACHE:
        return _TABLE_EXISTS_CACHE[db_table]

    try:
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
        ok = db_table in tables
        _TABLE_EXISTS_CACHE[db_table] = ok
        return ok
    except Exception as e:
        logger.warning(_safe_console_text(f"[DB GUARD] Failed to introspect tables: {e}"))
        return False


# -----------------------------------------------------------------------------
# Recurrence generator (FUTURE ONLY, completion-gated)
# -----------------------------------------------------------------------------
def _should_send_recur_email_now() -> bool:
    """
    Generator emails are usually undesirable (it creates FUTURE rows, not today).
    Keep OFF by default if SEND_RECUR_EMAILS_ONLY_AT_10AM=True.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return False
    if SEND_RECUR_EMAILS_ONLY_AT_10AM:
        return False
    return True


def _series_q_for_frequency(
    assign_to_id: int,
    task_name: str,
    mode: str,
    freq_norm: int,
    group_name: str | None,
):
    """
    Legacy tolerant series grouping: treat NULL frequency as 1.
    Excludes tombstoned rows so they don't drive series logic.
    """
    freq_set = [freq_norm, None]
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=freq_set)
    if hasattr(Checklist, "is_skipped_due_to_leave"):
        q &= Q(is_skipped_due_to_leave=False)
    return q


def _ensure_future_occurrence_for_series(series: dict, *, dry_run: bool = False) -> int:
    """
    Create the next STRICTLY-FUTURE occurrence only when:
      - NO Pending exists in the series (golden rule)
      - A Completed exists (base for stepping)
      - Next stepped date is > now
      - Next date is NOT today (today rows are materializer's job)
    """
    now = timezone.now()
    now_ist = _now_ist()
    today_ist = now_ist.date()

    freq_norm = max(int(series.get("frequency") or 1), 1)
    q_series = _series_q_for_frequency(
        assign_to_id=series["assign_to_id"],
        task_name=series["task_name"],
        mode=series["mode"],
        freq_norm=freq_norm,
        group_name=series.get("group_name"),
    )

    # Golden rule: If ANY Pending exists (today/past/future), do not create another.
    if Checklist.objects.filter(status="Pending").filter(q_series).exists():
        return 0

    completed = (
        Checklist.objects.filter(status="Completed")
        .filter(q_series)
        .order_by("-planned_date", "-id")
        .first()
    )
    if not completed or not getattr(completed, "planned_date", None):
        return 0

    next_dt = get_next_planned_date(completed.planned_date, series["mode"], freq_norm)

    safety = 0
    while next_dt and next_dt <= now and safety < 730:
        next_dt = get_next_planned_date(next_dt, series["mode"], freq_norm)
        safety += 1
    if not next_dt:
        return 0

    # Never create today here (today creation is materializer’s job).
    try:
        if next_dt.astimezone(IST).date() == today_ist:
            logger.info(
                _safe_console_text(
                    f"[RECUR GEN] Suppressed TODAY creation for series '{series['task_name']}' "
                    f"(assign_to_id={series['assign_to_id']})"
                )
            )
            return 0
    except Exception:
        return 0

    # Dupe guard in ±1 minute window (tolerant series)
    dupe = (
        Checklist.objects.filter(status="Pending")
        .filter(q_series)
        .filter(
            planned_date__gte=next_dt - timedelta(minutes=1),
            planned_date__lt=next_dt + timedelta(minutes=1),
        )
        .exists()
    )
    if dupe:
        return 0

    if dry_run:
        logger.info(
            _safe_console_text(
                f"[DRY RUN] Would create next checklist '{series['task_name']}' "
                f"for user_id={series['assign_to_id']} at {next_dt.astimezone(IST):%Y-%m-%d %H:%M IST}"
            )
        )
        return 0

    with transaction.atomic():
        obj = Checklist.objects.create(
            assign_by=completed.assign_by,
            task_name=completed.task_name,
            message=getattr(completed, "message", "") or "",
            assign_to=completed.assign_to,
            planned_date=next_dt,
            priority=getattr(completed, "priority", None),
            attachment_mandatory=getattr(completed, "attachment_mandatory", False),
            mode=completed.mode,
            frequency=freq_norm,
            time_per_task_minutes=getattr(completed, "time_per_task_minutes", 0) or 0,
            remind_before_days=getattr(completed, "remind_before_days", 0) or 0,
            assign_pc=getattr(completed, "assign_pc", None),
            notify_to=getattr(completed, "notify_to", None),
            auditor=getattr(completed, "auditor", None),
            set_reminder=getattr(completed, "set_reminder", False),
            reminder_mode=getattr(completed, "reminder_mode", None),
            reminder_frequency=getattr(completed, "reminder_frequency", None),
            reminder_starting_time=getattr(completed, "reminder_starting_time", None),
            checklist_auto_close=getattr(completed, "checklist_auto_close", False),
            checklist_auto_close_days=getattr(completed, "checklist_auto_close_days", 0) or 0,
            group_name=getattr(completed, "group_name", None),
            actual_duration_minutes=0,
            status="Pending",
        )

    # Optional generator email (generally off)
    if _should_send_recur_email_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix=f"Checklist Created – {obj.task_name}",
            )
        except Exception as e:
            logger.error(_safe_console_text(f"[RECUR GEN] Email failure for checklist {obj.id}: {e}"))

    logger.info(
        _safe_console_text(
            f"[RECUR GEN] Created next CL-{obj.id} '{obj.task_name}' for user_id={series['assign_to_id']} "
            f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
        )
    )
    return 1


def _generate_recurring_checklists_sync(user_id: int | None = None, dry_run: bool = False) -> dict:
    filters = {"mode__in": RECURRING_MODES}
    if hasattr(Checklist, "is_skipped_due_to_leave"):
        filters["is_skipped_due_to_leave"] = False
    if user_id:
        filters["assign_to_id"] = user_id

    seeds = (
        Checklist.objects.filter(**filters)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )

    created_total = 0
    per_user: Dict[int, int] = {}

    for s in seeds:
        m = normalize_mode(s["mode"])
        if m not in RECURRING_MODES:
            continue

        freq_norm = max(int(s.get("frequency") or 1), 1)
        s["mode"] = m
        s["frequency"] = freq_norm

        created = _ensure_future_occurrence_for_series(s, dry_run=dry_run)
        created_total += created
        if created:
            per_user[s["assign_to_id"]] = per_user.get(s["assign_to_id"], 0) + created

    if not per_user:
        logger.info(
            _safe_console_text(
                f"[RECUR GEN] No new items created @ {_now_ist():%Y-%m-%d %H:%M IST} "
                f"(dry_run={dry_run}, user_id={user_id})"
            )
        )

    return {"created": created_total, "per_user": per_user, "dry_run": dry_run, "user_id": user_id}


@shared_task(bind=True, max_retries=2, default_retry_delay=10)
def generate_recurring_checklists(self, user_id: int | None = None, dry_run: bool = False) -> dict:
    return _generate_recurring_checklists_sync(user_id=user_id, dry_run=dry_run)


@shared_task(bind=True)
def audit_recurring_health(self) -> dict:
    """
    Lightweight audit: counts suspicious series.
    NOTE: This is informational; it is not the generator.
    """
    series = (
        Checklist.objects.filter(mode__in=RECURRING_MODES)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )
    stuck = 0
    ok = 0
    details = []

    for s in series:
        # audit is best-effort; keep it simple
        has_pending = Checklist.objects.filter(status="Pending", **s).exists()
        has_completed = Checklist.objects.filter(status="Completed", **s).exists()
        if not has_pending and not has_completed:
            stuck += 1
            details.append({"series": s, "state": "no_pending_no_completed"})
        else:
            ok += 1

    logger.info(_safe_console_text(f"[RECUR AUDIT] OK series: {ok}, Stuck series: {stuck}"))
    return {"ok": ok, "stuck": stuck, "details": details}


# -----------------------------------------------------------------------------
# 10:00 IST daily due mailer (CHECKLIST DIGEST + DELEGATION ITEM MAILS)
# -----------------------------------------------------------------------------
def _is_self_assigned(obj) -> bool:
    try:
        return bool(obj.assign_by_id and obj.assign_to_id and obj.assign_by_id == obj.assign_to_id)
    except Exception:
        return False


def _sent_key(model: str, obj_id: int, day_ist_str: str) -> str:
    return f"due_mail_sent:{model}:{obj_id}:{day_ist_str}"


def _claim_send_for_today(model: str, obj_id: int, now_ist: Optional[datetime] = None) -> bool:
    """
    Atomic per-object/day claim. If claim fails, someone else already sent it.
    """
    n = now_ist or _now_ist()
    key = _sent_key(model, obj_id, n.date().isoformat())
    try:
        return bool(cache.add(key, True, _ttl_until_next_3am_ist(n)))
    except Exception:
        # Cache down -> best-effort fallback: allow send (may duplicate).
        return True


def _emp_day_key(user_id: int, day_iso: str) -> str:
    return f"due_today_checklist_digest_sent:emp:{user_id}:{day_iso}"


def _claim_emp_digest(user_id: int, now_ist: Optional[datetime] = None) -> bool:
    """
    Atomic per-user/day claim for digest to prevent race duplicates.
    """
    n = now_ist or _now_ist()
    key = _emp_day_key(user_id, n.date().isoformat())
    ttl_seconds = max(_ttl_until_next_3am_ist(n), 6 * 60 * 60)
    try:
        return bool(cache.add(key, True, ttl_seconds))
    except Exception:
        return True


def _send_delegation_email(obj: Delegation) -> None:
    if _is_self_assigned(obj):
        logger.info(_safe_console_text(f"[DUE@10] Delegation {obj.id} skipped: assigner == assignee"))
        return

    try:
        from .utils import send_delegation_assignment_to_user  # type: ignore

        complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
        send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix=f"Today’s Delegation – {obj.task_name} (due 7 PM)",
        )
        logger.info(_safe_console_text(f"[DUE@10] Delegation {obj.id} mailed to user_id={obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"[DUE@10] Delegation {obj.id} email failure: {e}"))


def _fetch_delegations_due_today(start_dt, end_dt):
    if not _table_exists_for_model(Delegation):
        logger.warning(_safe_console_text("[DUE@10] Delegation skipped: table not found"))
        return []

    try:
        qs = Delegation.objects.filter(status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt)
        if hasattr(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        return list(qs.select_related("assign_to", "assign_by"))
    except (OperationalError, ProgrammingError) as e:
        logger.warning(_safe_console_text(f"[DUE@10] Delegation skipped (DB not ready): {e}"))
        return []
    except Exception as e:
        logger.error(_safe_console_text(f"[DUE@10] Delegation fetch failed: {e}"))
        return []


def _fetch_checklists_due_today(start_dt, end_dt):
    if not _table_exists_for_model(Checklist):
        logger.warning(_safe_console_text("[DUE@10] Checklist skipped: table not found"))
        return []

    try:
        qs = Checklist.objects.filter(status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt)
        if hasattr(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        return list(qs.select_related("assign_to", "assign_by"))
    except (OperationalError, ProgrammingError) as e:
        logger.warning(_safe_console_text(f"[DUE@10] Checklist skipped (DB not ready): {e}"))
        return []
    except Exception as e:
        logger.error(_safe_console_text(f"[DUE@10] Checklist fetch failed: {e}"))
        return []


def _rows_for_checklists(objs: List[Checklist]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for obj in objs:
        title = obj.task_name or ""
        desc = (getattr(obj, "message", "") or "").strip()
        title_desc = title if not desc else f"{title} — {desc}"
        rows.append(
            {
                "task_id": f"CL-{obj.id}",
                "task_title": title_desc,
                "assigned_to": getattr(getattr(obj, "assign_to", None), "get_full_name", lambda: "")()
                or getattr(getattr(obj, "assign_to", None), "username", "")
                or getattr(getattr(obj, "assign_to", None), "email", "")
                or "-",
                "assigned_by": getattr(getattr(obj, "assign_by", None), "get_full_name", lambda: "")()
                or getattr(getattr(obj, "assign_by", None), "username", "")
                or getattr(getattr(obj, "assign_by", None), "email", "")
                or "-",
                "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                "task_type": "Checklist",
                "status": "Pending",
            }
        )
    try:
        rows.sort(key=lambda r: (r.get("due_date") or "9999-12-31", r.get("task_id") or ""))
    except Exception:
        pass
    return rows


@shared_task(bind=True, max_retries=2, default_retry_delay=30)
def send_due_today_assignments(self) -> dict:
    """
    Consolidated 10:00 IST fan-out.

    HARD GUARDS:
      - Shared day-level idempotency with cron endpoints (due10_fanout_done/lock).
      - Per-user digest atomic claim (prevents duplicate digest).
      - Per-delegation atomic claim (prevents duplicate emails).
      - Leave-aware (guard_assign @ 10:00 IST anchor).
      - Self-assign suppressions.
    """
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[DUE@10] Skipped: email notifications disabled"))
        return {"sent": 0, "checklists_emails": 0, "checklists_tasks": 0, "delegations": 0, "skipped": True, "reason": "feature_off"}

    now_ist = _now_ist()
    day_iso = _today_ist_iso(now_ist)

    if not _is_after_10am_ist(now_ist):
        logger.info(_safe_console_text("[DUE@10] Skipped: before 10:00 IST"))
        return {"sent": 0, "checklists_emails": 0, "checklists_tasks": 0, "delegations": 0, "skipped_before_10": True}

    # If already done today, no-op
    if _fanout_already_done(day_iso):
        logger.info(_safe_console_text(f"[DUE@10] Skipped: already_done_today ({day_iso})"))
        return {"sent": 0, "checklists_emails": 0, "checklists_tasks": 0, "delegations": 0, "skipped": True, "reason": "already_done_today", "day": day_iso}

    # Acquire short running lock (shared with cron endpoints)
    if not _acquire_fanout_lock(day_iso, seconds=180):
        logger.info(_safe_console_text("[DUE@10] Skipped: already_running"))
        return {"sent": 0, "checklists_emails": 0, "checklists_tasks": 0, "delegations": 0, "skipped": True, "reason": "already_running", "day": day_iso}

    try:
        start_dt, end_dt = _ist_day_bounds(now_ist)
        anchor_dt = _assignment_anchor_for_today_10am_ist(now_ist)

        checklists = _fetch_checklists_due_today(start_dt, end_dt)
        delegations = _fetch_delegations_due_today(start_dt, end_dt)

        sent_total = 0

        # -------------------------
        # Checklists: consolidated digest per user
        # -------------------------
        per_user: Dict[int, List[Checklist]] = {}
        for obj in checklists:
            if _is_self_assigned(obj):
                logger.info(_safe_console_text(f"[DUE@10] Checklist {obj.id} skipped in digest: self-assigned"))
                continue
            uid = getattr(obj, "assign_to_id", None)
            if not uid:
                continue
            per_user.setdefault(uid, []).append(obj)

        cl_emails_sent = 0
        cl_tasks_included = 0

        for uid, items in per_user.items():
            if not items:
                continue

            try:
                user = items[0].assign_to
            except Exception:
                user = None
            if not user:
                continue

            # leave guard at 10:00 IST
            try:
                if not guard_assign(user, anchor_dt):
                    logger.info(_safe_console_text(
                        f"[DUE@10] Checklist digest suppressed for user_id={getattr(user,'id','?')} (leave @ 10:00 IST)"
                    ))
                    continue
            except Exception:
                # fail-safe: don't send if we cannot evaluate
                continue

            # atomic claim for per-user digest
            if not _claim_emp_digest(uid, now_ist):
                logger.info(_safe_console_text(f"[DUE@10] Checklist digest already claimed/sent for user_id={uid}; skip"))
                continue

            email = (getattr(user, "email", "") or "").strip()
            to_list = _dedupe_emails([email]) if email else []
            if not to_list:
                logger.info(_safe_console_text(f"[DUE@10] Skip checklist digest for user_id={uid} – no email"))
                continue

            rows = _rows_for_checklists(items)
            if not rows:
                continue

            subject = f"Checklist Tasks Pending for Today ({len(rows)} Tasks)"
            title = f"Checklist Tasks Pending for Today — {day_iso}"

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
                    to=to_list,
                    fail_silently=False,
                )
                cl_emails_sent += 1
                cl_tasks_included += len(rows)
                sent_total += 1
                logger.info(_safe_console_text(
                    f"[DUE@10] Sent consolidated checklist digest to user_id={uid} items={len(rows)}"
                ))
            except Exception as e:
                logger.error(_safe_console_text(f"[DUE@10] Checklist digest email failure for user_id={uid}: {e}"))
                # Do NOT un-claim: safer to avoid resend spam loops.

        # -------------------------
        # Delegations: individual emails (per-object atomic claim)
        # -------------------------
        de_sent = 0
        for obj in delegations:
            # leave guard at 10:00 IST
            try:
                if not guard_assign(obj.assign_to, anchor_dt):
                    logger.info(_safe_console_text(
                        f"[DUE@10] Delegation {obj.id} suppressed (leave @ 10:00 IST)"
                    ))
                    continue
            except Exception:
                continue

            # atomic per-object claim
            if not _claim_send_for_today("Delegation", obj.id, now_ist):
                continue

            _send_delegation_email(obj)
            sent_total += 1
            de_sent += 1

        logger.info(_safe_console_text(
            f"[DUE@10] Completed fan-out @ {now_ist:%Y-%m-%d %H:%M IST}: "
            f"checklist_emails={cl_emails_sent} (tasks_included={cl_tasks_included}), "
            f"delegations={de_sent}, total_emails={sent_total}"
        ))

        # Mark day done (prevents repeats from celery schedule + cron endpoints)
        _mark_fanout_done(day_iso, now_ist)

        return {
            "sent": sent_total,
            "checklists_emails": cl_emails_sent,
            "checklists_tasks": cl_tasks_included,
            "delegations": de_sent,
            "skipped_before_10": False,
            "day": day_iso,
        }

    finally:
        # Release short running lock (day-done key remains)
        _release_fanout_lock(day_iso)


# -----------------------------------------------------------------------------
# Delegation reminders (every 5 minutes)
# -----------------------------------------------------------------------------
def _delegation_reminder_lock_key(obj_id: int) -> str:
    return f"delegation_reminder_lock:{obj_id}"


def _send_delegation_reminder_email(obj: Delegation) -> None:
    if _is_self_assigned(obj):
        logger.info(_safe_console_text(f"[DL REM] Delegation {obj.id} skipped: self-assigned"))
        return

    # leave guard (today @ 10:00 IST anchor)
    try:
        if not guard_assign(obj.assign_to, _assignment_anchor_for_today_10am_ist()):
            logger.info(_safe_console_text(f"[DL REM] Delegation {obj.id} suppressed (assignee on leave today)"))
            return
    except Exception:
        return

    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
    except Exception:
        complete_url = SITE_URL

    try:
        from .utils import send_delegation_assignment_to_user  # type: ignore

        send_delegation_assignment_to_user(
            delegation=obj,
            complete_url=complete_url,
            subject_prefix=f"Reminder – Delegation – {obj.task_name}",
        )
        return
    except Exception:
        pass

    # fallback (should rarely be used)
    send_checklist_assignment_to_user(
        task=obj,  # type: ignore[arg-type]
        complete_url=complete_url,
        subject_prefix=f"Reminder – Delegation – {obj.task_name}",
    )


@shared_task(bind=True, max_retries=1, default_retry_delay=30)
def dispatch_delegation_reminders(self) -> dict:
    now = timezone.now()
    sent = 0
    skipped = 0
    failed = 0

    qs = Delegation.objects.filter(
        status="Pending",
        set_reminder=True,
        reminder_time__isnull=False,
        reminder_sent_at__isnull=True,
        reminder_time__lte=now,
    )
    if hasattr(Delegation, "is_skipped_due_to_leave"):
        qs = qs.filter(is_skipped_due_to_leave=False)
    qs = qs.order_by("reminder_time", "id")

    for obj in qs:
        if not obj.reminder_time:
            skipped += 1
            continue

        lock_key = _delegation_reminder_lock_key(obj.id)
        if not cache.add(lock_key, True, 10 * 60):
            continue

        claim_ts = timezone.now()

        try:
            # DB-level claim (prevents races even if cache is weak)
            claimed = Delegation.objects.filter(
                id=obj.id,
                reminder_sent_at__isnull=True,
            ).update(reminder_sent_at=claim_ts)

            if claimed == 0:
                continue

            try:
                obj = Delegation.objects.select_related("assign_to", "assign_by").get(id=obj.id)
            except Exception:
                pass

            _send_delegation_reminder_email(obj)

            sent += 1
            logger.info(_safe_console_text(f"[DL REM] Sent reminder for Delegation {obj.id}"))

        except Exception as e:
            failed += 1
            logger.error(_safe_console_text(f"[DL REM] Failed reminder for Delegation {getattr(obj, 'id', '?')}: {e}"))
            # rollback claim so it can retry later
            try:
                Delegation.objects.filter(id=obj.id, reminder_sent_at=claim_ts).update(reminder_sent_at=None)
            except Exception:
                pass

    return {"sent": sent, "skipped": skipped, "failed": failed}


# -----------------------------------------------------------------------------
# Daily Pending Task Summary (Admin Report)
# -----------------------------------------------------------------------------
def _pending_summary_day_key(day_ist_iso: str) -> str:
    return f"daily_pending_task_summary_sent:{day_ist_iso}"


def _pending_summary_ttl_seconds(now_ist: datetime) -> int:
    try:
        nxt = (now_ist + timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)
        return max(int((nxt - now_ist).total_seconds()), 6 * 60 * 60)
    except Exception:
        return 24 * 60 * 60


def _is_sunday_or_holiday(d: dt_date) -> bool:
    try:
        if d.weekday() == 6:
            return True
    except Exception:
        pass

    try:
        from apps.settings.models import Holiday
        # support either Holiday.is_holiday(date) or ORM lookup
        if hasattr(Holiday, "is_holiday"):
            return bool(Holiday.is_holiday(d))
        return Holiday.objects.filter(date=d).exists()
    except Exception:
        return False


def _build_pending_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    end_today = _end_of_today_ist_in_project_tz()

    try:
        qs = (
            Checklist.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
        )
        if hasattr(Checklist, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                {
                    "task_id": f"CL-{obj.id}",
                    "task_title": title_desc,
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                    "task_type": "Checklist",
                    "status": "Pending",
                }
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] Checklist fetch failed: {e}"))

    try:
        qs = (
            Delegation.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
        )
        if hasattr(Delegation, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            title = obj.task_name or ""
            desc = (getattr(obj, "message", "") or "").strip() or (getattr(obj, "description", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                {
                    "task_id": f"DL-{obj.id}",
                    "task_title": title_desc,
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                    "task_type": "Delegation",
                    "status": "Pending",
                }
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] Delegation fetch failed: {e}"))

    try:
        qs = (
            FMS.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
        )
        if hasattr(FMS, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            rows.append(
                {
                    "task_id": f"FMS-{obj.id}",
                    "task_title": getattr(obj, "task_name", "") or "",
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                    "task_type": "FMS",
                    "status": "Pending",
                }
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] FMS fetch failed: {e}"))

    try:
        qs = (
            HelpTicket.objects.exclude(status="Closed")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
        )
        if hasattr(HelpTicket, "is_skipped_due_to_leave"):
            qs = qs.filter(is_skipped_due_to_leave=False)
        qs = qs.select_related("assign_to", "assign_by").order_by("planned_date", "id")

        for obj in qs:
            title = getattr(obj, "title", "") or ""
            desc = (getattr(obj, "description", "") or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                {
                    "task_id": f"HT-{obj.id}",
                    "task_title": title_desc,
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                    "task_type": "Help Ticket",
                    "status": getattr(obj, "status", "") or "Open",
                }
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] HelpTicket fetch failed: {e}"))

    try:
        rows.sort(key=lambda r: (r.get("due_date") or "9999-12-31", r.get("task_type") or "", r.get("task_id") or ""))
    except Exception:
        pass

    return rows


@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_daily_pending_task_summary(self, force: bool = False) -> dict:
    if not _email_notifications_enabled():
        logger.info(_safe_console_text("[PENDING SUMMARY] Skipped: email notifications disabled"))
        return {"ok": True, "skipped": True, "reason": "email_notifications_disabled"}

    now_ist = _now_ist()
    day_iso = now_ist.date().isoformat()

    if not force and _is_sunday_or_holiday(now_ist.date()):
        logger.info(_safe_console_text(f"[PENDING SUMMARY] Skipped: Sunday/Holiday for {day_iso}"))
        return {"ok": True, "skipped": True, "reason": "sunday_or_holiday", "day": day_iso}

    cache_key = _pending_summary_day_key(day_iso)

    # atomic claim (prevents races)
    if not force:
        try:
            if not cache.add(cache_key, True, _pending_summary_ttl_seconds(now_ist)):
                logger.info(_safe_console_text(f"[PENDING SUMMARY] Skipped: already sent/claimed for {day_iso}"))
                return {"ok": True, "skipped": True, "reason": "already_sent", "day": day_iso}
        except Exception:
            # cache down -> best-effort fallback: continue (may duplicate)
            pass

    try:
        admins = get_admin_emails(exclude=None)
    except Exception:
        admins = []

    recipients = _dedupe_emails((admins or []))
    if not recipients:
        logger.warning(_safe_console_text("[PENDING SUMMARY] No recipients resolved; aborting send"))
        return {"ok": False, "skipped": True, "reason": "no_recipients", "day": day_iso}

    rows = _build_pending_rows()

    subject = f"Daily Pending Task Summary - {day_iso}"
    title = f"Daily Pending Task Summary ({day_iso})"

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
            to=recipients,
            fail_silently=False,
        )
        logger.info(_safe_console_text(
            f"[PENDING SUMMARY] Sent for {day_iso} to {len(recipients)} recipient(s); total_pending={len(rows)}"
        ))
        return {"ok": True, "skipped": False, "day": day_iso, "recipients": len(recipients), "total_pending": len(rows)}
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] Send failed for {day_iso}: {e}"))
        # Do NOT delete claim key; safer to avoid resend loops. Use force=True to resend intentionally.
        raise


# -----------------------------------------------------------------------------
# AUTO-UNBLOCK + PRE-10AM GEN
# -----------------------------------------------------------------------------
def auto_unblock_overdue_dailies(*, user_id: int | None = None, dry_run: bool = False) -> dict:
    today = timezone.localdate()

    qs = Checklist.objects.filter(
        mode="Daily",
        planned_date__date__lt=today,
        status="Pending",
    )
    if hasattr(Checklist, "is_skipped_due_to_leave"):
        qs = qs.filter(is_skipped_due_to_leave=False)

    if user_id:
        qs = qs.filter(assign_to_id=user_id)

    count = qs.count()
    if dry_run:
        logger.info(_safe_console_text(f"[UNBLOCK:DRY] Would complete {count} overdue daily rows (user_id={user_id})"))
        return {"affected": count, "user_id": user_id, "dry_run": True}

    now = timezone.now()

    has_completed_at = False
    try:
        has_completed_at = any(getattr(f, "name", None) == "completed_at" for f in Checklist._meta.get_fields())
    except Exception:
        has_completed_at = False

    with transaction.atomic():
        if has_completed_at:
            updated = qs.update(status="Completed", completed_at=now)
        else:
            updated = qs.update(status="Completed")

    logger.info(_safe_console_text(f"[UNBLOCK] Completed {updated} overdue daily rows (user_id={user_id})"))
    return {"affected": updated, "user_id": user_id, "dry_run": False}


def pre10am_unblock_and_generate(*, user_id: int | None = None) -> dict:
    res_unblock = auto_unblock_overdue_dailies(user_id=user_id, dry_run=False)
    res_gen = _generate_recurring_checklists_sync(user_id=user_id, dry_run=False)
    out = {"ok": True, "unblock": res_unblock, "generate": res_gen}
    logger.info(_safe_console_text(f"[PRE10] {out}"))
    return out


@shared_task(bind=True, max_retries=1, default_retry_delay=30)
def run_pre10am_unblock_and_generate(self, user_id: int | None = None) -> dict:
    return pre10am_unblock_and_generate(user_id=user_id)
