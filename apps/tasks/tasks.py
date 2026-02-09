# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\tasks.py
from __future__ import annotations

import logging
import os  # ⬅️ added for cross-process file lock
from pathlib import Path  # ⬅️ added for lock directory
from datetime import timedelta, datetime, time as dt_time, date as dt_date
from typing import Tuple, List, Dict, Any

import pytz
from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db import connection  # for table introspection
from django.db.models import Q
from django.db.utils import OperationalError, ProgrammingError
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation, FMS, HelpTicket
# Final recurrence rules:
# get_next_planned_date → steps by mode/frequency and PINS to 19:00 IST on that stepped date.
# It does NOT shift Sundays/holidays. Shifting is performed by signals to avoid double-shifts.
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

# ✅ IMPORT FIX: digest lives in apps/tasks/pending_digest.py
from .pending_digest import (
    send_daily_employee_pending_digest,   # per-employee digest (one email per user)
    send_admin_all_pending_digest,        # single consolidated digest to admin
)

# ✅ LEAVE-BLOCKING GUARD: single source of truth used before any day-of fan-out
# If guard_assign(...) returns False, we MUST NOT "assign"/notify for that user at that time.
from apps.tasks.services.blocking import guard_assign

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email knobs
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)

# -------------------------------
# Cross-process lock (prevents duplicate fan-out across Celery/Web)
# -------------------------------
_LOCK_DIR = Path(getattr(settings, "MEDIA_ROOT", "/opt/render/project/src/db")) / "locks"

def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)

def _daily_mail_lock_path(for_dt_ist: datetime | None = None) -> Path:
    d = (for_dt_ist or _now_ist()).date().isoformat()
    return _LOCK_DIR / f"due_today_fanout_{d}.lock"

def _acquire_daily_mail_lock(for_dt_ist: datetime | None = None) -> str | None:
    """
    Create a filesystem sentinel that is shared by all processes (web + worker).
    If the file already exists, another runner has the lock for today's fan-out.
    """
    try:
        _LOCK_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        # If we cannot ensure dir, don't block execution; proceed without lock.
        return None
    lock_path = _daily_mail_lock_path(for_dt_ist)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return str(lock_path)
    except FileExistsError:
        return None
    except Exception:
        return None

def _release_daily_mail_lock(lock_path: str | None) -> None:
    try:
        if lock_path and os.path.exists(lock_path):
            os.unlink(lock_path)
    except Exception:
        # Best-effort; stale locks will naturally change with date.
        pass

# -------------------------------
# General IST helpers
# -------------------------------
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
    """End-of-today (23:59:59.999999) in IST, converted to project timezone."""
    now_ist = _now_ist()
    end_ist = IST.localize(datetime.combine(now_ist.date(), dt_time(23, 59, 59, 999999)))
    return end_ist.astimezone(timezone.get_current_timezone())


def _assignment_anchor_for_today_10am_ist() -> datetime:
    """
    The canonical 'assignment decision' instant for today: 10:00 IST.
    We pass THIS to guard_assign(...) so that:
      • Full-day leaves block the day
      • Half-day AM/PM leaves respect the 10:00 anchor (AM blocks, PM does not)
      • PENDING leaves apply only if requested before 09:30 IST of the day
    """
    today_ist = _now_ist().date()
    return IST.localize(datetime.combine(today_ist, dt_time(10, 0)))


def _is_after_10am_ist() -> bool:
    now_ist = _now_ist()
    return now_ist.time() >= dt_time(10, 0)


def _should_send_recur_email_now() -> bool:
    """
    With SEND_RECUR_EMAILS_ONLY_AT_10AM=True (default), we NEVER send an email
    here and rely entirely on the 10:00 IST fan-out (send_due_today_assignments)
    on the DUE DAY.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return False
    if SEND_RECUR_EMAILS_ONLY_AT_10AM:
        return False
    return True


# -------------------------------
# DB/table safety helper
# -------------------------------
def _table_exists_for_model(model) -> bool:
    """
    Runtime guard to avoid touching a table that may not exist yet in the current DB
    (migrations not applied / wrong DB file). This prevents cron crashes only.
    """
    try:
        db_table = model._meta.db_table
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
        return db_table in tables
    except Exception as e:
        logger.warning(_safe_console_text(f"[DB GUARD] Failed to introspect tables: {e}"))
        return False


# -------------------------------
# Recurrence generator (optional)
# -------------------------------
def _series_q_for_frequency(assign_to_id: int, task_name: str, mode: str, freq_norm: int, group_name: str | None):
    """
    Build a Q() that treats frequency=None as 1, so existing rows with NULL frequency
    are not orphaned from the series after normalization.

    This is the ONLY behavioral change, and it does NOT alter recurrence math/timings;
    it just makes the queries tolerant to legacy rows with frequency=NULL.
    """
    freq_set = [freq_norm, None]  # tolerate both exact freq and NULL (legacy)
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=freq_set)
    return q


def _ensure_future_occurrence_for_series(series: dict, *, dry_run: bool = False) -> int:
    now = timezone.now()

    # normalize freq (treat NULL as 1)
    freq_norm = max(int(series.get("frequency") or 1), 1)
    q_series = _series_q_for_frequency(
        assign_to_id=series["assign_to_id"],
        task_name=series["task_name"],
        mode=series["mode"],
        freq_norm=freq_norm,
        group_name=series.get("group_name"),
    )

    # If a FUTURE Pending exists in this tolerant series → do not create another.
    if (
        Checklist.objects.filter(status="Pending")
        .filter(q_series)
        .filter(planned_date__gt=now)
        .exists()
    ):
        return 0

    # Use latest COMPLETED in the tolerant series as the stepping base
    completed = (
        Checklist.objects.filter(status="Completed")
        .filter(q_series)
        .order_by("-planned_date", "-id")
        .first()
    )
    if not completed:
        return 0

    next_dt = get_next_planned_date(
        completed.planned_date, series["mode"], freq_norm
    )

    # Step forward until strictly after "now" (keeps recurrence strictly future)
    safety = 0
    while next_dt and next_dt <= now and safety < 730:
        next_dt = get_next_planned_date(next_dt, series["mode"], freq_norm)
        safety += 1
    if not next_dt:
        return 0

    # ⛔ CRITICAL FIX: never (re)create a *today-due* occurrence here.
    # This respects "recurrence is generated ONLY on completion, NOT on delete"."
    # If a user deletes today's checklist, this generator must *not* repopulate it.
    try:
        if next_dt.astimezone(IST).date() == _now_ist().date():
            logger.info(
                _safe_console_text(
                    f"[RECUR GEN] Suppressed creation for TODAY {next_dt.astimezone(IST):%Y-%m-%d} "
                    f"for series '{series['task_name']}' (assign_to_id={series['assign_to_id']})"
                )
            )
            return 0
    except Exception:
        # If timezone conversion fails for any reason, err on the side of not creating
        return 0

    # Dupe guard within ±1 minute (still in tolerant series)
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
            message=completed.message,
            assign_to=completed.assign_to,
            planned_date=next_dt,  # 19:00 IST pinned by recurrence_utils; shift is handled in signals
            priority=completed.priority,
            attachment_mandatory=completed.attachment_mandatory,
            mode=completed.mode,
            frequency=freq_norm,  # normalize going forward
            time_per_task_minutes=completed.time_per_task_minutes,
            remind_before_days=completed.remind_before_days,
            assign_pc=completed.assign_pc,
            notify_to=completed.notify_to,
            auditor=getattr(completed, "auditor", None),
            set_reminder=completed.set_reminder,
            reminder_mode=completed.reminder_mode,
            reminder_frequency=completed.reminder_frequency,
            reminder_starting_time=completed.reminder_starting_time,
            checklist_auto_close=completed.checklist_auto_close,
            checklist_auto_close_days=completed.checklist_auto_close_days,
            group_name=getattr(completed, "group_name", None),
            actual_duration_minutes=0,
            status="Pending",
        )

    if _should_send_recur_email_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix=f"Today’s Checklist – {obj.task_name}",
            )
        except Exception as e:
            logger.error(_safe_console_text(f"Email failure for checklist {obj.id}: {e}"))

    logger.info(
        _safe_console_text(
            f"Created next recurring checklist {obj.id} '{obj.task_name}' for user_id={series['assign_to_id']} "
            f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
        )
    )
    return 1


@shared_task(bind=True, max_retries=2, default_retry_delay=10)
def generate_recurring_checklists(self, user_id: int | None = None, dry_run: bool = False) -> dict:
    # NOTE: keep seeds broad; we normalize inside the loop and query with tolerant series Q
    filters = {"mode__in": RECURRING_MODES}
    if user_id:
        filters["assign_to_id"] = user_id

    seeds = (
        Checklist.objects.filter(**filters)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )

    created_total = 0
    per_user = {}

    for s in seeds:
        m = normalize_mode(s["mode"])
        if m not in RECURRING_MODES:
            continue

        # Normalize freq in-memory; DB lookups will tolerate NULL via frequency__in
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
                f"[RECUR GEN] No new items created at {_now_ist():%Y-%m-%d %H:%M IST} "
                f"(dry_run={dry_run}, user_id={user_id})"
            )
        )

    return {"created": created_total, "per_user": per_user, "dry_run": dry_run, "user_id": user_id}


@shared_task(bind=True)
def audit_recurring_health(self) -> dict:
    series = (
        Checklist.objects.filter(mode__in=RECURRING_MODES)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )
    stuck = 0
    ok = 0
    details = []

    for s in series:
        has_pending = Checklist.objects.filter(status="Pending", **s).exists()
        has_completed = Checklist.objects.filter(status="Completed", **s).exists()
        if not has_pending and not has_completed:
            stuck += 1
            details.append({"series": s, "state": "no_pending_no_completed"})
        else:
            ok += 1

    logger.info(_safe_console_text(f"[RECUR AUDIT] OK series: {ok}, Stuck series: {stuck}"))
    return {"ok": ok, "stuck": stuck, "details": details}


# -------------------------------
# 10:00 IST daily due mailer
# -------------------------------
def _sent_key(model: str, obj_id: int, day_ist_str: str) -> str:
    return f"due_mail_sent:{model}:{obj_id}:{day_ist_str}"


def _mark_sent_for_today(model: str, obj_id: int) -> None:
    today_ist = _now_ist().date().isoformat()
    key = _sent_key(model, obj_id, today_ist)
    now_ist = _now_ist()
    next3 = (now_ist + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    ttl_seconds = int((next3 - now_ist).total_seconds())
    cache.set(key, True, ttl_seconds)


def _already_sent_today(model: str, obj_id: int) -> bool:
    today_ist = _now_ist().date().isoformat()
    return bool(cache.get(_sent_key(model, obj_id, today_ist), False))


def _is_self_assigned(obj) -> bool:
    try:
        return bool(obj.assign_by_id and obj.assign_to_id and obj.assign_by_id == obj.assign_to_id)
    except Exception:
        return False


def _send_checklist_email(obj: Checklist) -> None:
    if _is_self_assigned(obj):
        logger.info(_safe_console_text(f"[DUE@10] Checklist {obj.id} skipped: assigner == assignee"))
        return

    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=f"Today’s Checklist – {obj.task_name}",
        )
        logger.info(_safe_console_text(f"[DUE@10] Checklist {obj.id} mailed to user_id={obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"[DUE@10] Checklist {obj.id} email failure: {e}"))


def _send_delegation_email(obj: Delegation) -> None:
    if _is_self_assigned(obj):
        logger.info(_safe_console_text(f"[DUE@10] Delegation {obj.id} skipped: assigner == assignee"))
        return

    try:
        try:
            from .utils import send_delegation_assignment_to_user  # type: ignore
            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            send_delegation_assignment_to_user(
                delegation=obj,
                complete_url=complete_url,
                subject_prefix=f"Today’s Delegation – {obj.task_name} (due 7 PM)",
            )
        except Exception:
            try:
                complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            except Exception:
                complete_url = SITE_URL
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix=f"Today’s Delegation – {obj.task_name} (due 7 PM)",
            )
        logger.info(_safe_console_text(f"[DUE@10] Delegation {obj.id} mailed to user_id={obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"[DUE@10] Delegation {obj.id} email failure: {e}"))


def _fetch_delegations_due_today(start_dt, end_dt):
    """
    Fetch delegation tasks due today.

    SAFETY: identical to checklist guard — skip gracefully if table is missing.
    """
    # ✅ Hard guard: avoid querying a non-existent table
    if not _table_exists_for_model(Delegation):
        logger.warning(_safe_console_text("[DUE@10] Delegation skipped: table 'tasks_delegation' not found"))
        return []

    try:
        qs = Delegation.objects.filter(status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt)
        if qs.exists():
            return list(qs)

        today_ist = _now_ist().date()
        qs2 = Delegation.objects.filter(status="Pending", planned_date__date=today_ist)
        return list(qs2)
    except (OperationalError, ProgrammingError) as e:
        logger.warning(_safe_console_text(f"[DUE@10] Delegation skipped (DB not ready): {e}"))
        return []
    except Exception:
        try:
            return list(qs)  # type: ignore[name-defined]
        except Exception:
            return []


def _fetch_checklists_due_today(start_dt, end_dt):
    """
    Fetch checklist tasks due today.

    IMPORTANT SAFETY:
    If the table `tasks_checklist` does not exist, skip gracefully and continue.
    """
    # ✅ Hard guard: skip querying if table does not exist
    if not _table_exists_for_model(Checklist):
        logger.warning(_safe_console_text("[DUE@10] Checklist skipped: table 'tasks_checklist' not found"))
        return []

    try:
        qs = Checklist.objects.filter(status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt)
        if qs.exists():
            return list(qs)

        today_ist = _now_ist().date()
        qs2 = Checklist.objects.filter(status="Pending", planned_date__date=today_ist)
        return list(qs2)

    except (OperationalError, ProgrammingError) as e:
        logger.warning(_safe_console_text(f"[DUE@10] Checklist skipped (DB not ready): {e}"))
        return []

    except Exception:
        try:
            return list(qs)  # type: ignore[name-defined]
        except Exception:
            return []


# -------------------------------
# Consolidated checklist mail (per employee)
# -------------------------------
def _emp_day_key(user_id: int, day_iso: str) -> str:
    return f"due_today_checklist_digest_sent:emp:{user_id}:{day_iso}"


def _mark_emp_digest_sent(user_id: int) -> None:
    day_iso = _now_ist().date().isoformat()
    key = _emp_day_key(user_id, day_iso)
    now_ist = _now_ist()
    # expire at ~03:00 IST next day (or at least 6 hours)
    next3 = (now_ist + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    ttl_seconds = max(int((next3 - now_ist).total_seconds()), 6 * 60 * 60)
    cache.set(key, True, ttl_seconds)


def _already_sent_emp_digest(user_id: int) -> bool:
    day_iso = _now_ist().date().isoformat()
    return bool(cache.get(_emp_day_key(user_id, day_iso), False))


def _rows_for_checklists(objs: List[Checklist]) -> List[Dict[str, Any]]:
    """
    Build rows for the summary template matching the style used by pending_digest.
    """
    rows: List[Dict[str, Any]] = []
    for obj in objs:
        title = obj.task_name or ""
        desc = (getattr(obj, "message", "") or "").strip()
        title_desc = title if not desc else f"{title} — {desc}"
        rows.append(
            {
                "task_id": f"CL-{obj.id}",
                "task_title": title_desc,
                "assigned_to": getattr(getattr(obj, "assign_to", None), "get_full_name", lambda: "")() or getattr(getattr(obj, "assign_to", None), "username", "") or getattr(getattr(obj, "assign_to", None), "email", "") or "-",
                "assigned_by": getattr(getattr(obj, "assign_by", None), "get_full_name", lambda: "")() or getattr(getattr(obj, "assign_by", None), "username", "") or getattr(getattr(obj, "assign_by", None), "email", "") or "-",
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
    10:00 IST fan-out of "due-today" notifications.

    🚫 LEAVE AWARE: before emailing each assignee, we call guard_assign(user, 10:00 IST today).
    If it returns False, we **skip** notifying that user for the day because they are on
    APPROVED leave or a qualifying PENDING leave (applied before 09:30 IST).

    🔒 DUPLICATE GUARD (cross-process):
    We take a same-day filesystem lock so that only one runner (Celery or HTTP cron)
    performs the fan-out per day, even if both are triggered.

    ✅ BEHAVIORAL CHANGE (Checklists only):
    Instead of sending one email per checklist, we now send ONE consolidated email
    per employee containing ALL of their *today-due* Pending checklists.
    Delegations continue to be sent per task as before.
    """
    # Gate by time first (so we don't take lock before 10:00)
    if not _is_after_10am_ist():
        logger.info(_safe_console_text("[DUE@10] Skipped: before 10:00 IST"))
        return {"sent": 0, "checklists": 0, "delegations": 0, "skipped_before_10": True}

    # Acquire a per-day cross-process lock
    lock_path = _acquire_daily_mail_lock(_now_ist())
    if not lock_path:
        logger.info(_safe_console_text("[DUE@10] Skipped: another instance already ran or is running"))
        return {"sent": 0, "checklists": 0, "delegations": 0, "locked_out": True, "skipped_before_10": False}

    try:
        now_ist = _now_ist()
        start_dt, end_dt = _ist_day_bounds(now_ist)
        anchor_dt = _assignment_anchor_for_today_10am_ist()

        # Strictly today's PENDING items
        checklists = _fetch_checklists_due_today(start_dt, end_dt)
        delegations = _fetch_delegations_due_today(start_dt, end_dt)

        sent_total = 0

        # -----------------------------
        # CHECKLISTS: Consolidate per employee
        # -----------------------------
        # Group non-self-assigned checklists by assignee
        per_user: Dict[int, List[Checklist]] = {}
        for obj in checklists:
            if _is_self_assigned(obj):
                logger.info(_safe_console_text(f"[DUE@10] Checklist {obj.id} skipped in digest: assigner == assignee"))
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
            # Resolve the fresh user object (assignee)
            try:
                user = items[0].assign_to
            except Exception:
                user = None
            if not user:
                continue

            # Leave guard once per employee at the 10:00 anchor
            try:
                if not guard_assign(user, anchor_dt):
                    logger.info(_safe_console_text(f"[DUE@10] Checklist digest suppressed for user_id={getattr(user,'id','?')} (on leave @ 10:00 IST)"))
                    continue
            except Exception:
                pass

            # Idempotency: one checklist digest per employee per day
            try:
                if _already_sent_emp_digest(uid):
                    logger.info(_safe_console_text(f"[DUE@10] Checklist digest already sent for user_id={uid}; skip"))
                    continue
            except Exception:
                pass

            # Recipient resolution (assignee only)
            try:
                email = (getattr(user, "email", "") or "").strip()
            except Exception:
                email = ""
            to_list = _dedupe_emails([email]) if email else []
            if not to_list:
                logger.info(_safe_console_text(f"[DUE@10] Skip checklist digest for user_id={uid} – no email"))
                continue

            rows = _rows_for_checklists(items)
            if not rows:
                continue

            day_iso = now_ist.date().isoformat()
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
                        # Note: no special category; utils will apply D1–D4 guards if needed.
                    },
                    to=to_list,
                    fail_silently=False,
                )
                _mark_emp_digest_sent(uid)
                cl_emails_sent += 1
                cl_tasks_included += len(rows)
                sent_total += 1
                logger.info(_safe_console_text(f"[DUE@10] Sent consolidated checklist digest to user_id={uid} items={len(rows)}"))
            except Exception as e:
                logger.error(_safe_console_text(f"[DUE@10] Checklist digest email failure for user_id={uid}: {e}"))

        # -----------------------------
        # DELEGATIONS: unchanged (per-task)
        # -----------------------------
        de_sent = 0
        for obj in delegations:
            # ⛔ Do not send if user is blocked for 10:00 IST today
            try:
                if not guard_assign(obj.assign_to, anchor_dt):
                    logger.info(
                        _safe_console_text(
                            f"[DUE@10] Delegation {obj.id} suppressed (assignee on leave @ 10:00 IST)"
                        )
                    )
                    continue
            except Exception:
                pass

            if _already_sent_today("Delegation", obj.id):
                continue
            _send_delegation_email(obj)
            _mark_sent_for_today("Delegation", obj.id)
            sent_total += 1
            de_sent += 1

        logger.info(
            _safe_console_text(
                f"[DUE@10] Completed fan-out at {now_ist:%Y-%m-%d %H:%M IST}: "
                f"checklist_emails={cl_emails_sent} (tasks_included={cl_tasks_included}), "
                f"delegations={de_sent}, total_emails={sent_total}"
            )
        )
        return {
            "sent": sent_total,
            "checklists_emails": cl_emails_sent,
            "checklists_tasks": cl_tasks_included,
            "delegations": de_sent,
            "skipped_before_10": False,
        }
    finally:
        _release_daily_mail_lock(lock_path)


# -------------------------------
# Delegation reminders (every 5 minutes)
# -------------------------------
def _delegation_reminder_lock_key(obj_id: int) -> str:
    return f"delegation_reminder_lock:{obj_id}"


def _send_delegation_reminder_email(obj: Delegation) -> None:
    """
    Reminder email rules:
      - Never email assigner if self-assigned.
      - Respect leave blocking at the 10:00 IST anchor of TODAY.
    """
    if _is_self_assigned(obj):
        logger.info(_safe_console_text(f"[DL REM] Delegation {obj.id} skipped: assigner == assignee"))
        return

    # ⛔ Skip reminders if the assignee is on leave today per 10:00 IST anchor
    try:
        if not guard_assign(obj.assign_to, _assignment_anchor_for_today_10am_ist()):
            logger.info(_safe_console_text(f"[DL REM] Delegation {obj.id} suppressed (assignee on leave today)"))
            return
    except Exception:
        pass

    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
    except Exception:
        complete_url = SITE_URL

    # Preferred helper, with fallback:
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

    try:
        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=f"Reminder – Delegation – {obj.task_name}",
        )
    except Exception as e:
        raise e


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
    ).order_by("reminder_time", "id")

    for obj in qs:
        if not obj.reminder_time:
            skipped += 1
            continue

        lock_key = _delegation_reminder_lock_key(obj.id)
        if not cache.add(lock_key, True, 10 * 60):  # 10 minutes
            continue

        claim_ts = timezone.now()

        try:
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
            try:
                Delegation.objects.filter(id=obj.id, reminder_sent_at=claim_ts).update(reminder_sent_at=None)
            except Exception:
                pass

        finally:
            pass

    return {"sent": sent, "skipped": skipped, "failed": failed}


# -------------------------------
# Daily Pending Task Summary (Admin Report)
# -------------------------------
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
        return bool(Holiday.is_holiday(d))
    except Exception:
        return False


def _build_pending_rows() -> List[Dict[str, Any]]:
    """
    Build rows for the admin consolidated summary.
    IMPORTANT: include only tasks due till the end of today (IST), or without a due date.
    """
    rows: List[Dict[str, Any]] = []
    end_today = _end_of_today_ist_in_project_tz()

    # Checklist (Pending only; due <= today OR no due date)
    try:
        qs = (
            Checklist.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.task_name or ""
            desc = (obj.message or "").strip()
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

    # Delegation (Pending only; due <= today OR no due date)
    try:
        qs = (
            Delegation.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
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

    # FMS (Pending only; due <= today OR no due date)
    try:
        qs = (
            FMS.objects.filter(status="Pending")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            due = ""
            try:
                due = obj.planned_date.isoformat() if getattr(obj, "planned_date", None) else ""
            except Exception:
                due = str(getattr(obj, "planned_date", "") or "")
            rows.append(
                {
                    "task_id": f"FMS-{obj.id}",
                    "task_title": obj.task_name or "",
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": due,
                    "task_type": "FMS",
                    "status": "Pending",
                }
            )
    except Exception as e:
        logger.error(_safe_console_text(f"[PENDING SUMMARY] FMS fetch failed: {e}"))

    # Help Ticket (not closed; due <= today OR no due date)
    try:
        qs = (
            HelpTicket.objects.exclude(status="Closed")
            .filter(Q(planned_date__isnull=True) | Q(planned_date__lte=end_today))
            .select_related("assign_to", "assign_by")
            .order_by("planned_date", "id")
        )
        for obj in qs:
            title = obj.title or ""
            desc = (obj.description or "").strip()
            title_desc = title if not desc else f"{title} — {desc}"
            rows.append(
                {
                    "task_id": f"HT-{obj.id}",
                    "task_title": title_desc,
                    "assigned_to": obj.assign_to,
                    "assigned_by": obj.assign_by,
                    "due_date": _fmt_dt_date(getattr(obj, "planned_date", None)),
                    "task_type": "Help Ticket",
                    "status": "Pending",
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
    if not force and cache.get(cache_key):
        logger.info(_safe_console_text(f"[PENDING SUMMARY] Skipped: already sent for {day_iso}"))
        return {"ok": True, "skipped": True, "reason": "already_sent", "day": day_iso}

    try:
        admins = get_admin_emails(exclude=None)
    except Exception:
        admins = []

    # ⛔ D1/D4: Do NOT force-add Pankaj here. Central utils filtering also protects other paths.
    recipients = _dedupe_emails((admins or []))
    if not recipients:
        logger.warning(_safe_console_text("[PENDING SUMMARY] No recipients resolved; aborting send"))
        return {"ok": False, "skipped": True, "reason": "no_recipients", "day": day_iso}

    rows = _build_pending_rows()

    subject = f"Daily Pending Task Summary - {day_iso}"
    title = f"Daily Pending Task Summary ({day_iso})"

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

    cache.set(cache_key, True, _pending_summary_ttl_seconds(now_ist))
    logger.info(_safe_console_text(f"[PENDING SUMMARY] Sent for {day_iso} to {len(recipients)} recipient(s); total_pending={len(rows)}"))

    return {"ok": True, "skipped": False, "day": day_iso, "recipients": len(recipients), "total_pending": len(rows)}


# =========================
# AUTO-UNBLOCK + PRE-10AM GEN (NEW)
# =========================
def auto_unblock_overdue_dailies(*, user_id: int | None = None, dry_run: bool = False) -> dict:
    """
    System safeguard:
      - Finds DAILY checklists with planned_date < today AND status='Pending'
      - Marks them COMPLETED (admin/system action) to unblock the next recurrence

    Preserves rule: "next occurrence spawns only on completion".
    """
    today = timezone.localdate()

    qs = Checklist.objects.filter(
        mode="Daily",
        planned_date__date__lt=today,
        status="Pending",
    )
    if user_id:
        qs = qs.filter(assign_to_id=user_id)

    count = qs.count()
    if dry_run:
        logger.info(_safe_console_text(f"[UNBLOCK:DRY] Would complete {count} overdue daily rows (user_id={user_id})"))
        return {"affected": count, "user_id": user_id, "dry_run": True}

    with transaction.atomic():
        updated = qs.update(status="Completed")
    logger.info(_safe_console_text(f"[UNBLOCK] Completed {updated} overdue daily rows (user_id={user_id})"))
    return {"affected": updated, "user_id": user_id, "dry_run": False}


def pre10am_unblock_and_generate(*, user_id: int | None = None) -> dict:
    """
    Runs just before 10:00 AM IST:
      1) auto_unblock_overdue_dailies (real)
      2) generate_recurring_checklists (real)
    """
    res_unblock = auto_unblock_overdue_dailies(user_id=user_id, dry_run=False)
    res_gen = generate_recurring_checklists.run(dry_run=False, user_id=user_id)
    out = {"ok": True, "unblock": res_unblock, "generate": res_gen}
    logger.info(_safe_console_text(f"[PRE10] {out}"))
    return out


# ✅ Celery wrapper so Beat can schedule at 09:55 IST
@shared_task(bind=True, max_retries=1, default_retry_delay=30)
def run_pre10am_unblock_and_generate(self, user_id: int | None = None) -> dict:
    return pre10am_unblock_and_generate(user_id=user_id)
