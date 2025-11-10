from __future__ import annotations

import logging
from datetime import timedelta, datetime, time as dt_time
from typing import Tuple

import pytz
from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation
# ✅ Final recurrence rules (WORKING-DAY SHIFT; 19:00 IST on next stepped working day)
from .recurrence_utils import (
    RECURRING_MODES,
    normalize_mode,
    get_next_planned_date,  # shifts Sun/holiday → next working day @ 19:00 IST
)
from .utils import (
    _safe_console_text,
    send_checklist_assignment_to_user,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email knobs
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


# -------------------------------
# General IST helpers
# -------------------------------
def _now_ist() -> datetime:
    return timezone.now().astimezone(IST)


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


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    now_ist = _now_ist()
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (
        anchor + timedelta(minutes=leeway_minutes)
    )


def _is_after_10am_ist() -> bool:
    now_ist = _now_ist()
    return now_ist.time() >= dt_time(10, 0)


def _should_send_recur_email_now() -> bool:
    """
    Controls immediate email at the moment a new recurring instance is generated.

    With SEND_RECUR_EMAILS_ONLY_AT_10AM=True (default), we NEVER send an email
    here and rely entirely on the 10:00 IST fan-out (send_due_today_assignments)
    on the DUE DAY. This matches the client's requirement:
    - Emails for recurring checklists go only at 10 AM on the planned day.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return False
    if SEND_RECUR_EMAILS_ONLY_AT_10AM:
        return False
    # If ever allowed, we’d send immediately on creation.
    return True


# -------------------------------
# Recurrence generator (optional)
# -------------------------------
def _ensure_future_occurrence_for_series(series: dict, *, dry_run: bool = False) -> int:
    """
    Ensure exactly one future Pending exists for a recurring checklist 'series'.
    Series key fields: assign_to_id, task_name, mode, frequency, group_name.

    Rules:
      • Never generate if any Pending exists (past or future). Next only after completion.
      • Compute next from the latest Completed occurrence's planned_date.
      • Next planned is **19:00 IST** on the next working day (Sun/holidays shifted).
      • Dupe guard ±1 minute.
      • Emails are normally NOT sent here; 10:00 AM fan-out handles due-day emails.

    Returns: number of items created (0/1).
    """
    now = timezone.now()

    # If ANY pending exists in the series (even past-due), do NOT generate next.
    if Checklist.objects.filter(status="Pending", **series).exists():
        return 0

    # Need a completed seed to move forward
    completed = (
        Checklist.objects.filter(status="Completed", **series)
        .order_by("-planned_date", "-id")
        .first()
    )
    if not completed:
        # No completed item yet → nothing to generate.
        return 0

    # Compute next (19:00 IST + shift to working day)
    next_dt = get_next_planned_date(
        completed.planned_date, series["mode"], series["frequency"] or 1
    )

    # Catch-up loop to strictly move into the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:  # ~2 years safety
        next_dt = get_next_planned_date(
            next_dt, series["mode"], series["frequency"] or 1
        )
        safety += 1
    if not next_dt:
        return 0

    # Dupe guard (±1 minute)
    dupe = Checklist.objects.filter(
        planned_date__gte=next_dt - timedelta(minutes=1),
        planned_date__lt=next_dt + timedelta(minutes=1),
        status="Pending",
        **series,
    ).exists()
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

    # Create (emails handled separately by 10 AM job)
    with transaction.atomic():
        obj = Checklist.objects.create(
            assign_by=completed.assign_by,
            task_name=completed.task_name,
            message=completed.message,
            assign_to=completed.assign_to,
            planned_date=next_dt,  # 19:00 IST (shifted to working day)
            priority=completed.priority,
            attachment_mandatory=completed.attachment_mandatory,
            mode=completed.mode,
            frequency=completed.frequency,
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

    # Usually we skip immediate sends and rely on 10:00 fan-out.
    if _should_send_recur_email_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix=f"Today’s Checklist – {obj.task_name}",
            )
        except Exception as e:
            logger.error(
                _safe_console_text(f"Email failure for checklist {obj.id}: {e}")
            )

    logger.info(
        _safe_console_text(
            f"✅ Created next recurring checklist {obj.id} '{obj.task_name}' for user_id={series['assign_to_id']} "
            f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
        )
    )
    return 1


@shared_task(bind=True, max_retries=2, default_retry_delay=10)
def generate_recurring_checklists(
    self, user_id: int | None = None, dry_run: bool = False
) -> dict:
    """
    Idempotent generator for recurring CHECKLIST tasks.
    Safe to run hourly or daily (e.g., via celery beat or cron).

    NOTE: If you've enabled auto-creation on completion via signals,
          this task is optional / can serve as a safety net.
    """
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
        # Normalize and sanity-check
        m = normalize_mode(s["mode"])
        if m not in RECURRING_MODES:
            continue
        s["mode"] = m
        s["frequency"] = max(int(s.get("frequency") or 1), 1)

        # Create next if eligible
        created = _ensure_future_occurrence_for_series(s, dry_run=dry_run)
        created_total += created
        if created:
            per_user[s["assign_to_id"]] = per_user.get(s["assign_to_id"], 0) + created

    # Helpful logs for user-wise verification
    if per_user:
        for uid, count in per_user.items():
            logger.info(
                _safe_console_text(
                    f"[RECUR GEN] user_id={uid} → created {count} next occurrence(s)"
                )
            )
    else:
        logger.info(
            _safe_console_text(
                f"[RECUR GEN] No new items created at {_now_ist():%Y-%m-%d %H:%M IST} "
                f"(dry_run={dry_run}, user_id={user_id})"
            )
        )

    return {"created": created_total, "per_user": per_user, "dry_run": dry_run, "user_id": user_id}


@shared_task(bind=True)
def audit_recurring_health(self) -> dict:
    """
    Quick health check: counts series with pending items vs. series stuck (no pending & no completed).
    Emits logs you can filter per user to ensure consistency across the org.
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
        has_pending = Checklist.objects.filter(status="Pending", **s).exists()
        has_completed = Checklist.objects.filter(status="Completed", **s).exists()
        if not has_pending and not has_completed:
            stuck += 1
            details.append({"series": s, "state": "no_pending_no_completed"})
        else:
            ok += 1

    logger.info(
        _safe_console_text(f"[RECUR AUDIT] OK series: {ok}, Stuck series: {stuck}")
    )
    return {"ok": ok, "stuck": stuck, "details": details}


# -------------------------------
# 10:00 IST daily due mailer
# -------------------------------
def _sent_key(model: str, obj_id: int, day_ist_str: str) -> str:
    return f"due_mail_sent:{model}:{obj_id}:{day_ist_str}"


def _mark_sent_for_today(model: str, obj_id: int) -> None:
    today_ist = _now_ist().date().isoformat()
    key = _sent_key(model, obj_id, today_ist)
    # expire at next 03:00 IST
    now_ist = _now_ist()
    next3 = (now_ist + timedelta(days=1)).replace(
        hour=3, minute=0, second=0, microsecond=0
    )
    ttl_seconds = int((next3 - now_ist).total_seconds())
    cache.set(key, True, ttl_seconds)


def _already_sent_today(model: str, obj_id: int) -> bool:
    today_ist = _now_ist().date().isoformat()
    return bool(cache.get(_sent_key(model, obj_id, today_ist), False))


def _send_checklist_email(obj: Checklist) -> None:
    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        # SUBJECT updated
        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=f"Today’s Checklist – {obj.task_name}",
        )
        logger.info(
            _safe_console_text(
                f"[DUE@10] Checklist {obj.id} mailed to user_id={obj.assign_to_id}"
            )
        )
    except Exception as e:
        logger.error(
            _safe_console_text(f"[DUE@10] Checklist {obj.id} email failure: {e}")
        )


def _send_delegation_email(obj) -> None:
    """
    Send delegation assignment; if helper is missing, reuse checklist sender with different subject.
    """
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
            from .utils import send_checklist_assignment_to_user  # fallback

            try:
                complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            except Exception:
                complete_url = SITE_URL
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix=f"Today’s Delegation – {obj.task_name} (due 7 PM)",
            )
        logger.info(
            _safe_console_text(
                f"[DUE@10] Delegation {obj.id} mailed to user_id={obj.assign_to_id}"
            )
        )
    except Exception as e:
        logger.error(
            _safe_console_text(f"[DUE@10] Delegation {obj.id} email failure: {e}")
        )


def _send_delegation_reminder(obj: Delegation) -> None:
    """
    Send reminder for a delegated task (email + optional in-app notification).

    Message:
      "Reminder: Your delegated task [Task Title] is still pending.
       Please complete it before the due date."
    """
    reminder_message = (
        f"Reminder: Your delegated task '{obj.task_name}' is still pending. "
        f"Please complete it before the due date."
    )

    # Optional in-app notification (safe, optional – won’t break if app/model differ)
    try:
        from apps.notifications.models import Notification  # type: ignore
    except Exception:
        Notification = None  # type: ignore[assignment]

    if Notification and obj.assign_to_id:
        try:
            Notification.objects.create(
                user=obj.assign_to,
                title="Pending Delegated Task Reminder",
                message=reminder_message,
            )
        except Exception as e:
            logger.error(
                _safe_console_text(
                    f"[REMINDER] Failed to create in-app notification for Delegation "
                    f"{obj.id}: {e}"
                )
            )

    # Email reminder
    try:
        try:
            from .utils import send_delegation_assignment_to_user  # type: ignore

            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            send_delegation_assignment_to_user(
                delegation=obj,
                complete_url=complete_url,
                subject_prefix="Pending Delegated Task Reminder",
            )
        except Exception:
            # Fallback: reuse checklist email helper
            try:
                complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            except Exception:
                complete_url = SITE_URL
            send_checklist_assignment_to_user(
                task=obj,  # type: ignore[arg-type]
                complete_url=complete_url,
                subject_prefix="Pending Delegated Task Reminder",
            )

        logger.info(
            _safe_console_text(
                f"[REMINDER] Delegation reminder sent for {obj.id} to user_id={obj.assign_to_id}"
            )
        )
    except Exception as e:
        logger.error(
            _safe_console_text(
                f"[REMINDER] Delegation {obj.id} reminder email failure: {e}"
            )
        )


def _fetch_delegations_due_today(start_dt, end_dt):
    """
    Import lazily to avoid circulars. Works whether planned_date is DateTimeField or
    you filter by date equivalently.
    """
    try:
        from .models import Delegation  # type: ignore
    except Exception:
        return []

    # Primary: DateTimeField window
    qs = Delegation.objects.filter(
        status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt
    )
    if qs.exists():
        return list(qs)

    # Fallback: filter by IST date equality if schema differs
    try:
        today_ist = _now_ist().date()
        qs2 = Delegation.objects.filter(
            status="Pending", planned_date__date=today_ist
        )
        return list(qs2)
    except Exception:
        return list(qs)


def _fetch_checklists_due_today(start_dt, end_dt):
    """
    Same resilience for Checklist: handle DateTimeField or DateField-like filtering.
    """
    # Primary: DateTimeField window
    qs = Checklist.objects.filter(
        status="Pending", planned_date__gte=start_dt, planned_date__lte=end_dt
    )
    if qs.exists():
        return list(qs)

    # Fallback by date equality
    try:
        today_ist = _now_ist().date()
        qs2 = Checklist.objects.filter(
            status="Pending", planned_date__date=today_ist
        )
        return list(qs2)
    except Exception:
        return list(qs)


@shared_task(bind=True, max_retries=2, default_retry_delay=30)
def send_due_today_assignments(self) -> dict:
    """
    Send assignment emails for *today's* Checklist & Delegation at/after 10:00 IST.
    Re-entrant & de-duplicated per item per day via cache keys.
    Safe to run every few minutes between 10:00–10:10 IST, or hourly.
    """
    if not _is_after_10am_ist():
        logger.info(_safe_console_text("[DUE@10] Skipped: before 10:00 IST"))
        return {
            "sent": 0,
            "checklists": 0,
            "delegations": 0,
            "skipped_before_10": True,
        }

    now_ist = _now_ist()
    start_dt, end_dt = _ist_day_bounds(now_ist)

    # Checklists due today, still pending
    checklists = _fetch_checklists_due_today(start_dt, end_dt)
    # Delegations due today, still pending
    delegations = _fetch_delegations_due_today(start_dt, end_dt)

    sent = 0
    cl_sent = 0
    de_sent = 0

    # Checklist fan-out
    for obj in checklists:
        if _already_sent_today("Checklist", obj.id):
            continue
        _send_checklist_email(obj)
        _mark_sent_for_today("Checklist", obj.id)
        sent += 1
        cl_sent += 1

    # Delegation fan-out
    for obj in delegations:
        if _already_sent_today("Delegation", obj.id):
            continue
        _send_delegation_email(obj)
        _mark_sent_for_today("Delegation", obj.id)
        sent += 1
        de_sent += 1

    logger.info(
        _safe_console_text(
            f"[DUE@10] Completed fan-out at {now_ist:%Y-%m-%d %H:%M IST}: "
            f"checklists={cl_sent}, delegations={de_sent}, total={sent}"
        )
    )
    return {
        "sent": sent,
        "checklists": cl_sent,
        "delegations": de_sent,
        "skipped_before_10": False,
    }


# -------------------------------
# Delegation reminder scheduler
# -------------------------------
@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def dispatch_delegation_reminders(self) -> dict:
    """
    Periodic task to send reminders for Delegation items that:

      • have set_reminder = True
      • have a non-null reminder_time
      • reminder_time <= now
      • are still Pending
      • have not already fired (reminder_sent_at is null)

    Safe to run every few minutes via Celery beat or cron.
    """
    now = timezone.now()
    pending_qs = (
        Delegation.objects.filter(
            set_reminder=True,
            status="Pending",
            reminder_time__isnull=False,
            reminder_time__lte=now,
            reminder_sent_at__isnull=True,
        )
        .select_related("assign_to")
        .order_by("reminder_time", "id")
    )

    count = 0
    for obj in pending_qs:
        _send_delegation_reminder(obj)
        obj.reminder_sent_at = now
        obj.save(update_fields=["reminder_sent_at"])
        count += 1

    if count:
        logger.info(
            _safe_console_text(
                f"[REMINDER] Dispatched {count} delegation reminder(s) at "
                f"{_now_ist():%Y-%m-%d %H:%M IST}"
            )
        )
    else:
        logger.info(
            _safe_console_text("[REMINDER] No delegation reminders due at this run")
        )

    return {"sent": count}
