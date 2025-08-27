# apps/tasks/tasks.py
from __future__ import annotations

import logging
from datetime import timedelta

import pytz
from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from .models import Checklist
from .recurrence import (
    RECURRING_MODES,
    normalize_mode,
    get_next_planned_date,
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
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", False)


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _should_send_recur_email_now() -> bool:
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return False
    if not SEND_RECUR_EMAILS_ONLY_AT_10AM:
        return True
    return _within_10am_ist_window()


def _ensure_future_occurrence_for_series(series: dict, *, dry_run: bool = False) -> int:
    """
    Ensure exactly one future Pending exists for a recurring checklist 'series'.
    Series key fields: assign_to_id, task_name, mode, frequency, group_name.
    Rules:
      • Never generate if any Pending exists (past or future). Next only after completion.
      • Compute next from the latest Completed occurrence's planned_date.
      • Preserve planned time-of-day and skip Sundays/holidays (handled by get_next_planned_date()).
      • Dupe guard ±1 minute.
      • Send email to assignee for every generated occurrence.
    Returns: number of items created (0/1).
    """
    now = timezone.now()

    # If ANY pending exists in the series (even past-due), do NOT generate next.
    if Checklist.objects.filter(status="Pending", **series).exists():
        return 0

    # Need a completed seed to move forward
    completed = (
        Checklist.objects
        .filter(status="Completed", **series)
        .order_by("-planned_date", "-id")
        .first()
    )
    if not completed:
        # No completed item yet → nothing to generate.
        return 0

    # Compute next (preserves time, shifts to working day)
    next_dt = get_next_planned_date(completed.planned_date, series["mode"], series["frequency"] or 1)

    # Catch-up loop to strictly move into the future
    safety = 0
    while next_dt and next_dt <= now and safety < 730:  # ~2 years safety
        next_dt = get_next_planned_date(next_dt, series["mode"], series["frequency"] or 1)
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
        logger.info(_safe_console_text(
            f"[DRY RUN] Would create next checklist '{series['task_name']}' "
            f"for user_id={series['assign_to_id']} at {next_dt.astimezone(IST):%Y-%m-%d %H:%M IST}"
        ))
        return 0

    # Create + email
    with transaction.atomic():
        obj = Checklist.objects.create(
            assign_by=completed.assign_by,
            task_name=completed.task_name,
            message=completed.message,
            assign_to=completed.assign_to,
            planned_date=next_dt,  # PRESERVED time-of-day (delay computed against this)
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

    # Send assignee email (no admin confirmation for auto-recur)
    if _should_send_recur_email_now():
        try:
            complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
            send_checklist_assignment_to_user(
                task=obj,
                complete_url=complete_url,
                subject_prefix="Recurring Checklist Generated",
            )
        except Exception as e:
            logger.error(_safe_console_text(f"Email failure for checklist {obj.id}: {e}"))

    logger.info(_safe_console_text(
        f"✅ Created next recurring checklist {obj.id} '{obj.task_name}' for user_id={series['assign_to_id']} "
        f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
    ))
    return 1


@shared_task(bind=True, max_retries=2, default_retry_delay=10)
def generate_recurring_checklists(self, user_id: int | None = None, dry_run: bool = False) -> dict:
    """
    Idempotent generator for recurring CHECKLIST tasks.
    Safe to run hourly or daily (e.g., via celery beat or cron).

    • Scans distinct recurring 'series' (assign_to, task_name, mode, frequency, group_name).
    • For each series, generates NEXT only after the current is completed.
    • Preserves planned time-of-day; dashboards handle 10:00 IST visibility separately.
    • Logs per-user counts to help diagnose “only first occurrence created” issues.
    """
    now = timezone.now()
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

    # Helpful logs for user-wise verification (e.g., dinesh@ case)
    if per_user:
        for uid, count in per_user.items():
            logger.info(_safe_console_text(f"[RECUR GEN] user_id={uid} → created {count} next occurrence(s)"))
    else:
        logger.info(_safe_console_text(
            f"[RECUR GEN] No new items created at {now.astimezone(IST):%Y-%m-%d %H:%M IST} "
            f"(dry_run={dry_run}, user_id={user_id})"
        ))

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

    logger.info(_safe_console_text(f"[RECUR AUDIT] OK series: {ok}, Stuck series: {stuck}"))
    return {"ok": ok, "stuck": stuck, "details": details}
