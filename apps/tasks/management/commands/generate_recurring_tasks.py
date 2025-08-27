from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date

import pytz
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.recurrence import (
    normalize_mode,
    is_working_day,
    next_working_day,
    RECURRING_MODES,
)
from apps.tasks.utils import (
    _safe_console_text,
    send_checklist_assignment_to_user,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email policy knobs (match signals/tasks defaults)
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
# If True → only send during ~10:00 IST window; if False → send immediately on generation
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", False)


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    """
    True if now (IST) is within [10:00 - leeway, 10:00 + leeway].
    Default window: 09:55–10:05 IST.
    """
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _send_recur_email(obj: Checklist) -> None:
    """Send assignee-only email for a generated recurrence (no admin CC)."""
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return
    if SEND_RECUR_EMAILS_ONLY_AT_10AM and not _within_10am_ist_window():
        logger.info(_safe_console_text(f"Skip recur email for CL-{obj.id}: outside 10:00 IST window"))
        return
    try:
        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix="Recurring Checklist Generated",
        )
        logger.info(_safe_console_text(f"Sent recurring email for CL-{obj.id} to {obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Failed to send recurring email for CL-{obj.id}: {e}"))


def _next_preserve_time(prev_dt: datetime, mode: str, freq: int, *, end_date: date | None) -> datetime | None:
    """
    Compute next occurrence while PRESERVING wall-clock time from prev_dt.
    If the computed date is Sunday/holiday, push FORWARD to the next working day at the SAME time.
    If `end_date` is set and next date would be AFTER it, return None.
    Returns aware datetime in project timezone.
    """
    m = normalize_mode(mode)
    if m not in RECURRING_MODES or not prev_dt:
        return None

    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, timezone.get_current_timezone())
    prev_ist = prev_dt.astimezone(IST)
    t = dt_time(prev_ist.hour, prev_ist.minute, prev_ist.second, prev_ist.microsecond)

    step = max(int(freq or 1), 1)
    if m == "Daily":
        nxt_ist = prev_ist + relativedelta(days=step)
    elif m == "Weekly":
        nxt_ist = prev_ist + relativedelta(weeks=step)
    elif m == "Monthly":
        nxt_ist = prev_ist + relativedelta(months=step)
    else:  # Yearly
        nxt_ist = prev_ist + relativedelta(years=step)

    # Reapply preserved time-of-day
    nxt_ist = nxt_ist.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)

    # Shift forward if non-working
    d = nxt_ist.date()
    if not is_working_day(d):
        d = next_working_day(d)
        nxt_ist = IST.localize(datetime.combine(d, t))

    if end_date and d > end_date:
        return None

    return nxt_ist.astimezone(timezone.get_current_timezone())


class Command(BaseCommand):
    help = (
        "Generate next occurrences for recurring CHECKLIST tasks only.\n"
        "• Next created ONLY AFTER the current one is completed (no pending in series).\n"
        "• Preserves the exact planned time-of-day; Sunday/holiday → next working day at SAME time.\n"
        "• Respects optional Checklist.recurrence_end_date.\n"
        "• Sends email to assignee on every recurrence (admin not CC'd).\n"
        "• Adds per-user debug logs to verify consistency."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--user-id",
            type=int,
            help="Limit to a specific assignee (user id).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be created without writing to the database.",
        )
        parser.add_argument(
            "--no-email",
            action="store_true",
            help="Skip sending emails for created items.",
        )

    def handle(self, *args, **opts):
        user_id = opts.get("user_id")
        dry_run = bool(opts.get("dry_run", False))
        send_emails = not bool(opts.get("no_email", False))
        now = timezone.now()

        created_total = 0
        per_user: dict[int, int] = {}

        # Distinct recurring series seeds
        filters = {"mode__in": RECURRING_MODES}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        for s in seeds:
            series_key = dict(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            # If ANY pending exists in the series (past or future), DO NOT create next.
            if Checklist.objects.filter(status="Pending", **series_key).exists():
                logger.debug(_safe_console_text(f"Series has pending; skip: {series_key}"))
                continue

            # Need the latest COMPLETED as the seed for the next occurrence
            last_completed = (
                Checklist.objects.filter(status="Completed", **series_key)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not last_completed:
                # No completed yet → initial item exists or not completed; skip.
                logger.debug(_safe_console_text(f"No completed seed; skip: {series_key}"))
                continue

            end_date = getattr(last_completed, "recurrence_end_date", None)

            # Compute next while preserving time and honoring end_date
            next_dt = _next_preserve_time(last_completed.planned_date, s["mode"], s["frequency"] or 1, end_date=end_date)

            # Catch-up to the future (if computed <= now), safety ~2 years
            safety = 0
            while next_dt and next_dt <= now and safety < 730:
                next_dt = _next_preserve_time(next_dt, s["mode"], s["frequency"] or 1, end_date=end_date)
                safety += 1

            if not next_dt:
                logger.info(_safe_console_text(f"End or no future date reachable; skip: {series_key}"))
                continue

            # Dupe guard (±1 minute around next_dt)
            dupe = Checklist.objects.filter(
                status="Pending",
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
                **series_key,
            ).exists()
            if dupe:
                logger.debug(_safe_console_text(f"Duplicate pending exists near {next_dt}; skip: {series_key}"))
                continue

            if dry_run:
                created_total += 1
                per_user[s["assign_to_id"]] = per_user.get(s["assign_to_id"], 0) + 1
                logger.info(_safe_console_text(
                    f"[DRY RUN] Would create '{s['task_name']}' for user_id={s['assign_to_id']} "
                    f"at {next_dt.astimezone(IST):%Y-%m-%d %H:%M IST}"
                ))
                continue

            # Create the next occurrence
            try:
                with transaction.atomic():
                    obj = Checklist.objects.create(
                        assign_by=last_completed.assign_by,
                        task_name=last_completed.task_name,
                        message=last_completed.message,
                        assign_to=last_completed.assign_to,
                        planned_date=next_dt,  # PRESERVED time-of-day
                        priority=last_completed.priority,
                        attachment_mandatory=last_completed.attachment_mandatory,
                        mode=last_completed.mode,
                        frequency=last_completed.frequency,
                        recurrence_end_date=end_date,
                        time_per_task_minutes=last_completed.time_per_task_minutes,
                        remind_before_days=last_completed.remind_before_days,
                        assign_pc=last_completed.assign_pc,
                        notify_to=last_completed.notify_to,
                        auditor=getattr(last_completed, "auditor", None),
                        set_reminder=last_completed.set_reminder,
                        reminder_mode=last_completed.reminder_mode,
                        reminder_frequency=last_completed.reminder_frequency,
                        reminder_starting_time=last_completed.reminder_starting_time,
                        checklist_auto_close=last_completed.checklist_auto_close,
                        checklist_auto_close_days=last_completed.checklist_auto_close_days,
                        group_name=getattr(last_completed, "group_name", None),
                        actual_duration_minutes=0,
                        status="Pending",
                    )
                created_total += 1
                per_user[s["assign_to_id"]] = per_user.get(s["assign_to_id"], 0) + 1

                logger.info(_safe_console_text(
                    f"✅ Created next CL-{obj.id} '{obj.task_name}' for user_id={s['assign_to_id']} "
                    f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
                ))

                # Email the assignee (admin not CC'd)
                if send_emails:
                    _send_recur_email(obj)

            except Exception as e:
                logger.exception("Failed to create next occurrence for series %s: %s", series_key, e)

        # Per-user summary to diagnose gaps (e.g., dinesh@ case)
        if per_user:
            for uid, count in per_user.items():
                logger.info(_safe_console_text(f"[RECUR GEN] user_id={uid} → created {count} occurrence(s)"))
        else:
            logger.info(_safe_console_text(
                f"[RECUR GEN] No new items created at {now.astimezone(IST):%Y-%m-%d %H:%M IST} "
                f"(dry_run={dry_run}, user_id={user_id})"
            ))

        # Final stdout line for CLI
        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created_total} task(s)"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created_total} task(s)"))
