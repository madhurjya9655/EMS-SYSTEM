# apps/tasks/management/commands/generate_recurring_tasks.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time

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
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


def _within_10am_ist_window(leeway_minutes: int = 5) -> bool:
    """
    True if now (IST) is within [10:00 - leeway, 10:00 + leeway].
    Default window: 09:55–10:05 IST.
    """
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _next_planned_preserve_time(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    Compute the next occurrence for a recurring checklist while PRESERVING the
    original planned time-of-day. If the next date lands on Sunday/holiday, move
    forward to the next working day but KEEP the same time-of-day.

    Returns an aware datetime in the project's timezone.
    """
    if not prev_dt:
        return None

    m = normalize_mode(mode)
    if m not in RECURRING_MODES:
        return None

    step = max(int(frequency or 1), 1)

    # Work in IST for wall-clock stability
    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, timezone.get_current_timezone())
    prev_ist = prev_dt.astimezone(IST)

    # Original time-of-day to preserve
    t_planned = dt_time(prev_ist.hour, prev_ist.minute, prev_ist.second, prev_ist.microsecond)

    # Add interval
    if m == "Daily":
        nxt_ist = prev_ist + relativedelta(days=step)
    elif m == "Weekly":
        nxt_ist = prev_ist + relativedelta(weeks=step)
    elif m == "Monthly":
        nxt_ist = prev_ist + relativedelta(months=step)
    elif m == "Yearly":
        nxt_ist = prev_ist + relativedelta(years=step)
    else:
        return None

    # Re-apply preserved time-of-day
    nxt_ist = nxt_ist.replace(
        hour=t_planned.hour,
        minute=t_planned.minute,
        second=t_planned.second,
        microsecond=t_planned.microsecond,
    )

    # If Sunday/holiday, push FORWARD to next working day, SAME time
    d = nxt_ist.date()
    if not is_working_day(d):
        d = next_working_day(d)
        nxt_ist = IST.localize(datetime.combine(d, t_planned))

    # Return in project timezone
    return nxt_ist.astimezone(timezone.get_current_timezone())


class Command(BaseCommand):
    help = (
        "Generate next occurrences for recurring CHECKLIST tasks only.\n"
        "• Next occurrences PRESERVE the original planned time-of-day.\n"
        "• If the calculated date is Sunday/holiday, shift to the next working day (same time).\n"
        "• Dashboard handles 10:00 IST visibility; delay is always from the planned time.\n"
        "• Delegations/Help Tickets are NOT generated (no recurrence)."
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
        dry_run = opts.get("dry_run", False)
        send_emails = not opts.get("no_email", False)
        now = timezone.now()

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created.\n"))

        created = 0

        # ---- CHECKLISTS ONLY (delegations/help tickets have no recurrence by policy) ----
        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        for s in seeds:
            last = (
                Checklist.objects
                .filter(
                    assign_to_id=s["assign_to_id"],
                    task_name=s["task_name"],
                    mode=s["mode"],
                    frequency=s["frequency"],
                    group_name=s["group_name"],
                )
                .order_by("-planned_date", "-id")
                .first()
            )
            if not last:
                continue

            # Already has a future pending? skip
            if Checklist.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
                continue

            # Compute next occurrence while preserving time-of-day
            next_dt = _next_planned_preserve_time(last.planned_date, last.mode, last.frequency)

            # Catch up to future if needed (safety ~2 years)
            safety = 0
            while next_dt and next_dt <= now and safety < 730:
                next_dt = _next_planned_preserve_time(next_dt, last.mode, last.frequency)
                safety += 1
            if not next_dt:
                continue

            # Dupe guard (±1 minute)
            dupe = Checklist.objects.filter(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(f"[DRY RUN] Would create checklist: {s['task_name']} at {next_dt}")
                continue

            try:
                with transaction.atomic():
                    obj = Checklist.objects.create(
                        assign_by=last.assign_by,
                        task_name=last.task_name,
                        message=last.message,
                        assign_to=last.assign_to,
                        planned_date=next_dt,  # PRESERVED time-of-day
                        priority=last.priority,
                        attachment_mandatory=last.attachment_mandatory,
                        mode=last.mode,
                        frequency=last.frequency,
                        time_per_task_minutes=last.time_per_task_minutes,
                        remind_before_days=last.remind_before_days,
                        assign_pc=last.assign_pc,
                        notify_to=last.notify_to,
                        auditor=getattr(last, "auditor", None),
                        set_reminder=last.set_reminder,
                        reminder_mode=last.reminder_mode,
                        reminder_frequency=last.reminder_frequency,
                        reminder_starting_time=last.reminder_starting_time,
                        checklist_auto_close=last.checklist_auto_close,
                        checklist_auto_close_days=last.checklist_auto_close_days,
                        group_name=getattr(last, "group_name", None),
                        actual_duration_minutes=0,
                        status="Pending",
                    )
                created += 1
                self.stdout.write(self.style.SUCCESS(f"✅ Created checklist: {obj.task_name} at {next_dt}"))

                # Email policy: only if enabled and (optionally) in 10:00 IST window
                if send_emails and SEND_EMAILS_FOR_AUTO_RECUR and (
                    not SEND_RECUR_EMAILS_ONLY_AT_10AM or _within_10am_ist_window()
                ):
                    try:
                        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
                        send_checklist_assignment_to_user(
                            task=obj,
                            complete_url=complete_url,
                            subject_prefix="Recurring Checklist Generated",
                        )
                        send_checklist_admin_confirmation(
                            task=obj,
                            subject_prefix="Recurring Checklist Generated",
                        )
                    except Exception as e:
                        logger.exception("Email failure for checklist %s: %s", obj.id, e)

            except Exception as e:
                logger.exception("Failed checklist generation: %s", e)

        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} tasks"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} tasks"))
