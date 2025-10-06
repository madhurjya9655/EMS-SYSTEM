# apps/tasks/management/commands/send_daily_emails.py
from __future__ import annotations

import logging
from datetime import datetime, time as dt_time, timedelta

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
# Use the canonical recurring modes list (keeps in sync with recurrence rules)
try:
    from apps.tasks.recurrence import RECURRING_MODES
except Exception:
    RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

from apps.tasks.utils import send_checklist_assignment_to_user, _safe_console_text

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email policy toggles (match the rest of the codebase)
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


class Command(BaseCommand):
    help = (
        "Send assignee-only reminder emails around 10:00 AM IST for today's "
        "pending *recurring* checklist tasks (planned for today, typically 19:00 IST)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--window-minutes",
            type=int,
            default=5,
            help="Run only within ±N minutes around 10:00 IST (default: 5).",
        )
        parser.add_argument(
            "--user-id",
            type=int,
            help="Limit reminders to a specific assignee (user id).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be emailed without sending.",
        )

    def handle(self, *args, **opts):
        if not SEND_EMAILS_FOR_AUTO_RECUR:
            self.stdout.write(self.style.WARNING("SEND_EMAILS_FOR_AUTO_RECUR is disabled; nothing to do."))
            return

        window = int(opts["window_minutes"])
        user_id = opts.get("user_id")
        dry_run = bool(opts.get("dry_run", False))

        now_ist = timezone.now().astimezone(IST)
        anchor_10 = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)

        # Respect the ~10:00 IST window unless the global toggle allows anytime
        if SEND_RECUR_EMAILS_ONLY_AT_10AM:
            minutes_off = abs((now_ist - anchor_10).total_seconds()) / 60.0
            if minutes_off > window:
                self.stdout.write(
                    self.style.WARNING(
                        f"Current time {now_ist.strftime('%H:%M')} IST is outside ±{window} min of 10:00. Exiting."
                    )
                )
                return

        # Build IST 'today' bounds (00:00..23:59) and convert to project TZ for DB filtering
        start_today_ist = IST.localize(datetime.combine(now_ist.date(), dt_time.min))
        end_today_ist = IST.localize(datetime.combine(now_ist.date(), dt_time.max))
        proj_tz = timezone.get_current_timezone()
        start_proj = start_today_ist.astimezone(proj_tz)
        end_proj = end_today_ist.astimezone(proj_tz)

        # Select PENDING recurring checklists planned *today* (any time, usually 19:00 IST)
        qs = (
            Checklist.objects.filter(
                status="Pending",
                planned_date__gte=start_proj,
                planned_date__lte=end_proj,
                mode__in=RECURRING_MODES,
            )
            .select_related("assign_to", "assign_by")
            .order_by("planned_date")
        )
        if user_id:
            qs = qs.filter(assign_to_id=user_id)

        sent = 0
        total = qs.count()

        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would evaluate {total} checklist(s) for reminders."))

        for task in qs.iterator():
            # Skip self-assign emails (assigner == assignee)
            try:
                if task.assign_by_id and task.assign_by_id == task.assign_to_id:
                    logger.info(_safe_console_text(f"Skip email for CL-{task.id}: assigner == assignee"))
                    continue
            except Exception:
                pass

            # Assignee email required
            to_email = (getattr(getattr(task, "assign_to", None), "email", "") or "").strip()
            if not to_email:
                logger.info(_safe_console_text(f"Skip email for CL-{task.id}: no assignee email"))
                continue

            # Build subject with a friendly planned date/time (IST)
            planned_ist = task.planned_date.astimezone(IST) if task.planned_date else None
            pretty_date = planned_ist.strftime("%d %b %Y") if planned_ist else "N/A"
            pretty_time = planned_ist.strftime("%H:%M") if planned_ist else "N/A"
            subject = f"✅ Task Reminder: {task.task_name} scheduled for {pretty_date}, {pretty_time}"

            complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[task.id])}"

            if dry_run:
                sent += 1
                logger.info(
                    _safe_console_text(
                        f"[DRY RUN] Would send reminder for CL-{task.id} to {to_email} "
                        f"(planned {pretty_date} {pretty_time} IST)"
                    )
                )
                continue

            try:
                send_checklist_assignment_to_user(
                    task=task,
                    complete_url=complete_url,
                    subject_prefix=subject,  # pass exact subject
                )
                sent += 1
                logger.info(_safe_console_text(f"Reminder sent for CL-{task.id} to {to_email}"))
            except Exception as e:
                logger.exception("Failed sending reminder for checklist %s: %s", task.id, e)

        summary = f"Reminder emails {'(dry run) ' if dry_run else ''}sent: {sent} / {total}"
        self.stdout.write(self.style.SUCCESS(summary))
