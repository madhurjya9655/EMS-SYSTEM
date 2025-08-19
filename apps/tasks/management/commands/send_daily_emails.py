# apps/tasks/management/commands/send_daily_emails.py
from __future__ import annotations

import pytz
import logging
from datetime import datetime, time, timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.utils import send_checklist_assignment_to_user

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class Command(BaseCommand):
    help = "Send reminder emails around 10:00 AM IST for today's *recurring* checklist tasks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--window-minutes",
            type=int,
            default=5,
            help="Send reminders only within ±N minutes around 10:00 IST (default: 5).",
        )

    def handle(self, *args, **opts):
        window = int(opts["window_minutes"])
        now_ist = timezone.now().astimezone(IST)

        # Only run within [10:00 - window, 10:00 + window] IST
        target = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
        delta = abs((now_ist - target).total_seconds()) / 60.0
        if delta > window:
            self.stdout.write(
                self.style.WARNING(
                    f"Current time {now_ist.strftime('%H:%M')} IST outside window ±{window} min of 10:00."
                )
            )
            return

        # Build a 1-minute range around 10:00:00 IST to locate tasks scheduled at 10am
        start_ist = datetime.combine(now_ist.date(), time(10, 0, 0, 0, tzinfo=IST))
        end_ist = start_ist + timedelta(minutes=1)

        # Convert range to project timezone for DB query
        proj_tz = timezone.get_current_timezone()
        start_proj = start_ist.astimezone(proj_tz)
        end_proj = end_ist.astimezone(proj_tz)

        qs = Checklist.objects.filter(
            status="Pending",
            planned_date__gte=start_proj,
            planned_date__lt=end_proj,
            mode__in=["Daily", "Weekly", "Monthly", "Yearly"],  # recurring only
        )

        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
        sent = 0

        for task in qs.iterator():
            if not (getattr(task.assign_to, "email", "") or "").strip():
                continue
            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[task.id])}"
            try:
                send_checklist_assignment_to_user(
                    task=task,
                    complete_url=complete_url,
                    subject_prefix="Daily Reminder - Recurring Checklist",
                )
                sent += 1
                logger.info("Reminder sent for checklist %s to %s", task.id, task.assign_to.email)
            except Exception as e:
                logger.exception("Failed sending reminder for checklist %s: %s", task.id, e)

        self.stdout.write(self.style.SUCCESS(f"Reminder emails sent: {sent}"))
