from __future__ import annotations

import logging
from datetime import datetime, time as dt_time

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Delegation
from apps.tasks.utils import (
    send_delegation_assignment_to_user,
    _safe_console_text,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


class Command(BaseCommand):
    help = (
        "Send daily 10:00 AM IST reminder emails for PENDING delegations "
        "where set_reminder=True, until they are completed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--window-minutes",
            type=int,
            default=5,
            help="Only send if current time is within ±N minutes of 10:00 IST (default: 5).",
        )
        parser.add_argument(
            "--user-id",
            type=int,
            help="Limit reminders to a specific assignee (user id).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be emailed without actually sending.",
        )

    def handle(self, *args, **opts):
        if not SEND_EMAILS_FOR_AUTO_RECUR:
            self.stdout.write(
                self.style.WARNING(
                    "SEND_EMAILS_FOR_AUTO_RECUR is disabled; delegation reminders will not be sent."
                )
            )
            return

        window = int(opts["window_minutes"])
        user_id = opts.get("user_id")
        dry_run = bool(opts.get("dry_run", False))

        now_ist = timezone.now().astimezone(IST)
        anchor_10 = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)

        if SEND_RECUR_EMAILS_ONLY_AT_10AM:
            minutes_off = abs((now_ist - anchor_10).total_seconds()) / 60.0
            if minutes_off > window:
                self.stdout.write(
                    self.style.WARNING(
                        f"Current time {now_ist.strftime('%H:%M')} IST is outside "
                        f"±{window} min of 10:00. Exiting."
                    )
                )
                return

        today_ist = now_ist.date()
        end_today_ist = IST.localize(datetime.combine(today_ist, dt_time.max))
        proj_tz = timezone.get_current_timezone()
        end_today_proj = end_today_ist.astimezone(proj_tz)

        qs = (
            Delegation.objects.filter(
                status="Pending",
                set_reminder=True,
                planned_date__lte=end_today_proj,
            )
            .select_related("assign_to", "assign_by")
            .order_by("planned_date")
        )
        if user_id:
            qs = qs.filter(assign_to_id=user_id)

        total = qs.count()
        sent = 0

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"[DRY RUN] Would evaluate {total} delegation(s) for reminders."
                )
            )

        for delegation in qs.iterator():
            try:
                if (
                    delegation.assign_by_id
                    and delegation.assign_by_id == delegation.assign_to_id
                ):
                    logger.info(
                        _safe_console_text(
                            f"Skip delegation reminder for DL-{delegation.id}: assigner == assignee"
                        )
                    )
                    continue
            except Exception:
                pass

            to_email = (
                getattr(getattr(delegation, "assign_to", None), "email", "") or ""
            ).strip()
            if not to_email:
                logger.info(
                    _safe_console_text(
                        f"Skip delegation reminder for DL-{delegation.id}: no assignee email"
                    )
                )
                continue

            planned_ist = (
                delegation.planned_date.astimezone(IST)
                if delegation.planned_date
                else None
            )
            pretty_date = planned_ist.strftime("%d %b %Y") if planned_ist else "N/A"
            pretty_time = planned_ist.strftime("%H:%M") if planned_ist else "N/A"

            if planned_ist and planned_ist.date() < today_ist:
                subject_prefix = (
                    f"⏰ OVERDUE Delegation Reminder – {delegation.task_name} "
                    f"(planned {pretty_date} {pretty_time} IST)"
                )
            else:
                subject_prefix = (
                    f"⏰ Delegation Reminder – {delegation.task_name} "
                    f"(due today {pretty_date} at 19:00 IST)"
                )

            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[delegation.id])}"

            if dry_run:
                sent += 1
                logger.info(
                    _safe_console_text(
                        f"[DRY RUN] Would send delegation reminder for DL-{delegation.id} "
                        f"to {to_email} (planned {pretty_date} {pretty_time} IST)"
                    )
                )
                continue

            try:
                send_delegation_assignment_to_user(
                    delegation=delegation,
                    complete_url=complete_url,
                    subject_prefix=subject_prefix,
                )
                sent += 1
                logger.info(
                    _safe_console_text(
                        f"Delegation reminder sent for DL-{delegation.id} to {to_email}"
                    )
                )
            except Exception as e:
                logger.exception(
                    "Failed sending delegation reminder %s: %s", delegation.id, e
                )

        summary = (
            f"Delegation reminder emails {'(dry run) ' if dry_run else ''}"
            f"sent: {sent} / {total}"
        )
        self.stdout.write(self.style.SUCCESS(summary))
