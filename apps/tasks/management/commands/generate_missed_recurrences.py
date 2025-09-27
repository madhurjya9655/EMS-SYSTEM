# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\generate_missed_recurrences.py
# apps/tasks/management/commands/generate_missed_recurrences.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.recurrence import (
    get_next_planned_date,   # final rule: 19:00 IST on working days
    RECURRING_MODES,
)
from apps.tasks.utils import (
    send_checklist_assignment_to_user,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email policy knobs
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)
EMAIL_WINDOW_MINUTES = 5


def _safe_console_text(s: object) -> str:
    try:
        return ("" if s is None else str(s)).encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        try:
            return repr(s)
        except Exception:
            return ""


def _within_10am_ist_window(leeway_minutes: int = EMAIL_WINDOW_MINUTES) -> bool:
    """
    True if now (IST) is within [10:00 - leeway, 10:00 + leeway].
    Default window: 09:55–10:05 IST.
    """
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _assignee_email_or_none(obj: Checklist) -> str | None:
    try:
        email = (obj.assign_to.email or "").strip()
        return email or None
    except Exception:
        return None


class Command(BaseCommand):
    help = (
        "Ensure exactly one FUTURE 'Pending' checklist per recurring series exists.\n"
        "Next recurrences are scheduled at 19:00 IST on working days (Sun/holidays skipped), "
        "per the final product rule. Dashboard handles 10:00 IST visibility gating."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show actions without writing to DB")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id)")
        parser.add_argument("--no-email", action="store_true", help="Skip email notifications")

    def handle(self, *args, **opts):
        dry_run = bool(opts.get("dry_run", False))
        user_id = opts.get("user_id")
        send_emails = not bool(opts.get("no_email", False))

        now = timezone.now()

        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        # One row per (assignee, task_name, mode, frequency, group_name)
        groups = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        processed = 0

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created."))

        for g in groups:
            processed += 1
            # Find the latest occurrence as the stepping base
            instance = (
                Checklist.objects.filter(
                    assign_to_id=g["assign_to_id"],
                    task_name=g["task_name"],
                    mode=g["mode"],
                    frequency=g["frequency"],
                    group_name=g["group_name"],
                )
                .order_by("-planned_date", "-id")
                .first()
            )
            if not instance or not instance.planned_date:
                continue

            # If there is already a future pending item in this series, skip.
            if Checklist.objects.filter(
                assign_to_id=instance.assign_to_id,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gt=now,
                status="Pending",
            ).exists():
                continue

            # Compute next planned occurrence (ALWAYS 19:00 IST on a working day)
            next_planned = get_next_planned_date(instance.planned_date, instance.mode, instance.frequency)
            if not next_planned:
                continue

            # Catch up until next_planned is in the future (robust)
            safety = 0
            while next_planned and next_planned <= now and safety < 730:  # ≈ 2 years safety
                next_planned = get_next_planned_date(next_planned, instance.mode, instance.frequency)
                safety += 1
            if not next_planned:
                continue

            # Dupe guard (±1 minute window)
            dupe = Checklist.objects.filter(
                assign_to_id=instance.assign_to_id,
                task_name=instance.task_name,
                mode=instance.mode,
                frequency=instance.frequency,
                group_name=instance.group_name,
                planned_date__gte=next_planned - timedelta(minutes=1),
                planned_date__lt=next_planned + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(
                    f"[DRY RUN] Would create: {instance.task_name} → {next_planned.astimezone(IST):%Y-%m-%d %H:%M IST}"
                )
                continue

            try:
                with transaction.atomic():
                    kwargs = dict(
                        assign_by=instance.assign_by,
                        task_name=instance.task_name,
                        assign_to=instance.assign_to,
                        planned_date=next_planned,  # 19:00 IST (final rule)
                        priority=instance.priority,
                        attachment_mandatory=instance.attachment_mandatory,
                        mode=instance.mode,
                        frequency=instance.frequency,
                        status="Pending",
                        actual_duration_minutes=0,
                    )
                    # Optional fields mirrored when present
                    for opt in (
                        "message",
                        "time_per_task_minutes",
                        "remind_before_days",
                        "assign_pc",
                        "notify_to",
                        "auditor",
                        "set_reminder",
                        "reminder_mode",
                        "reminder_frequency",
                        "reminder_starting_time",
                        "checklist_auto_close",
                        "checklist_auto_close_days",
                        "group_name",
                    ):
                        if hasattr(instance, opt):
                            kwargs[opt] = getattr(instance, opt)
                    new_obj = Checklist.objects.create(**kwargs)

                created += 1
                self.stdout.write(self.style.SUCCESS(
                    f"✅ Created: CL-{new_obj.id} '{new_obj.task_name}' "
                    f"@ {new_obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
                ))

                # Email policy (assignee-only), matching other auto-recur flows
                if send_emails and SEND_EMAILS_FOR_AUTO_RECUR:
                    if not (new_obj.assign_by_id and new_obj.assign_by_id == new_obj.assign_to_id):
                        assignee_email = _assignee_email_or_none(new_obj)
                        if assignee_email:
                            if (not SEND_RECUR_EMAILS_ONLY_AT_10AM) or _within_10am_ist_window():
                                try:
                                    complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
                                    send_checklist_assignment_to_user(
                                        task=new_obj,
                                        complete_url=complete_url,
                                        subject_prefix="Recurring Checklist Generated",
                                    )
                                    logger.info(_safe_console_text(
                                        f"Sent recur email for CL-{new_obj.id} to user_id={new_obj.assign_to_id}"
                                    ))
                                except Exception as e:
                                    logger.exception("Email failure for recurring checklist %s: %s", new_obj.id, e)
                        else:
                            logger.info(_safe_console_text(
                                f"Skip email for CL-{new_obj.id}: assignee has no email."
                            ))
                    else:
                        logger.info(_safe_console_text(
                            f"Skip email for CL-{new_obj.id}: assigner == assignee (self-assign)."
                        ))

            except Exception as e:
                logger.exception("Failed to create recurrence for %s: %s", instance.task_name, e)
                self.stdout.write(self.style.ERROR(f"❌ Failed: {instance.task_name} - {e}"))

        # Summary
        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} task(s) from {processed} series"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} task(s) from {processed} series"))
        if created == 0:
            self.stdout.write("No missed recurrences needed to be created.")
