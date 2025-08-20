# apps/tasks/management/commands/generate_recurring_tasks.py
from __future__ import annotations

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
from django.conf import settings
from django.urls import reverse

from apps.tasks.models import Checklist, Delegation
from apps.tasks.recurrence import get_next_planned_date, schedule_recurring_at_10am, RECURRING_MODES
from apps.tasks.utils import (
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
    send_delegation_assignment_to_user,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Generate next occurrences for recurring tasks (Checklist / Delegation) and optionally email assignees."

    def add_arguments(self, parser):
        parser.add_argument(
            "--task-type",
            choices=["checklist", "delegation", "all"],
            default="all",
            help="Limit to a specific type.",
        )
        parser.add_argument("--user-id", type=int, help="Limit to specific assignee (user id).")
        parser.add_argument("--dry-run", action="store_true", help="Show without creating.")
        parser.add_argument("--no-email", action="store_true", help="Skip sending emails for created items.")

    def handle(self, *args, **opts):
        task_type = opts["task_type"]
        user_id = opts.get("user_id")
        dry_run = opts.get("dry_run", False)
        send_emails = not opts.get("no_email", False)
        now = timezone.now()

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created.\n"))

        total_created = 0

        if task_type in ("checklist", "all"):
            total_created += self._process_checklists(now, user_id, dry_run, send_emails)
        if task_type in ("delegation", "all"):
            total_created += self._process_delegations(now, user_id, dry_run, send_emails)

        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {total_created} total tasks"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {total_created} total tasks"))

    # ----------------------------- CHECKLISTS ----------------------------- #
    def _process_checklists(self, now, user_id, dry_run, send_emails) -> int:
        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        for s in seeds:
            last = Checklist.objects.filter(**s).order_by("-planned_date", "-id").first()
            if not last:
                continue

            # already has a future pending?
            if Checklist.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
                continue

            # compute next, normalize to 10:00 IST & skip Sunday/holidays
            next_dt = get_next_planned_date(last.planned_date, last.mode, last.frequency)

            safety = 0
            while next_dt and next_dt <= now and safety < 730:
                next_dt = get_next_planned_date(next_dt, last.mode, last.frequency)
                safety += 1
            if not next_dt:
                continue

            next_dt = schedule_recurring_at_10am(next_dt)

            # dupe guard
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
                        planned_date=next_dt,
                        priority=last.priority,
                        attachment_mandatory=last.attachment_mandatory,
                        mode=last.mode,
                        frequency=last.frequency,
                        time_per_task_minutes=last.time_per_task_minutes,
                        remind_before_days=last.remind_before_days,
                        assign_pc=last.assign_pc,
                        notify_to=last.notify_to,
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

                if send_emails:
                    try:
                        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
                        send_checklist_assignment_to_user(
                            task=obj, complete_url=complete_url, subject_prefix="Recurring Checklist Generated"
                        )
                        send_checklist_admin_confirmation(
                            task=obj, subject_prefix="Recurring Checklist Generated"
                        )
                    except Exception as e:
                        logger.exception("Email failure for checklist %s: %s", obj.id, e)

            except Exception as e:
                logger.exception("Failed checklist generation: %s", e)

        return created

    # ---------------------------- DELEGATIONS ---------------------------- #
    def _process_delegations(self, now, user_id, dry_run, send_emails) -> int:
        site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Delegation.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency")
            .distinct()
        )

        created = 0
        for s in seeds:
            last = Delegation.objects.filter(**s).order_by("-planned_date", "-id").first()
            if not last:
                continue

            if Delegation.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
                continue

            next_dt = get_next_planned_date(last.planned_date, last.mode, last.frequency)
            safety = 0
            while next_dt and next_dt <= now and safety < 730:
                next_dt = get_next_planned_date(next_dt, last.mode, last.frequency)
                safety += 1
            if not next_dt:
                continue

            next_dt = schedule_recurring_at_10am(next_dt)

            dupe = Delegation.objects.filter(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(f"[DRY RUN] Would create delegation: {s['task_name']} at {next_dt}")
                continue

            try:
                with transaction.atomic():
                    obj = Delegation.objects.create(
                        assign_by=last.assign_by,
                        task_name=last.task_name,
                        assign_to=last.assign_to,
                        planned_date=next_dt,
                        priority=last.priority,
                        attachment_mandatory=last.attachment_mandatory,
                        mode=last.mode,
                        frequency=last.frequency,
                        time_per_task_minutes=last.time_per_task_minutes,
                        actual_duration_minutes=0,
                        status="Pending",
                    )
                created += 1
                self.stdout.write(self.style.SUCCESS(f"✅ Created delegation: {obj.task_name} at {next_dt}"))

                if send_emails and getattr(obj.assign_to, "email", None):
                    try:
                        # If you have a dedicated completion URL for delegations, update the route name
                        complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
                        send_delegation_assignment_to_user(
                            delegation=obj, complete_url=complete_url, subject_prefix="Recurring Delegation Generated"
                        )
                    except Exception as e:
                        logger.exception("Email failure for delegation %s: %s", obj.id, e)

            except Exception as e:
                logger.exception("Failed delegation generation: %s", e)

        return created
