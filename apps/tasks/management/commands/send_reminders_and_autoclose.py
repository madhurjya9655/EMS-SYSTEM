from __future__ import annotations

import logging
from datetime import timedelta

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation
from apps.tasks import utils as _utils

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")


class Command(BaseCommand):
    """
    Daily 10:00 AM IST job:

      • Send reminder emails for ALL pending checklists & delegations
        that have `set_reminder=True`. The reminder goes every day at
        10:00 AM IST from the start date until the task is completed.

      • Auto-close overdue checklists when `checklist_auto_close` is
        enabled and `checklist_auto_close_days` is reached.

    IMPORTANT:
      - Schedule this command ONCE per day around 10:00 AM IST via cron
        or Celery beat, for example:

          0 10 * * *  /path/to/venv/bin/python manage.py send_reminders_and_autoclose
    """

    help = (
        "Send daily 10:00 AM IST reminders for pending checklists & delegations "
        "and auto-close overdue checklists."
    )

    def handle(self, *args, **kwargs):
        now = timezone.now()
        today_ist = now.astimezone(IST).date()

        self.stdout.write(
            self.style.NOTICE(
                f"[send_reminders_and_autoclose] Running for {today_ist} (IST)"
            )
        )

        cl_sent = self._send_checklist_reminders(today_ist)
        dl_sent = self._send_delegation_reminders(today_ist)
        closed = self._auto_close_checklists(now)

        self.stdout.write(self.style.SUCCESS(f"Checklist reminders sent: {cl_sent}"))
        self.stdout.write(self.style.SUCCESS(f"Delegation reminders sent: {dl_sent}"))
        self.stdout.write(self.style.WARNING(f"Checklists auto-closed: {closed}"))

    # ------------------------------------------------------------------ #
    # CHECKLIST REMINDERS (EVERY DAY 10:00 IST UNTIL COMPLETED)
    # ------------------------------------------------------------------ #
    def _send_checklist_reminders(self, today_ist):
        """
        For each Checklist:
          • set_reminder = True
          • status != 'Completed'
          • today's date >= (planned_date.date - remind_before_days)

        One reminder per day (because this command runs once per day).
        Time-of-day is controlled by cron/Celery (10:00 AM IST).
        """
        qs = (
            Checklist.objects.select_related("assign_to", "assign_by")
            .filter(set_reminder=True)
            .exclude(status="Completed")
        )

        count = 0

        for task in qs:
            try:
                # Never email self-assigned tasks
                if task.assign_by_id and task.assign_by_id == task.assign_to_id:
                    continue

                assignee = getattr(task, "assign_to", None)
                to_email = (getattr(assignee, "email", "") or "").strip()
                if not to_email:
                    continue

                # Determine when reminders should start
                if task.planned_date:
                    planned_ist = timezone.localtime(task.planned_date, IST)
                    remind_days = task.remind_before_days or 0
                    start_date = planned_ist.date() - timedelta(days=remind_days)
                else:
                    # No planned date -> start today
                    start_date = today_ist

                # Don't send before the start date
                if today_ist < start_date:
                    continue

                # Optional end date for recurring series (if present)
                recurrence_end_date = getattr(task, "recurrence_end_date", None)
                if recurrence_end_date and today_ist > recurrence_end_date:
                    continue

                complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[task.id])}"
                subject_prefix = f"Reminder – Checklist '{task.task_name}'"

                _utils.send_checklist_assignment_to_user(
                    task=task,
                    complete_url=complete_url,
                    subject_prefix=subject_prefix,
                )

                count += 1
            except Exception as e:
                logger.exception(
                    _utils._safe_console_text(
                        f"[send_reminders_and_autoclose] Checklist "
                        f"{getattr(task, 'id', '?')} reminder failed: {e}"
                    )
                )

        return count

    # ------------------------------------------------------------------ #
    # DELEGATION REMINDERS (EVERY DAY 10:00 IST UNTIL COMPLETED)
    # ------------------------------------------------------------------ #
    def _send_delegation_reminders(self, today_ist):
        """
        For each Delegation:
          • set_reminder = True
          • status != 'Completed'
          • today's date >= reminder start date

        Reminder start date:
          • if reminder_time set  -> date(reminder_time in IST)
          • elif planned_date set -> date(planned_date in IST)
          • else                  -> today
        """
        qs = (
            Delegation.objects.select_related("assign_to", "assign_by")
            .filter(set_reminder=True)
            .exclude(status="Completed")
        )

        count = 0

        for dl in qs:
            try:
                # Never email self-assigned delegations
                if dl.assign_by_id and dl.assign_by_id == dl.assign_to_id:
                    continue

                assignee = getattr(dl, "assign_to", None)
                to_email = (getattr(assignee, "email", "") or "").strip()
                if not to_email:
                    continue

                # Determine when reminders should start
                if dl.reminder_time:
                    start_ist = timezone.localtime(dl.reminder_time, IST)
                    start_date = start_ist.date()
                elif dl.planned_date:
                    planned_ist = timezone.localtime(dl.planned_date, IST)
                    start_date = planned_ist.date()
                else:
                    start_date = today_ist

                if today_ist < start_date:
                    continue

                complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[dl.id])}"
                subject_prefix = f"Reminder – Delegation '{dl.task_name}'"

                _utils.send_delegation_assignment_to_user(
                    delegation=dl,
                    complete_url=complete_url,
                    subject_prefix=subject_prefix,
                )

                count += 1
            except Exception as e:
                logger.exception(
                    _utils._safe_console_text(
                        f"[send_reminders_and_autoclose] Delegation "
                        f"{getattr(dl, 'id', '?')} reminder failed: {e}"
                    )
                )

        return count

    # ------------------------------------------------------------------ #
    # AUTO-CLOSE CHECKLISTS
    # ------------------------------------------------------------------ #
    def _auto_close_checklists(self, now):
        """
        Keep the old auto-close behaviour:

          • checklist_auto_close = True
          • checklist_auto_close_days >= 1
          • status = 'Pending'
          • planned_date + checklist_auto_close_days <= now

        When closing, mark status='Completed' and set completed_at.
        """
        qs = Checklist.objects.filter(
            status="Pending",
            checklist_auto_close=True,
            checklist_auto_close_days__gte=1,
        )

        closed = 0

        for task in qs:
            try:
                if not task.planned_date:
                    continue

                deadline = task.planned_date + timedelta(days=task.checklist_auto_close_days)
                if now >= deadline:
                    task.status = "Completed"
                    task.completed_at = now
                    task.save(update_fields=["status", "completed_at"])

                    closed += 1
                    logger.warning(
                        _utils._safe_console_text(
                            f"[send_reminders_and_autoclose] Auto-closed "
                            f"Checklist {task.id} '{task.task_name}'"
                        )
                    )
            except Exception as e:
                logger.exception(
                    _utils._safe_console_text(
                        f"[send_reminders_and_autoclose] Auto-close failed for "
                        f"Checklist {getattr(task, 'id', '?')}: {e}"
                    )
                )

        return closed
