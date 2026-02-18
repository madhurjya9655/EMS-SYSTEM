# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\send_daily_emails.py
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    """
    BACKWARDS-COMPATIBILITY WRAPPER.

    Older deployments may still be calling `send_daily_emails` from cron.
    To keep those working, this command simply delegates to the new,
    consolidated command:

        send_reminders_and_autoclose

    You can safely switch your cron/Celery schedule to call
    `send_reminders_and_autoclose` directly and eventually delete this file.
    """

    help = "Deprecated – forwards to `send_reminders_and_autoclose`."

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "send_daily_emails is deprecated – calling "
                "`send_reminders_and_autoclose` instead."
            )
        )
        call_command("send_reminders_and_autoclose")
