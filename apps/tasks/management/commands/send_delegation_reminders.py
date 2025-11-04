# apps/leave/management/commands/send_delegation_reminders.py

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """
    Legacy command (was using a DelegationReminder model that no longer exists).

    Kept only so that if someone runs:
        python manage.py send_delegation_reminders

    they will get a clear message instead of a database error.
    """

    help = "Deprecated. Use 'send_tasks_delegation_reminders' instead."

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "This command is deprecated.\n"
                "Please run instead:\n"
                "    python manage.py send_tasks_delegation_reminders"
            )
        )
