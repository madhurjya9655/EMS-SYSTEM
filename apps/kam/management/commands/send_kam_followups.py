from django.core.management.base import BaseCommand
from django.utils import timezone
from django.contrib.auth import get_user_model
from apps.kam.models import VisitActual

User = get_user_model()

class Command(BaseCommand):
    help = "Send (or list) follow-up reminders for VisitActual with next_action_date due/overdue and reminder_cc_manager=True."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Only print; do not send.")

    def handle(self, *args, **opts):
        today = timezone.localdate()
        qs = VisitActual.objects.select_related("plan__kam").filter(
            reminder_cc_manager=True, next_action_date__isnull=False, next_action__isnull=False
        ).exclude(next_action="").filter(next_action_date__lte=today).order_by("next_action_date")

        if not qs.exists():
            self.stdout.write("No follow-ups due.")
            return

        for va in qs:
            kam = va.plan.kam
            due = va.next_action_date
            msg = f"[FOLLOW-UP] {kam.username}: {va.next_action} (due {due})"
            if opts["dry_run"]:
                self.stdout.write(msg)
            else:
                # integrate email/SMS/notification provider here
                # For now, print to console.
                self.stdout.write(msg)
