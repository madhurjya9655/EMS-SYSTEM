from django.core.management.base import BaseCommand
from apps.sales.signals import sync_targets_plan_from_sheet


class Command(BaseCommand):
    help = "Pull manager-controlled Targets_Plan from Google Sheet into EMS (read-only mirror)."

    def handle(self, *args, **options):
        count = sync_targets_plan_from_sheet()
        self.stdout.write(self.style.SUCCESS(f"Synced {count} Targets_Plan rows."))
