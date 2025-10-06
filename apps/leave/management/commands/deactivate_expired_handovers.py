# apps/leave/management/commands/deactivate_expired_handovers.py
from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.leave.services.task_handover import deactivate_expired_handovers


class Command(BaseCommand):
    help = "Deactivate expired leave handovers and revert delegated tasks back to original assignees."

    def handle(self, *args, **options):
        reverted = deactivate_expired_handovers()
        self.stdout.write(self.style.SUCCESS(f"Expired handovers processed. Tasks reverted: {reverted}"))
