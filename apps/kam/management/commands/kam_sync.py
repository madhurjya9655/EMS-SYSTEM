# apps/kam/management/commands/kam_sync.py
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from apps.kam.sheets import run_sync_now

class Command(BaseCommand):
    help = (
        "KAM read-only import from Google Sheets into EMS tables "
        "(Customers, Sales, Leads, Overdues). "
        "Idempotent upserts; no deletes."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-n", "--dry-run", action="store_true",
            help="Parse Sheets and compute upserts but do not write any DB rows."
        )

    def handle(self, *args, **opts):
        # Optional dry-run flag is already honored by sheets.py via KAM_IMPORT_DRY_RUN;
        # mirror it here for convenience.
        if opts.get("dry_run"):
            from os import environ
            environ["KAM_IMPORT_DRY_RUN"] = "1"

        # Keep the whole sync in a short transaction batches inside sheets.py;
        # here we just orchestrate and print a summary line.
        stats = run_sync_now()
        self.stdout.write(self.style.SUCCESS(stats.as_message()))
