# FILE: apps/kam/management/commands/sync_kam_sheets.py
# PURPOSE: Management command to trigger KAM Google Sheets sync
#
# USAGE:
#   python manage.py sync_kam_sheets
#   python manage.py sync_kam_sheets --section customers
#   python manage.py sync_kam_sheets --section sales_f
#   python manage.py sync_kam_sheets --section sheet1
#   python manage.py sync_kam_sheets --section frontend
#   python manage.py sync_kam_sheets --section enquiry_f
#   python manage.py sync_kam_sheets --section overdues
#   python manage.py sync_kam_sheets --section collection_plan_sync
#   python manage.py sync_kam_sheets --section collection

from __future__ import annotations

import os

from django.core.management.base import BaseCommand, CommandError

from apps.kam import sheets_adapter


VALID_SECTIONS = [
    "customers",
    "sales_f",
    "sheet1",
    "frontend",
    "enquiry_f",
    "overdues",
    "collection_plan_sync",
    "collection",
]


SECTION_FLAG_MAP = {
    "customers": "KAM_SYNC_CUSTOMERS",
    "sales_f": "KAM_SYNC_SALES",
    "sheet1": "KAM_SYNC_SHEET1",
    "frontend": "KAM_SYNC_FRONTEND",
    "enquiry_f": "KAM_SYNC_ENQUIRY",
    "overdues": "KAM_SYNC_OVERDUES",
    # CollectionPlan snapshot reads the Overdues tab, so this intentionally
    # uses the same flag as overdues.
    "collection_plan_sync": "KAM_SYNC_OVERDUES",
    "collection": "KAM_SYNC_COLLECTION",
}


class Command(BaseCommand):
    help = "Sync KAM data from Google Sheets into PostgreSQL"

    def add_arguments(self, parser):
        parser.add_argument(
            "--section",
            choices=VALID_SECTIONS,
            default=None,
            help=(
                "Sync only this section. "
                f"Choices: {', '.join(VALID_SECTIONS)}. "
                "Default: sync all sections."
            ),
        )

    def handle(self, *args, **options):
        section = options.get("section")

        if section:
            for flag in set(SECTION_FLAG_MAP.values()):
                os.environ[flag] = "0"

            os.environ[SECTION_FLAG_MAP[section]] = "1"

            self.stdout.write(self.style.NOTICE(f"Running single section: {section}"))

            if section in {"overdues", "collection_plan_sync"}:
                self.stdout.write(
                    self.style.NOTICE(
                        "Note: overdues and collection_plan_sync both use the Overdues tab."
                    )
                )
        else:
            self.stdout.write(self.style.NOTICE("Running full sync."))

        try:
            stats = sheets_adapter.run_sync_now()
        except Exception as exc:
            raise CommandError(f"Sync failed: {exc}") from exc

        self.stdout.write(self.style.SUCCESS(f"Sync complete: {stats.as_message()}"))

        if stats.notes:
            self.stdout.write(self.style.WARNING("Notes:"))
            for note in stats.notes:
                self.stdout.write(f"  - {note}")

        self.stdout.write("")
        self.stdout.write("Summary:")
        self.stdout.write(f"  Customers upserted   : {stats.customers_upserted}")
        self.stdout.write(f"  Sales upserted       : {stats.sales_upserted}")
        self.stdout.write(f"  Leads upserted       : {stats.leads_upserted}")
        self.stdout.write(f"  Overdues upserted    : {stats.overdues_upserted}")
        self.stdout.write(f"  Collections upserted : {stats.collections_upserted}")
        self.stdout.write(f"  Skipped rows         : {stats.skipped}")
        self.stdout.write(f"  Unknown KAM names    : {stats.unknown_kam}")

        if stats.skipped or stats.unknown_kam:
            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING(
                    "Some rows were skipped or had unknown KAM mapping. Check logs."
                )
            )