# FILE: apps/kam/management/commands/sync_kam_sheets.py
# PURPOSE: Management command to trigger KAM Google Sheets sync
# USAGE:
#   python manage.py sync_kam_sheets                   # full sync
#   python manage.py sync_kam_sheets --section sheet1  # single section only
#   python manage.py sync_kam_sheets --section customers
#   python manage.py sync_kam_sheets --section sales_f
#   python manage.py sync_kam_sheets --section overdues

from django.core.management.base import BaseCommand, CommandError

from apps.kam import sheets_adapter

VALID_SECTIONS = ["customers", "sales_f", "sheet1", "frontend", "enquiry_f", "overdues"]


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
            # Override env flags to run only the requested section
            import os
            section_flag_map = {
                "customers":  "KAM_SYNC_CUSTOMERS",
                "sales_f":    "KAM_SYNC_SALES",
                "sheet1":     "KAM_SYNC_SHEET1",
                "frontend":   "KAM_SYNC_FRONTEND",
                "enquiry_f":  "KAM_SYNC_ENQUIRY",
                "overdues":   "KAM_SYNC_OVERDUES",
            }
            # Disable all sections first
            for flag in section_flag_map.values():
                os.environ[flag] = "0"
            # Enable only requested section
            os.environ[section_flag_map[section]] = "1"
            self.stdout.write(self.style.NOTICE(f"Running single section: {section}"))
        else:
            self.stdout.write(self.style.NOTICE("Running full sync (all sections)..."))

        try:
            stats = sheets_adapter.run_sync_now()
        except Exception as exc:
            raise CommandError(f"Sync failed: {exc}") from exc

        self.stdout.write(self.style.SUCCESS(f"Sync complete: {stats.as_message()}"))

        if stats.notes:
            self.stdout.write(self.style.WARNING("Notes:"))
            for note in stats.notes:
                self.stdout.write(f"  - {note}")

        if stats.skipped:
            self.stdout.write(
                self.style.WARNING(
                    f"\n{stats.skipped} rows skipped. "
                    "Check logs for details: grep 'Cannot parse date' in your Render log tail."
                )
            )

        self.stdout.write("")
        self.stdout.write("Summary:")
        self.stdout.write(f"  Customers upserted : {stats.customers_upserted}")
        self.stdout.write(f"  Sales upserted     : {stats.sales_upserted}")
        self.stdout.write(f"  Leads upserted     : {stats.leads_upserted}")
        self.stdout.write(f"  Overdues upserted  : {stats.overdues_upserted}")
        self.stdout.write(f"  Skipped rows       : {stats.skipped}")
        self.stdout.write(f"  Unknown KAM names  : {stats.unknown_kam}")