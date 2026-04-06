# FILE: apps/kam/management/commands/sync_kam_sheets.py
# USAGE: python manage.py sync_kam_sheets
#        python manage.py sync_kam_sheets --section sales_f

from django.core.management.base import BaseCommand
from apps.kam.sheets import run_sync_now


class Command(BaseCommand):
    help = "Manually trigger Google Sheet → PostgreSQL sync for KAM module"

    def add_arguments(self, parser):
        parser.add_argument(
            "--section",
            type=str,
            default=None,
            help="Sync only one section: customers|sales_f|sheet1|frontend|enquiry_f|overdues",
        )

    def handle(self, *args, **options):
        section = options.get("section")

        if section:
            self.stdout.write(f"Syncing section: {section}")
            from apps.kam.tasks import sync_single_section
            result = sync_single_section(section)
            self.stdout.write(self.style.SUCCESS(str(result)))
        else:
            self.stdout.write("Starting full KAM sync...")
            try:
                result = run_sync_now()
                self.stdout.write(self.style.SUCCESS(f"Sync complete: {result['summary']}"))
                self.stdout.write(f"  Customers : {result['customers_upserted']}")
                self.stdout.write(f"  Sales     : {result['sales_upserted']}")
                self.stdout.write(f"  Leads     : {result['leads_upserted']}")
                self.stdout.write(f"  Overdues  : {result['overdues_upserted']}")
                self.stdout.write(f"  Skipped   : {result['skipped']}")
                if result.get("notes"):
                    for note in result["notes"]:
                        self.stdout.write(self.style.WARNING(f"  NOTE: {note}"))
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Sync failed: {exc}"))
                raise