from __future__ import annotations

import os
from textwrap import dedent

from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Install bulk upload template CSV files to the static directory."

    def handle(self, *args, **options):
        base_static = os.path.join(settings.BASE_DIR, "static")
        target_dir = os.path.join(base_static, "bulk_upload_templates")
        os.makedirs(target_dir, exist_ok=True)

        # -------------------------- Checklist CSV -------------------------- #
        checklist_csv = dedent(
            """\
            Task Name,Message,Assign To,Planned Date,Priority,Mode,Frequency,Time per Task (minutes),Reminder Before Days,Assign PC,Group Name,Notify To,Auditor,Set Reminder,Reminder Mode,Reminder Frequency,Reminder Starting Time,Checklist Auto Close,Checklist Auto Close Days
            Weekly Report Submission,Submit weekly progress report,john_doe,2025-08-18 10:00,Medium,Weekly,1,30,1,jane_manager,Operations,admin_user,audit_user,Yes,Daily,1,09:00,No,0
            Monthly Budget Review,Review department budget,jane_manager,2025-09-16,High,Monthly,1,60,2,,Finance,admin_user,,No,,,,,
            Daily Standup Meeting,Attend daily team standup,team_lead,2025-08-17,Low,Daily,1,15,0,,,,,No,,,,,
            """
        )

        # ------------------------- Delegation CSV ------------------------- #
        delegation_csv = dedent(
            """\
            Task Name,Assign To,Planned Date,Priority,Mode,Frequency,Time per Task (minutes)
            Prepare Client Presentation,john_doe,2025-08-18 10:00,High,Weekly,1,45
            Update Website Content,web_dev,2025-08-19,Medium,Weekly,2,120
            Process Invoice Payments,finance_team,2025-08-17 16:00,Low,Daily,1,20
            """
        )

        # ----------------------------- README ----------------------------- #
        readme_txt = dedent(
            f"""\
            BULK UPLOAD TEMPLATES

            Location:
              {target_dir}

            Files:
              - checklist_template.csv
              - delegation_template.csv

            General Guidance:
              • Use system usernames for "Assign To", "Notify To", "Auditor", "Assign PC".
              • "Planned Date" accepts either:
                  - full datetime: YYYY-MM-DD HH:MM (24h)
                  - date only: YYYY-MM-DD  (will be scheduled at 10:00 AM IST automatically)
              • Supported Mode values: Daily, Weekly, Monthly, Yearly
              • Frequency: integer >= 1
              • For recurring tasks, next occurrences are created at 10:00 AM IST and skip Sundays/holidays.

            CSV Columns — Checklist:
              Task Name, Message, Assign To, Planned Date, Priority, Mode, Frequency,
              Time per Task (minutes), Reminder Before Days, Assign PC, Group Name, Notify To,
              Auditor, Set Reminder, Reminder Mode, Reminder Frequency, Reminder Starting Time,
              Checklist Auto Close, Checklist Auto Close Days

            CSV Columns — Delegation:
              Task Name, Assign To, Planned Date, Priority, Mode, Frequency, Time per Task (minutes)

            Examples (Valid):
              2025-08-18 15:30
              2025-08-18
              18/08/2025 15:30   (if your importer supports localized formats)
              08/18/2025 15:30   (US format, if supported by your importer)

            IMPORTANT:
              • Ensure the number of rows you upload matches the number of tasks created.
              • Duplicates are prevented by the uploader using your system's de-duplication rules.
              • If a username does not exist or a required field is invalid, that row will be skipped
                or reported by the importer (check server logs / summary email).

            """
        )

        # Write files (overwrite if they exist)
        chk_path = os.path.join(target_dir, "checklist_template.csv")
        dlg_path = os.path.join(target_dir, "delegation_template.csv")
        readme_path = os.path.join(target_dir, "README.txt")

        with open(chk_path, "w", encoding="utf-8", newline="") as f:
            f.write(checklist_csv)
        with open(dlg_path, "w", encoding="utf-8", newline="") as f:
            f.write(delegation_csv)
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_txt)

        self.stdout.write(self.style.SUCCESS("Bulk upload templates installed:"))
        self.stdout.write(f"  - {chk_path}")
        self.stdout.write(f"  - {dlg_path}")
        self.stdout.write(f"  - {readme_path}")
