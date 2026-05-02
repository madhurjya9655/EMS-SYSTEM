#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\send_mis_report.py
from __future__ import annotations

import json
from datetime import date
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_date

from apps.tasks.services.mis_report import send_mis_report_email


class Command(BaseCommand):
    help = "Send employee-wise MIS task performance report."

    def add_arguments(self, parser):
        parser.add_argument(
            "--week",
            choices=["current", "last"],
            default="current",
            help="Report week. current = this Monday to Saturday. last = previous Monday to Saturday.",
        )

        parser.add_argument(
            "--date",
            default=None,
            help="Optional anchor date in YYYY-MM-DD format.",
        )

        parser.add_argument(
            "--formula",
            choices=["variance", "completion_pct"],
            default=None,
            help=(
                "Score formula. "
                "variance = ((Actual - Planned) / Planned) * 100. "
                "completion_pct = (Actual / Planned) * 100."
            ),
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Build report but do not send email.",
        )

        parser.add_argument(
            "--print-json",
            action="store_true",
            help="Print full report JSON. Useful for Render validation.",
        )

        parser.add_argument(
            "--to",
            action="append",
            default=None,
            help="Override primary recipient. Can be passed multiple times.",
        )

        parser.add_argument(
            "--cc",
            action="append",
            default=None,
            help="Override CC recipient. Can be passed multiple times.",
        )

    def handle(self, *args, **options):
        anchor_date: Optional[date] = None

        raw_date = options.get("date")
        if raw_date:
            anchor_date = parse_date(raw_date)
            if not anchor_date:
                raise CommandError("--date must be in YYYY-MM-DD format.")

        result = send_mis_report_email(
            anchor_date=anchor_date,
            week_selector=options["week"],
            formula=options.get("formula"),
            to=options.get("to"),
            cc=options.get("cc"),
            dry_run=options["dry_run"],
        )

        if options["dry_run"]:
            self.stdout.write(self.style.WARNING("DRY RUN - email not sent"))
        else:
            self.stdout.write(self.style.SUCCESS("MIS report email sent successfully"))

        self.stdout.write("")
        self.stdout.write(f"Subject: {result['subject']}")
        self.stdout.write(f"To: {', '.join(result['to'])}")
        self.stdout.write(f"CC: {', '.join(result['cc'])}")
        self.stdout.write(f"Employees: {result['employee_count']}")
        self.stdout.write(f"Totals: {result['totals']}")
        self.stdout.write(f"Model Breakdown: {result['model_breakdown']}")

        if options["print_json"]:
            if options["dry_run"]:
                payload = result["report"]
            else:
                payload = result

            self.stdout.write("")
            self.stdout.write(json.dumps(payload, default=str, indent=2))