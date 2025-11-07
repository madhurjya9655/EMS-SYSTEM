# apps/reimbursement/management/commands/send_reimbursement_summary.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from django.core.management.base import BaseCommand, CommandParser
from django.utils import timezone

from apps.reimbursement.tasks import send_monthly_admin_summary


class Command(BaseCommand):
    help = (
        "Send a monthly reimbursement summary email to Admins.\n"
        "By default, summarises the previous calendar month.\n"
        "Optionally pass --month YYYY-MM to summarise a specific month."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--month",
            type=str,
            help="Target month in YYYY-MM format (defaults to previous month).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Do not send email; print summary to stdout instead.",
        )

    def handle(self, *args, **options):
        month_str: Optional[str] = options.get("month")
        dry_run: bool = options.get("dry_run", False)

        target_date = None
        if month_str:
            try:
                dt = datetime.strptime(month_str, "%Y-%m")
                # normalise to first of month in current timezone
                target_date = timezone.localdate(dt)
            except ValueError:
                self.stderr.write(self.style.ERROR("Invalid --month format. Use YYYY-MM, e.g. 2025-01"))
                return

        send_monthly_admin_summary(target_month=target_date, dry_run=dry_run)

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry-run completed; summary printed to console."))
        else:
            self.stdout.write(self.style.SUCCESS("Monthly reimbursement summary email sent (if any data)."))
