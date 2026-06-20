#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reimbursement\management\commands\apply_receipt_map.py
import csv

from django.core.management.base import BaseCommand
from django.db import transaction

from apps.reimbursement.models import ReimbursementLine


class Command(BaseCommand):
    help = "Apply manually verified receipt_file mappings only."

    def add_arguments(self, parser):
        parser.add_argument("--csv", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--audit", default="receipt_apply_audit.csv")

    def handle(self, *args, **opts):
        if not opts["dry_run"] and not opts["apply"]:
            self.stderr.write("Use --dry-run or --apply")
            return

        mappings = []
        with open(opts["csv"], newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                line_id = (row.get("line_id") or "").strip()
                receipt_file = (row.get("receipt_file") or "").strip()
                if line_id and receipt_file:
                    mappings.append((int(line_id), receipt_file))

        self.stdout.write(f"Mappings found: {len(mappings)}")
        audit_rows = []

        with transaction.atomic():
            for line_id, receipt_file in mappings:
                line = (
                    ReimbursementLine.objects
                    .select_related("expense_item")
                    .select_for_update()
                    .get(pk=line_id)
                )

                if line.receipt_file:
                    self.stdout.write(
                        f"SKIP line={line_id}: already has {line.receipt_file.name}"
                    )
                    continue

                storage = line.receipt_file.storage
                if not storage.exists(receipt_file):
                    self.stdout.write(
                        f"SKIP line={line_id}: file not found {receipt_file}"
                    )
                    continue

                old_file = line.receipt_file.name if line.receipt_file else ""
                audit_rows.append([
                    line.id,
                    line.request_id,
                    old_file,
                    receipt_file,
                ])

                self.stdout.write(
                    f"OK line={line.id} req={line.request_id} -> {receipt_file}"
                )

                if opts["apply"]:
                    line.receipt_file = receipt_file
                    line.save(update_fields=["receipt_file", "updated_at"])

                    exp = line.expense_item
                    if exp and not exp.receipt_file:
                        exp.receipt_file = receipt_file
                        exp.save(update_fields=["receipt_file", "updated_at"])

            if opts["dry_run"]:
                transaction.set_rollback(True)

        with open(opts["audit"], "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "line_id",
                "request_id",
                "old_receipt_file",
                "new_receipt_file",
            ])
            writer.writerows(audit_rows)

        self.stdout.write(f"Audit written: {opts['audit']}")