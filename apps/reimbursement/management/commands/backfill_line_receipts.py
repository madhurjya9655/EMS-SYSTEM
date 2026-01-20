from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from apps.reimbursement.models import ReimbursementLine


class Command(BaseCommand):
    help = (
        "Copy receipt files from ExpenseItem to ReimbursementLine "
        "where the line has no file but the expense has one."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would change without saving.",
        )

    def handle(self, *args, **opts):
        dry = bool(opts.get("dry_run"))
        # Lines with no receipt_file (NULL or empty string)
        qs = (
            ReimbursementLine.objects.select_related("expense_item")
            .filter(Q(receipt_file__isnull=True) | Q(receipt_file__exact=""))
            .order_by("id")
        )

        total = qs.count()
        fixed = 0
        skipped_no_expense = 0
        skipped_missing_file = 0
        skipped_storage_missing = 0

        self.stdout.write(f"Scanning {total} line(s) with empty receipt_file...")

        # We don't need a global atomic block for dry-run. For real runs,
        # a single transaction is fine. If something fails, no partial writes.
        if dry:
            for line in qs.iterator():
                exp = line.expense_item
                if not exp:
                    skipped_no_expense += 1
                    continue
                f = getattr(exp, "receipt_file", None)
                name = getattr(f, "name", "") if f else ""
                if not name:
                    skipped_missing_file += 1
                    continue
                # Try checking storage existence; if storage backend fails, we still count
                try:
                    if hasattr(f, "storage") and f.storage and not f.storage.exists(name):
                        skipped_storage_missing += 1
                        continue
                except Exception:
                    # If the storage check errors, assume ok (we're only previewing)
                    pass
                fixed += 1

            self._print_summary(total, fixed, skipped_no_expense, skipped_missing_file, skipped_storage_missing, dry=True)
            return

        # Real write
        with transaction.atomic():
            for line in qs.iterator():
                exp = line.expense_item
                if not exp:
                    skipped_no_expense += 1
                    continue
                f = getattr(exp, "receipt_file", None)
                name = getattr(f, "name", "") if f else ""
                if not name:
                    skipped_missing_file += 1
                    continue
                try:
                    if hasattr(f, "storage") and f.storage and not f.storage.exists(name):
                        skipped_storage_missing += 1
                        continue
                except Exception:
                    # If storage existence lookup fails, proceed anyway rather than blocking the fix.
                    pass

                line.receipt_file = f
                line.save(update_fields=["receipt_file", "updated_at"])
                fixed += 1

            self._print_summary(total, fixed, skipped_no_expense, skipped_missing_file, skipped_storage_missing, dry=False)

    def _print_summary(self, total, fixed, skipped_no_expense, skipped_missing_file, skipped_storage_missing, *, dry):
        mode = "DRY-RUN" if dry else "APPLIED"
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"=== {mode} SUMMARY ==="))
        self.stdout.write(f"Candidates: {total}")
        self.stdout.write(self.style.SUCCESS(f"Updated (or would update): {fixed}"))
        self.stdout.write(f"Skipped (no expense on line): {skipped_no_expense}")
        self.stdout.write(f"Skipped (expense has no file): {skipped_missing_file}")
        self.stdout.write(f"Skipped (file missing in storage): {skipped_storage_missing}")
        self.stdout.write(self.style.NOTICE("=== END SUMMARY ==="))
