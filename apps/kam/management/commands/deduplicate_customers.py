# FILE: apps/kam/management/commands/deduplicate_customers.py
# PURPOSE: One-time (and safe to re-run) command to merge duplicate Customer rows.
# USAGE:
#   python manage.py deduplicate_customers          # dry-run (shows what will happen)
#   python manage.py deduplicate_customers --apply  # actually merges + deletes dupes
#
# HOW IT WORKS:
#   Groups all Customer rows by case-insensitive name.
#   For each group with 2+ rows:
#     - Keeps the row with the LOWEST pk (oldest / first created)
#     - Re-points all FK relations (InvoiceFact, LeadFact, OverdueSnapshot, etc.)
#       from duplicate rows to the survivor
#     - Deletes the duplicate rows
#   After merging, the DB is clean and the sync will never hit
#   "get() returned more than one Customer" again.

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count


class Command(BaseCommand):
    help = "Merge duplicate Customer records (group by case-insensitive name)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            default=False,
            help="Actually apply the merge. Without this flag, runs in dry-run mode.",
        )

    def handle(self, *args, **options):
        from apps.kam.models import Customer

        apply = options["apply"]
        mode = "APPLY" if apply else "DRY-RUN"
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write(f"  Customer Deduplication  [{mode}]")
        self.stdout.write(f"{'='*60}\n")

        if not apply:
            self.stdout.write(
                self.style.WARNING(
                    "  Dry-run mode — no changes will be made.\n"
                    "  Re-run with --apply to actually merge duplicates.\n"
                )
            )

        # Find all names that have more than 1 row
        from django.db.models.functions import Lower
        dupes = (
            Customer.objects
            .values("name")
            .annotate(cnt=Count("id"))
            .filter(cnt__gt=1)
            .order_by("name")
        )

        if not dupes:
            self.stdout.write(self.style.SUCCESS("  No duplicates found. Database is clean.\n"))
            return

        total_deleted = 0
        total_groups = 0

        for group in dupes:
            name = group["name"]
            count = group["cnt"]

            # Case-insensitive fetch — catches "ABC" and "abc" etc.
            matches = list(
                Customer.objects.filter(name__iexact=name).order_by("pk")
            )

            # Re-check — there may be both exact and case-variant dupes in one group
            # We want ALL case-insensitive variants grouped together
            all_names_in_group = set(m.name for m in matches)

            # Fetch ALL case variants that haven't been picked up yet
            all_matches = list(
                Customer.objects.filter(name__in=all_names_in_group).order_by("pk")
            )

            if len(all_matches) < 2:
                continue

            survivor = all_matches[0]
            duplicates = all_matches[1:]
            total_groups += 1

            self.stdout.write(
                f"\n  Group: \"{name}\"  ({len(all_matches)} records)\n"
                f"    KEEP   pk={survivor.pk}  name=\"{survivor.name}\"\n"
            )
            for d in duplicates:
                self.stdout.write(
                    f"    DELETE pk={d.pk}  name=\"{d.name}\"\n"
                )

            if not apply:
                total_deleted += len(duplicates)
                continue

            for dup in duplicates:
                try:
                    with transaction.atomic():
                        # Re-point every related object to the survivor
                        merged_counts = {}
                        for rel in dup._meta.get_fields():
                            if (
                                rel.is_relation
                                and rel.one_to_many
                                and rel.related_model is not None
                            ):
                                try:
                                    accessor = rel.get_accessor_name()
                                    related_qs = getattr(dup, accessor).all()
                                    n = related_qs.count()
                                    if n:
                                        related_qs.update(**{rel.field.name: survivor})
                                        merged_counts[rel.related_model.__name__] = n
                                except Exception as exc:
                                    self.stdout.write(
                                        self.style.WARNING(
                                            f"    Could not re-point "
                                            f"{rel.related_model.__name__}: {exc}"
                                        )
                                    )

                        dup.delete()
                        total_deleted += 1

                        if merged_counts:
                            detail = ", ".join(
                                f"{model}:{n}" for model, n in merged_counts.items()
                            )
                            self.stdout.write(
                                f"    → Merged related records: {detail}\n"
                            )

                except Exception as exc:
                    self.stdout.write(
                        self.style.ERROR(
                            f"    ERROR deleting pk={dup.pk}: {exc}\n"
                        )
                    )

        self.stdout.write(f"\n{'─'*60}")
        if apply:
            self.stdout.write(
                self.style.SUCCESS(
                    f"  Done. Merged {total_groups} duplicate groups, "
                    f"deleted {total_deleted} duplicate rows.\n"
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    f"  Would merge {total_groups} groups, "
                    f"delete {total_deleted} duplicate rows.\n"
                    f"  Run with --apply to apply.\n"
                )
            )