import sqlite3
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.apps import apps


class Command(BaseCommand):
    help = "Fix datetime BLOB values in SQLite database by converting them to proper ISO 8601 text format"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be fixed without making changes",
        )
        parser.add_argument(
            "--model",
            type=str,
            help='Only fix specific model (e.g., "Delegation", "Checklist", "HelpTicket")',
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show detailed output for each fix",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of records to process in each batch (default: 100)",
        )

    def handle(self, *args, **options):
        # Only meaningful for SQLite backends
        if connection.vendor != "sqlite":
            raise CommandError("This command is intended for SQLite databases only.")

        self.dry_run = options["dry_run"]
        self.verbose = options["verbose"]
        self.batch_size = options["batch_size"]
        self.target_model = options.get("model")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE: No changes will be made to the database"))

        # Basic integrity check up front
        self._check_database_integrity()

        # Get all models that might have datetime issues
        models_to_check = self._get_models_with_datetime_fields()

        if self.target_model:
            # Filter to specific model
            models_to_check = [
                (model, fields)
                for model, fields in models_to_check
                if model._meta.model_name.lower() == self.target_model.lower()
            ]
            if not models_to_check:
                raise CommandError(f'Model "{self.target_model}" not found or has no datetime fields')

        total_fixed = 0

        for model, datetime_fields in models_to_check:
            model_name = f"{model._meta.app_label}.{model._meta.model_name}"
            self.stdout.write(f"\nChecking {model_name}...")

            fixed_count = self._fix_model_datetime_blobs(model, datetime_fields)
            total_fixed += fixed_count

            if fixed_count > 0:
                self.stdout.write(self.style.SUCCESS(f"  Fixed {fixed_count} records in {model_name}"))
            else:
                self.stdout.write(f"  No issues found in {model_name}")

        if total_fixed > 0:
            if self.dry_run:
                self.stdout.write(self.style.WARNING(f"\nDRY RUN: Would fix {total_fixed} datetime BLOB values"))
            else:
                self.stdout.write(self.style.SUCCESS(f"\nSuccessfully fixed {total_fixed} datetime BLOB values"))
        else:
            self.stdout.write(self.style.SUCCESS("\nNo datetime BLOB issues found in the database"))

    def _get_models_with_datetime_fields(self):
        """Get all models that have DateTimeField columns."""
        models_with_datetime = []

        # Check specific models we know have datetime issues
        target_models = ["tasks.Delegation", "tasks.Checklist", "tasks.HelpTicket", "tasks.FMS"]

        for model_path in target_models:
            try:
                model = apps.get_model(model_path)
                datetime_fields = []

                for field in model._meta.get_fields():
                    if hasattr(field, "get_internal_type") and field.get_internal_type() == "DateTimeField":
                        datetime_fields.append(field.column)

                if datetime_fields:
                    models_with_datetime.append((model, datetime_fields))

            except LookupError:
                # Model doesn't exist, skip
                continue

        return models_with_datetime

    def _fix_model_datetime_blobs(self, model, datetime_fields):
        """Fix datetime BLOB values for a specific model."""
        table_name = model._meta.db_table
        fixed_count = 0

        with connection.cursor() as cursor:
            # Check each datetime field for BLOBs
            for field_name in datetime_fields:
                if self.verbose:
                    self.stdout.write(f"    Checking field: {field_name}")

                # Select records where the datetime field might be a BLOB
                # We use typeof() to identify BLOB values
                cursor.execute(
                    f"""
                    SELECT id, {field_name}
                    FROM {table_name}
                    WHERE {field_name} IS NOT NULL
                      AND typeof({field_name}) = 'blob'
                    ORDER BY id
                """
                )

                blob_records = cursor.fetchall()

                if not blob_records:
                    continue

                if self.verbose:
                    self.stdout.write(f"      Found {len(blob_records)} records with BLOB {field_name}")

                # Process in batches
                for i in range(0, len(blob_records), self.batch_size):
                    batch = blob_records[i : i + self.batch_size]
                    batch_fixed = self._fix_datetime_batch(cursor, table_name, field_name, batch)
                    fixed_count += batch_fixed

        return fixed_count

    def _fix_datetime_batch(self, cursor, table_name, field_name, batch):
        """Fix a batch of datetime BLOB records."""
        fixed_count = 0

        for record_id, blob_value in batch:
            try:
                # Decode the BLOB value
                fixed_value = self._decode_datetime_blob(blob_value)

                if fixed_value:
                    if self.verbose:
                        self.stdout.write(f"        Record {record_id}: {blob_value!r} -> {fixed_value}")

                    if not self.dry_run:
                        # Update the record with the fixed value (TEXT ISO8601)
                        cursor.execute(
                            f"""
                            UPDATE {table_name}
                            SET {field_name} = ?
                            WHERE id = ?
                        """,
                            [fixed_value, record_id],
                        )

                    fixed_count += 1
                else:
                    if self.verbose:
                        self.stdout.write(f"        Record {record_id}: Could not decode {blob_value!r}")

            except Exception as e:
                if self.verbose:
                    self.stdout.write(f"        Record {record_id}: Error - {e}")
                continue

        return fixed_count

    def _decode_datetime_blob(self, blob_value):
        """
        Decode a datetime BLOB value into a proper ISO 8601 string (UTC, seconds resolution).
        Returns None if the value cannot be decoded properly.
        """
        if blob_value is None:
            return None

        # Handle different BLOB types
        if isinstance(blob_value, memoryview):
            blob_value = blob_value.tobytes()

        if isinstance(blob_value, (bytes, bytearray)):
            # Try multiple decoding strategies
            for encoding in ("utf-8", "latin-1", "ascii", "cp1252"):
                try:
                    decoded = blob_value.decode(encoding).strip().replace("\x00", "")
                    if decoded:
                        return self._normalize_datetime_string(decoded)
                except (UnicodeDecodeError, AttributeError):
                    continue

        # If it's already a string, just normalize it
        if isinstance(blob_value, str):
            cleaned = blob_value.strip().replace("\x00", "")
            if cleaned:
                return self._normalize_datetime_string(cleaned)

        # For other types, try to convert to string
        try:
            string_val = str(blob_value).strip()
            if string_val and string_val != "None":
                return self._normalize_datetime_string(string_val)
        except Exception:
            pass

        return None

    def _normalize_datetime_string(self, dt_string):
        """
        Normalize a datetime string to ISO 8601 (UTC) format: 'YYYY-MM-DD HH:MM:SS'.
        """
        if not dt_string or dt_string.lower() in ("none", "null", ""):
            return None

        # Try Django's robust parser first
        try:
            parsed_dt = parse_datetime(dt_string)
            if parsed_dt:
                if timezone.is_aware(parsed_dt):
                    utc_dt = parsed_dt.astimezone(timezone.utc)
                else:
                    aware_dt = timezone.make_aware(parsed_dt, timezone.get_current_timezone())
                    utc_dt = aware_dt.astimezone(timezone.utc)
                return utc_dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            pass

        # Try common formats
        clean_string = dt_string.strip()
        for prefix in ("datetime(", "timestamp("):
            if clean_string.startswith(prefix):
                clean_string = clean_string[len(prefix) :]
        if clean_string.endswith(")"):
            clean_string = clean_string[:-1]

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
        ):
            try:
                parsed_dt = datetime.strptime(clean_string, fmt)
                utc_dt = timezone.make_aware(parsed_dt, timezone.utc)
                return utc_dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        # If all parsing fails, return a cleaned value if it looks datetime-ish
        if len(dt_string) >= 10 and any(c.isdigit() for c in dt_string):
            return dt_string.strip()

        return None

    def _check_database_integrity(self):
        """Verify that the database is accessible and not corrupted (SQLite)."""
        try:
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                if not result or result[0] != "ok":
                    raise CommandError("Database integrity check failed")
        except Exception as e:
            raise CommandError(f"Database check failed: {e}")
