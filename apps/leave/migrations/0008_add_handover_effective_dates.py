from django.db import migrations


def add_effective_columns(apps, schema_editor):
    connection = schema_editor.connection
    vendor = connection.vendor

    with connection.cursor() as cursor:
        def has_col(table, col):
            if vendor == "sqlite":
                cursor.execute(f"PRAGMA table_info({table})")
                return any(r[1] == col for r in cursor.fetchall())

            if vendor == "postgresql":
                cursor.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                    """,
                    [table, col],
                )
                return cursor.fetchone() is not None

            raise NotImplementedError(f"Unsupported database vendor: {vendor}")

        if not has_col("leave_leavehandover", "effective_start_date"):
            cursor.execute(
                "ALTER TABLE leave_leavehandover ADD COLUMN effective_start_date DATE"
            )

        if not has_col("leave_leavehandover", "effective_end_date"):
            cursor.execute(
                "ALTER TABLE leave_leavehandover ADD COLUMN effective_end_date DATE"
            )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS leave_handover_effective_dates_idx "
            "ON leave_leavehandover (effective_start_date, effective_end_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS leave_handover_new_assignee_is_active_idx "
            "ON leave_leavehandover (new_assignee_id, is_active)"
        )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0007_fix_more_schema_desync"),
    ]

    operations = [
        migrations.RunPython(add_effective_columns, noop),
    ]