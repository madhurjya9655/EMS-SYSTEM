from django.db import migrations


def add_missing_columns(apps, schema_editor):
    connection = schema_editor.connection
    vendor = connection.vendor

    with connection.cursor() as c:
        def has_column(table, col):
            if vendor == "sqlite":
                c.execute(f"PRAGMA table_info({table})")
                return any(row[1] == col for row in c.fetchall())

            if vendor == "postgresql":
                c.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                    LIMIT 1
                    """,
                    [table, col],
                )
                return c.fetchone() is not None

            raise NotImplementedError(f"Unsupported database vendor: {vendor}")

        if not has_column("leave_leaverequest", "is_half_day"):
            if vendor == "sqlite":
                c.execute(
                    "ALTER TABLE leave_leaverequest "
                    "ADD COLUMN is_half_day BOOLEAN NOT NULL DEFAULT 0"
                )
            elif vendor == "postgresql":
                c.execute(
                    "ALTER TABLE leave_leaverequest "
                    "ADD COLUMN is_half_day BOOLEAN NOT NULL DEFAULT FALSE"
                )

        if not has_column("leave_leavehandover", "message"):
            c.execute("ALTER TABLE leave_leavehandover ADD COLUMN message TEXT")


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0005_update_help_text_cc_users"),
    ]

    operations = [
        migrations.RunPython(add_missing_columns, noop),
    ]