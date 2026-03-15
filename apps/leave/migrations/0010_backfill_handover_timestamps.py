from django.db import migrations


def forwards(apps, schema_editor):
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

        table = "leave_leavehandover"

        if not has_col(table, "created_at"):
            if vendor == "sqlite":
                cursor.execute(
                    f"ALTER TABLE {table} ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
                )
            elif vendor == "postgresql":
                cursor.execute(
                    f"ALTER TABLE {table} ADD COLUMN created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP"
                )

        if not has_col(table, "updated_at"):
            if vendor == "sqlite":
                cursor.execute(
                    f"ALTER TABLE {table} ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP"
                )
            elif vendor == "postgresql":
                cursor.execute(
                    f"ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP"
                )

        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {table}_updated_at_idx ON {table} (updated_at)"
        )


def backwards(apps, schema_editor):
    # keep data; no-op on reverse
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0009_backfill_ccconfiguration"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]