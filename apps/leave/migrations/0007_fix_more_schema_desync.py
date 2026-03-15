from django.db import migrations


def add_missing_columns(apps, schema_editor):
    connection = schema_editor.connection
    vendor = connection.vendor

    with connection.cursor() as cursor:
        def has_col(table, col):
            if vendor == "sqlite":
                cursor.execute(f"PRAGMA table_info({table})")
                return any(row[1] == col for row in cursor.fetchall())

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

        if not has_col("leave_leaverequest", "attachment"):
            cursor.execute("ALTER TABLE leave_leaverequest ADD COLUMN attachment TEXT")

        if not has_col("leave_leaverequest", "approver_id"):
            cursor.execute("ALTER TABLE leave_leaverequest ADD COLUMN approver_id INTEGER")

        if not has_col("leave_leaverequest", "decided_at"):
            cursor.execute("ALTER TABLE leave_leaverequest ADD COLUMN decided_at TIMESTAMP NULL")

        if not has_col("leave_leaverequest", "decision_comment"):
            cursor.execute(
                "ALTER TABLE leave_leaverequest ADD COLUMN decision_comment TEXT NOT NULL DEFAULT ''"
            )

        if not has_col("leave_leaverequest", "employee_name"):
            cursor.execute(
                "ALTER TABLE leave_leaverequest ADD COLUMN employee_name VARCHAR(150) NOT NULL DEFAULT ''"
            )

        if not has_col("leave_leaverequest", "employee_email"):
            cursor.execute(
                "ALTER TABLE leave_leaverequest ADD COLUMN employee_email VARCHAR(254) NOT NULL DEFAULT ''"
            )

        if not has_col("leave_leaverequest", "employee_designation"):
            cursor.execute(
                "ALTER TABLE leave_leaverequest ADD COLUMN employee_designation VARCHAR(150) NOT NULL DEFAULT ''"
            )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0006_fix_schema_desync"),
    ]

    operations = [
        migrations.RunPython(add_missing_columns, noop),
    ]