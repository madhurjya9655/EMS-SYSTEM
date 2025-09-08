from django.db import migrations


def add_blocked_days(apps, schema_editor):
    """
    Render DB is missing the 'blocked_days' column on leave_leaverequest.
    Add it ONLY if absent (SQLite-safe).
    """
    conn = schema_editor.connection
    with conn.cursor() as c:
        c.execute("PRAGMA table_info(leave_leaverequest)")
        cols = {row[1] for row in c.fetchall()}
        if "blocked_days" not in cols:
            c.execute(
                "ALTER TABLE leave_leaverequest "
                "ADD COLUMN blocked_days REAL NOT NULL DEFAULT 0.0"
            )


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0002_approver_mapping"),
    ]

    operations = [
        migrations.RunPython(add_blocked_days, migrations.RunPython.noop),
    ]
