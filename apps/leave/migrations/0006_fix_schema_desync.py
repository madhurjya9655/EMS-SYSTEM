from django.db import migrations

def add_missing_columns(apps, schema_editor):
    c = schema_editor.connection.cursor()

    def has_column(table, col):
        c.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in c.fetchall())

    if not has_column("leave_leaverequest", "is_half_day"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN is_half_day BOOLEAN NOT NULL DEFAULT 0")

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
