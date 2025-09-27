from django.db import migrations

def add_effective_columns(apps, schema_editor):
    c = schema_editor.connection.cursor()

    def has_col(table, col):
        c.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in c.fetchall())

    if not has_col("leave_leavehandover", "effective_start_date"):
        c.execute("ALTER TABLE leave_leavehandover ADD COLUMN effective_start_date DATE")

    if not has_col("leave_leavehandover", "effective_end_date"):
        c.execute("ALTER TABLE leave_leavehandover ADD COLUMN effective_end_date DATE")

    c.execute("CREATE INDEX IF NOT EXISTS leave_handover_effective_dates_idx ON leave_leavehandover (effective_start_date, effective_end_date)")
    c.execute("CREATE INDEX IF NOT EXISTS leave_handover_new_assignee_is_active_idx ON leave_leavehandover (new_assignee_id, is_active)")

def noop(apps, schema_editor):
    pass

class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0007_fix_more_schema_desync"),
    ]
    operations = [
        migrations.RunPython(add_effective_columns, noop),
    ]