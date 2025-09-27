from django.db import migrations

def add_missing_columns(apps, schema_editor):
    c = schema_editor.connection.cursor()

    def has_col(table, col):
        c.execute(f"PRAGMA table_info({table})")
        return any(row[1] == col for row in c.fetchall())

    if not has_col("leave_leaverequest", "attachment"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN attachment TEXT")

    if not has_col("leave_leaverequest", "approver_id"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN approver_id INTEGER")

    if not has_col("leave_leaverequest", "decided_at"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN decided_at DATETIME")

    if not has_col("leave_leaverequest", "decision_comment"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN decision_comment TEXT NOT NULL DEFAULT ''")

    if not has_col("leave_leaverequest", "employee_name"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN employee_name VARCHAR(150) NOT NULL DEFAULT ''")

    if not has_col("leave_leaverequest", "employee_email"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN employee_email VARCHAR(254) NOT NULL DEFAULT ''")

    if not has_col("leave_leaverequest", "employee_designation"):
        c.execute("ALTER TABLE leave_leaverequest ADD COLUMN employee_designation VARCHAR(150) NOT NULL DEFAULT ''")

def noop(apps, schema_editor):
    pass

class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0006_fix_schema_desync"),
    ]
    operations = [
        migrations.RunPython(add_missing_columns, noop),
    ]