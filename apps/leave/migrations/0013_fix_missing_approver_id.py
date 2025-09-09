# apps/leave/migrations/0013_fix_missing_approver_id.py
from django.db import migrations, connection

def ensure_approver_id(apps, schema_editor):
    table = "leave_leaverequest"
    col = "approver_id"

    with connection.cursor() as cursor:
        # Check the actual columns present in SQLite
        cursor.execute(f"PRAGMA table_info('{table}')")
        cols = [row[1] for row in cursor.fetchall()]  # row[1] is column name
        if col in cols:
            return  # already there, nothing to do

        # Add the column (NULL FK to auth_user.id). SQLite allows adding columns this way.
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} integer NULL REFERENCES auth_user(id)")
        # (No explicit index required for select_related to work.)

def noop(apps, schema_editor):
    pass

class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0012_add_employee_snapshots"),
    ]

    operations = [
        migrations.RunPython(ensure_approver_id, noop),
    ]
