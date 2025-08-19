# Create this file: apps/tasks/migrations/0002_optimize_database.py
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0001_initial'),  # Adjust based on your last migration
    ]

    operations = [
        # Add indexes for better performance
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_checklist_assign_to_status_planned ON tasks_checklist (assign_to_id, status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_checklist_assign_to_status_planned;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_checklist_status_planned ON tasks_checklist (status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_checklist_status_planned;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_checklist_mode_status ON tasks_checklist (mode, status);",
            reverse_sql="DROP INDEX IF EXISTS idx_checklist_mode_status;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_delegation_assign_to_status_planned ON tasks_delegation (assign_to_id, status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_delegation_assign_to_status_planned;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_delegation_status_planned ON tasks_delegation (status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_delegation_status_planned;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_helpticket_assign_to_status_planned ON tasks_helpticket (assign_to_id, status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_helpticket_assign_to_status_planned;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_helpticket_status_planned ON tasks_helpticket (status, planned_date);",
            reverse_sql="DROP INDEX IF EXISTS idx_helpticket_status_planned;"
        ),
    ]