# apps/leave/migrations/0018_create_leavehandover.py
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def create_table_if_missing(apps, schema_editor):
    """
    Only create the table if it doesn't exist.
    This handles prod where the table may already be present.
    """
    table_names = set(schema_editor.connection.introspection.table_names())
    if "leave_leavehandover" in table_names:
        return

    # use historical model
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    # Let Django build the proper CREATE TABLE + indexes
    schema_editor.create_model(LeaveHandover)


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0017_cc_users_and_handover_message"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # 1) DB operation (conditional)
        migrations.RunPython(create_table_if_missing, migrations.RunPython.noop),

        # 2) State operation (always add model to migration graph)
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.CreateModel(
                    name="LeaveHandover",
                    fields=[
                        ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                        ("task_type", models.CharField(
                            max_length=20,
                            choices=[("checklist", "Checklist"), ("delegation", "Delegation"), ("help_ticket", "Help Ticket")]
                        )),
                        ("original_task_id", models.PositiveIntegerField()),
                        ("message", models.TextField(blank=True)),

                        # nullable first; 0019 will backfill & enforce NOT NULL
                        ("effective_start_date", models.DateField(null=True, blank=True)),
                        ("effective_end_date", models.DateField(null=True, blank=True)),

                        ("is_active", models.BooleanField(default=True)),
                        ("created_at", models.DateTimeField(auto_now_add=True)),
                        ("updated_at", models.DateTimeField(auto_now=True)),

                        ("leave_request", models.ForeignKey(
                            on_delete=django.db.models.deletion.CASCADE,
                            related_name="handovers",
                            to="leave.leaverequest"
                        )),
                        ("original_assignee", models.ForeignKey(
                            on_delete=django.db.models.deletion.CASCADE,
                            related_name="handovers_given",
                            to=settings.AUTH_USER_MODEL
                        )),
                        ("new_assignee", models.ForeignKey(
                            on_delete=django.db.models.deletion.CASCADE,
                            related_name="handovers_received",
                            to=settings.AUTH_USER_MODEL
                        )),
                    ],
                    options={
                        "indexes": [
                            models.Index(fields=["task_type", "original_task_id"], name="leave_handover_task_obj_idx"),
                            models.Index(fields=["new_assignee"], name="leave_handover_new_idx"),
                            models.Index(fields=["effective_start_date", "effective_end_date"], name="leave_handover_eff_idx"),
                            models.Index(fields=["is_active"], name="leave_handover_active_idx"),
                        ],
                        "unique_together": {("task_type", "original_task_id", "leave_request", "new_assignee")},
                    },
                ),
            ],
        ),
    ]
