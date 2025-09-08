from django.conf import settings
from django.db import migrations, models


def ensure_schema(apps, schema_editor):
    """
    Idempotent schema fixer:
    - Creates leave_approvermapping only if missing.
    - Adds missing columns/indexes to leave_leaverequest only if absent.
    Works on SQLite; safe to run when objects already exist.
    """
    conn = schema_editor.connection
    with conn.cursor() as c:
        # --- helpers ---
        def table_exists(name: str) -> bool:
            return name in conn.introspection.table_names()

        def column_names(table: str):
            c.execute(f"PRAGMA table_info({table})")
            return {row[1] for row in c.fetchall()}

        # --- resolve user table ---
        app_label, model_name = settings.AUTH_USER_MODEL.split(".")
        User = apps.get_model(app_label, model_name)
        user_table = User._meta.db_table

        approver_table = "leave_approvermapping"
        leave_table = "leave_leaverequest"

        # 1) ApproverMapping table
        if not table_exists(approver_table):
            c.execute(
                f"""
                CREATE TABLE {approver_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    updated_at DATETIME NOT NULL,
                    notes TEXT NOT NULL DEFAULT '',
                    employee_id INTEGER NOT NULL UNIQUE
                        REFERENCES {user_table}(id) ON DELETE CASCADE,
                    reporting_person_id INTEGER NOT NULL
                        REFERENCES {user_table}(id) ON DELETE RESTRICT,
                    cc_person_id INTEGER NOT NULL
                        REFERENCES {user_table}(id) ON DELETE RESTRICT
                );
                """
            )

        # 2) LeaveRequest missing columns
        cols = column_names(leave_table)

        if "reporting_person_id" not in cols:
            c.execute(
                f"""
                ALTER TABLE {leave_table}
                ADD COLUMN reporting_person_id INTEGER
                    REFERENCES {user_table}(id) ON DELETE SET NULL;
                """
            )
        if "cc_person_id" not in cols:
            c.execute(
                f"""
                ALTER TABLE {leave_table}
                ADD COLUMN cc_person_id INTEGER
                    REFERENCES {user_table}(id) ON DELETE SET NULL;
                """
            )
        if "start_date" not in cols:
            c.execute(f"ALTER TABLE {leave_table} ADD COLUMN start_date DATE;")
        if "end_date" not in cols:
            c.execute(f"ALTER TABLE {leave_table} ADD COLUMN end_date DATE;")

        # 3) Indexes
        c.execute(
            f"CREATE INDEX IF NOT EXISTS leave_leave_reporti_1b231a_idx "
            f"ON {leave_table} (reporting_person_id);"
        )
        c.execute(
            f"CREATE INDEX IF NOT EXISTS leave_leave_start_d_611c6f_idx "
            f"ON {leave_table} (start_date, end_date);"
        )


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0001_initial"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # Perform DB changes conditionally
        migrations.RunPython(ensure_schema, migrations.RunPython.noop),

        # Tell Django's *state* what exists now (no DB ops here)
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.CreateModel(
                    name="ApproverMapping",
                    fields=[
                        ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                        ("updated_at", models.DateTimeField(auto_now=True)),
                        ("notes", models.TextField(blank=True)),
                        ("employee", models.OneToOneField(
                            on_delete=models.deletion.CASCADE,
                            related_name="approver_mapping",
                            to=settings.AUTH_USER_MODEL,
                        )),
                        ("reporting_person", models.ForeignKey(
                            on_delete=models.deletion.PROTECT,
                            related_name="reports_for_approval",
                            to=settings.AUTH_USER_MODEL,
                        )),
                        ("cc_person", models.ForeignKey(
                            on_delete=models.deletion.PROTECT,
                            related_name="cc_for_approval",
                            to=settings.AUTH_USER_MODEL,
                        )),
                    ],
                    options={
                        "ordering": ["employee__id"],
                        "verbose_name": "Approver Mapping",
                        "verbose_name_plural": "Approver Mappings",
                    },
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="reporting_person",
                    field=models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="leave_requests_to_approve",
                        to=settings.AUTH_USER_MODEL,
                        help_text="Reporting Person (manager) who must approve.",
                    ),
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="cc_person",
                    field=models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="leave_requests_cc",
                        to=settings.AUTH_USER_MODEL,
                        help_text="HR (or other) observer.",
                    ),
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="start_date",
                    field=models.DateField(null=True, blank=True, editable=False),
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="end_date",
                    field=models.DateField(null=True, blank=True, editable=False),
                ),
                migrations.AddIndex(
                    model_name="leaverequest",
                    index=models.Index(fields=["reporting_person"], name="leave_leave_reporti_1b231a_idx"),
                ),
                migrations.AddIndex(
                    model_name="leaverequest",
                    index=models.Index(fields=["start_date", "end_date"], name="leave_leave_start_d_611c6f_idx"),
                ),
            ],
        ),
    ]
