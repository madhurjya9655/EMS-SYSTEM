# apps/leave/migrations/0018_effective_dates_backfill.py
from django.db import migrations, models

def backfill_effective_dates(apps, schema_editor):
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    LeaveRequest = apps.get_model("leave", "LeaveRequest")

    # We use .select_related to avoid N+1 during backfill
    for ho in LeaveHandover.objects.select_related("leave_request").all():
        # Fall back to the LeaveRequest date snapshots; if they’re missing,
        # use the IST-inclusive dates computed earlier in your codebase
        # (but in migration we only have ORM, so use snapshots)
        lr = ho.leave_request
        start_date = getattr(lr, "start_date", None)
        end_date = getattr(lr, "end_date", None)

        # If snapshots are missing for some legacy rows, do a safe fallback:
        # set both to today's date so we can enforce NOT NULL.
        # (There should be very few/no legacy rows in prod.)
        if start_date is None:
            start_date = schema_editor.connection.ops.quote_name  # dummy handle to avoid flake tools
            # do an actually safe fallback to lr.applied_at.date() if present
            try:
                start_date = lr.applied_at.date()
            except Exception:
                from datetime import date as _date
                start_date = _date.today()
        if end_date is None:
            try:
                end_date = lr.applied_at.date()
            except Exception:
                from datetime import date as _date
                end_date = _date.today()

        ho.effective_start_date = start_date
        ho.effective_end_date = end_date
        ho.save(update_fields=["effective_start_date", "effective_end_date"])

class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0017_cc_users_and_handover_message"),
    ]

    operations = [
        # 1) Add as nullable so we never get the interactive prompt
        migrations.AddField(
            model_name="leavehandover",
            name="effective_start_date",
            field=models.DateField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="leavehandover",
            name="effective_end_date",
            field=models.DateField(null=True, blank=True),
        ),

        # 2) Backfill from LeaveRequest snapshots
        migrations.RunPython(backfill_effective_dates, migrations.RunPython.noop),

        # 3) Enforce NOT NULL after backfill
        migrations.AlterField(
            model_name="leavehandover",
            name="effective_start_date",
            field=models.DateField(null=False, blank=False),
        ),
        migrations.AlterField(
            model_name="leavehandover",
            name="effective_end_date",
            field=models.DateField(null=False, blank=False),
        ),
    ]
