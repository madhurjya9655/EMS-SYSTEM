from django.db import migrations, models


def backfill_effective_dates(apps, schema_editor):
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    LeaveRequest = apps.get_model("leave", "LeaveRequest")

    # Fill any NULLs using the leave_request snapshots
    for ho in LeaveHandover.objects.select_related("leave_request").all():
        lr = ho.leave_request
        start_date = getattr(lr, "start_date", None)
        end_date = getattr(lr, "end_date", None)

        # fallback: use applied_at date if snapshots are empty
        if start_date is None and getattr(lr, "applied_at", None):
            start_date = lr.applied_at.date()
        if end_date is None and getattr(lr, "applied_at", None):
            end_date = lr.applied_at.date()

        # absolute fallback to today if still missing
        from datetime import date as _d
        if start_date is None:
            start_date = _d.today()
        if end_date is None:
            end_date = _d.today()

        # ensure pure date
        if hasattr(start_date, "date"):
            start_date = start_date.date()
        if hasattr(end_date, "date"):
            end_date = end_date.date()

        updates = {}
        if ho.effective_start_date is None:
            updates["effective_start_date"] = start_date
        if ho.effective_end_date is None:
            updates["effective_end_date"] = end_date
        if updates:
            for k, v in updates.items():
                setattr(ho, k, v)
            ho.save(update_fields=list(updates.keys()))


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0018_create_leavehandover"),
    ]

    operations = [
        migrations.RunPython(backfill_effective_dates, migrations.RunPython.noop),

        # Now enforce NOT NULL once data is filled
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
