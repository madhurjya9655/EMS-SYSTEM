from django.db import migrations, models


def backfill_effective_dates(apps, schema_editor):
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    LeaveRequest = apps.get_model("leave", "LeaveRequest")

    # Iterate without selecting the new columns (avoid SELECT * which can choke if state mismatches)
    for ho_id, lr_id in LeaveHandover.objects.values_list("id", "leave_request_id").iterator():
        lr = LeaveRequest.objects.filter(id=lr_id).only("start_date", "end_date", "applied_at").first()
        if not lr:
            continue

        start_date = getattr(lr, "start_date", None)
        end_date = getattr(lr, "end_date", None)

        # Fallbacks: use applied_at.date() if snapshots are not present
        if not start_date and getattr(lr, "applied_at", None):
            start_date = lr.applied_at.date()
        if not end_date and getattr(lr, "applied_at", None):
            end_date = lr.applied_at.date()

        # Absolute fallbacks if still missing
        if not start_date or not end_date:
            from datetime import date as _d
            today = _d.today()
            start_date = start_date or today
            end_date = end_date or today

        # Update without loading whole object
        LeaveHandover.objects.filter(id=ho_id).update(
            effective_start_date=start_date,
            effective_end_date=end_date,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0019_add_effective_dates"),
    ]

    operations = [
        migrations.RunPython(backfill_effective_dates, migrations.RunPython.noop),
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
