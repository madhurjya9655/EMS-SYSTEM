# apps/leave/migrations/0008_seed_leave_types.py
from django.db import migrations


def seed_leave_types(apps, schema_editor):
    LeaveType = apps.get_model("leave", "LeaveType")
    items = [
        ("Casual Leave", 12),
        ("Sick Leave", 10),
        ("Earned Leave", 0),
        ("Comp Off", 0),
        ("Unpaid Leave", 0),
        ("Maternity Leave", 0),
        ("Paternity Leave", 0),
        ("Bereavement Leave", 0),
        ("Marriage Leave", 0),
        ("Other", 0),
    ]
    for name, default_days in items:
        # idempotent: create or update default_days if it already exists
        LeaveType.objects.update_or_create(
            name=name,
            defaults={"default_days": default_days},
        )


def unseed_leave_types(apps, schema_editor):
    LeaveType = apps.get_model("leave", "LeaveType")
    names = [
        "Casual Leave",
        "Sick Leave",
        "Earned Leave",
        "Comp Off",
        "Unpaid Leave",
        "Maternity Leave",
        "Paternity Leave",
        "Bereavement Leave",
        "Marriage Leave",
        "Other",
    ]
    LeaveType.objects.filter(name__in=names).delete()


class Migration(migrations.Migration):

    # If you created 0007_add_attachment_field earlier, keep that as dependency,
    # otherwise point this to your latest migration.
    dependencies = [
        ("leave", "0007_add_attachment_field"),
    ]

    operations = [
        migrations.RunPython(seed_leave_types, unseed_leave_types),
    ]
