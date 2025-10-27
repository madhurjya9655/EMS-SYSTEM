# apps/leave/migrations/0012_restrict_leave_types.py
from django.db import migrations

ALLOWED = ["Casual Leave", "Maternity Leave", "Compensatory Off"]

def forwards(apps, schema_editor):
    LeaveType = apps.get_model("leave", "LeaveType")
    LeaveRequest = apps.get_model("leave", "LeaveRequest")

    # Ensure the three canonical leave types exist, create if missing
    name_to_obj = {}
    for name in ALLOWED:
        obj, _ = LeaveType.objects.get_or_create(name=name, defaults={"default_days": 0})
        name_to_obj[name] = obj

    # Any LeaveRequest pointing to a disallowed LeaveType should be reassigned
    # to "Casual Leave" (safe default) before we delete the disallowed types.
    casual = name_to_obj["Casual Leave"]

    disallowed = LeaveType.objects.exclude(name__in=ALLOWED)
    disallowed_ids = list(disallowed.values_list("id", flat=True))

    if disallowed_ids:
        LeaveRequest.objects.filter(leave_type_id__in=disallowed_ids).update(leave_type_id=casual.id)
        # Now we can safely delete the disallowed types
        disallowed.delete()

def backwards(apps, schema_editor):
    """
    No-op safe reverse: we won't recreate previously deleted types.
    """
    # Intentionally left empty.
    return

class Migration(migrations.Migration):

    # IMPORTANT: set this to your last real migration
    dependencies = [
        ("leave", "0011_add_default_cc_users"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
