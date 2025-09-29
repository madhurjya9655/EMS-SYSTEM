from django.conf import settings
from django.db import migrations, models


def copy_cc_person_to_default_cc_users(apps, schema_editor):
    ApproverMapping = apps.get_model("leave", "ApproverMapping")
    db_alias = schema_editor.connection.alias
    for mapping in ApproverMapping.objects.using(db_alias).all():
        cc_id = getattr(mapping, "cc_person_id", None)
        if cc_id:
            # Use the M2M manager available on historical model
            mapping.default_cc_users.add(cc_id)


def reverse_noop(apps, schema_editor):
    # Keep data in M2M; no reverse action.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0010_backfill_handover_timestamps"),
    ]

    operations = [
        migrations.AddField(
            model_name="approvermapping",
            name="default_cc_users",
            field=models.ManyToManyField(
                to=settings.AUTH_USER_MODEL,
                blank=True,
                related_name="default_cc_for",
                help_text="Multiple default CC recipients (admin-managed).",
            ),
        ),
        migrations.RunPython(copy_cc_person_to_default_cc_users, reverse_noop),
    ]
