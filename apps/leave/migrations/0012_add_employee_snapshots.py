# apps/leave/migrations/0012_add_employee_snapshots.py
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0011_add_decision_comment"),  # last one you just applied
    ]

    operations = [
        migrations.AddField(
            model_name="leaverequest",
            name="employee_name",
            # backfill existing rows once with "", then no default going forward
            field=models.CharField(max_length=150, blank=True, default=""),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="employee_email",
            field=models.EmailField(max_length=254, blank=True, default=""),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="employee_designation",
            field=models.CharField(max_length=150, blank=True, default=""),
            preserve_default=False,
        ),
    ]
