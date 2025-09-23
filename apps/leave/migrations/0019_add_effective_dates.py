from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0018_create_leavehandover"),  # the migration that created LeaveHandover
    ]

    operations = [
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
    ]
