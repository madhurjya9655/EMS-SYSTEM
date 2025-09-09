from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0009_add_approver_field"),
    ]

    operations = [
        migrations.AddField(
            model_name="leaverequest",
            name="decided_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
