from django.db import migrations, models
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0008_seed_leave_types"),
    ]

    operations = [
        migrations.AddField(
            model_name="leaverequest",
            name="approver",
            field=models.ForeignKey(
                to=settings.AUTH_USER_MODEL,
                on_delete=models.SET_NULL,
                null=True,
                blank=True,
                related_name="approved_leaves",
            ),
        ),
    ]
