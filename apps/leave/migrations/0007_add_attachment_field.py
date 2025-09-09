# apps/leave/migrations/0007_add_attachment_field.py
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('leave', '0006_update_leave_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='leaverequest',
            name='attachment',
            field=models.FileField(
                upload_to='apps.leave.models.leave_attachment_upload_to',
                null=True,
                blank=True,
            ),
        ),
    ]
