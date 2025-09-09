# apps/leave/migrations/0006_update_leave_fields.py
from django.db import migrations, models
from django.conf import settings
from django.db.models import deletion


class Migration(migrations.Migration):

    dependencies = [
        ('leave', '0005_start_end_and_mapping_nullable'),
    ]

    operations = [
        # Ensure LeaveRequest FKs match models.py (nullable + SET_NULL)
        migrations.AlterField(
            model_name='leaverequest',
            name='reporting_person',
            field=models.ForeignKey(
                to=settings.AUTH_USER_MODEL,
                related_name='leave_requests_to_approve',
                null=True,
                blank=True,
                on_delete=deletion.SET_NULL,
                help_text='Reporting Person (manager) who must approve.',
            ),
        ),
        migrations.AlterField(
            model_name='leaverequest',
            name='cc_person',
            field=models.ForeignKey(
                to=settings.AUTH_USER_MODEL,
                related_name='leave_requests_cc',
                null=True,
                blank=True,
                on_delete=deletion.SET_NULL,
                help_text='HR (or other) observer.',
            ),
        ),

        # Add the missing is_half_day field
        migrations.AddField(
            model_name='leaverequest',
            name='is_half_day',
            field=models.BooleanField(default=False),
        ),
    ]
