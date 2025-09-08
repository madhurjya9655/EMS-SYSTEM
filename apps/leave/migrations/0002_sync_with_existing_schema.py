from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import apps.leave.models


class Migration(migrations.Migration):

    dependencies = [
        ('leave', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # Only add fields that don't exist yet
        # Since ApproverMapping table already exists, we'll assume it was created
        # We just need to add any missing fields to LeaveRequest
        
        # Add new fields that likely don't exist yet
        migrations.AddField(
            model_name='leaverequest',
            name='reporting_person',
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL,
                related_name='leave_requests_to_approve', to=settings.AUTH_USER_MODEL,
                help_text='Reporting Person (manager) who must approve.'
            ),
        ),
        migrations.AddField(
            model_name='leaverequest',
            name='cc_person',
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL,
                related_name='leave_requests_cc', to=settings.AUTH_USER_MODEL,
                help_text='HR (or other) observer.'
            ),
        ),
    ]