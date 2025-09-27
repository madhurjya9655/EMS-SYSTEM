# Generated migration to update help text for cc_users field
from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('leave', '0004_add_cc_configuration'),
    ]

    operations = [
        migrations.AlterField(
            model_name='leaverequest',
            name='cc_users',
            field=models.ManyToManyField(blank=True, help_text='Additional CC recipients selected by the employee from admin-configured options.', related_name='leave_requests_cc_user', to=settings.AUTH_USER_MODEL),
        ),
    ]