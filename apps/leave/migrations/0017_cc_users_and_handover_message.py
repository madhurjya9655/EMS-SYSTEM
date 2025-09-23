# apps/leave/migrations/0017_cc_users_and_handover_message.py
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Brings production DB in sync with current models:

      • Add LeaveHandover.message (TextField, blank=True)
      • Add LeaveRequest.cc_users (ManyToMany to AUTH_USER)

    On environments where these already exist (e.g. your local box),
    you can mark this migration as faked:  manage.py migrate leave 0017 --fake
    """

    dependencies = [
        ("leave", "0016_alter_approvermapping_cc_person_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # 1) LeaveHandover.message
        migrations.AddField(
            model_name="leavehandover",
            name="message",
            field=models.TextField(blank=True, default=""),
            preserve_default=False,  # keep model clean (no runtime default)
        ),

        # 2) LeaveRequest.cc_users (creates the implicit M2M 'leave_leaverequest_cc_users' table)
        migrations.AddField(
            model_name="leaverequest",
            name="cc_users",
            field=models.ManyToManyField(
                to=settings.AUTH_USER_MODEL,
                related_name="leave_requests_cc_user",
                blank=True,
                help_text="Additional CC recipients selected by the employee.",
            ),
        ),
    ]
