from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Brings production DB in sync with current models:

      • Add LeaveHandover.message (TextField, blank=True)
      • Add LeaveRequest.cc_users (ManyToMany to AUTH_USER)

    Safe on production (Render). If your local DB already has these
    (because you created them manually), you can --fake locally.
    """

    dependencies = [
        # If your last leave migration file name is different, update it here
        ("leave", "0016_alter_approvermapping_cc_person_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # 1) LeaveHandover.message
        migrations.AddField(
            model_name="leavehandover",
            name="message",
            field=models.TextField(blank=True, default=""),
            preserve_default=False,
        ),

        # 2) LeaveRequest.cc_users (implicit M2M table: leave_leaverequest_cc_users)
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
