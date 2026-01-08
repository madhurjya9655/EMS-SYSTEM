# Generated manually – safe to apply
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0005_profile_cc_override_emails_profile_employee_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="photo",
            field=models.ImageField(
                upload_to="profiles/%Y/%m/",
                null=True,
                blank=True,
                verbose_name="Profile Photo",
            ),
        ),
    ]
