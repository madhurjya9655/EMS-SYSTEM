from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leave", "0002_approver_mapping"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                # DB already has start_date/end_date on many envs — only add blocked_days here.
                migrations.AddField(
                    model_name="leaverequest",
                    name="blocked_days",
                    field=models.FloatField(
                        default=0.0,
                        help_text="How many calendar days are blocked by this leave (IST).",
                    ),
                ),
                migrations.AddIndex(
                    model_name="leaverequest",
                    index=models.Index(
                        fields=["start_date", "end_date"], name="leave_leavereq_sd_ed_idx"
                    ),
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="leaverequest",
                    name="start_date",
                    field=models.DateField(null=True, blank=True, editable=False),
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="end_date",
                    field=models.DateField(null=True, blank=True, editable=False),
                ),
                migrations.AddField(
                    model_name="leaverequest",
                    name="blocked_days",
                    field=models.FloatField(
                        default=0.0,
                        help_text="How many calendar days are blocked by this leave (IST).",
                    ),
                ),
                migrations.AddIndex(
                    model_name="leaverequest",
                    index=models.Index(
                        fields=["start_date", "end_date"], name="leave_leavereq_sd_ed_idx"
                    ),
                ),
            ],
        ),
    ]
