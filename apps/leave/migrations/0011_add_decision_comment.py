# apps/leave/migrations/0011_add_decision_comment.py
from django.db import migrations, models


class Migration(migrations.Migration):

    # Your latest applied migration is 0010_add_decided_at (per showmigrations)
    dependencies = [
        ("leave", "0010_add_decided_at"),
    ]

    operations = [
        migrations.AddField(
            model_name="leaverequest",
            name="decision_comment",
            # Backfill existing rows once with "", then remove default for future saves
            field=models.TextField(blank=True, default=""),
            preserve_default=False,
        ),
    ]
