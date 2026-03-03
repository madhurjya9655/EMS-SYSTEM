# FILE: apps/kam/migrations/0002_customer_name_unique.py
# PURPOSE: Add unique constraint on Customer.name to prevent future duplicates.
#
# ⚠️  RUN THE DEDUPLICATION COMMAND FIRST before applying this migration:
#     python manage.py deduplicate_customers --apply
#
# Then apply this migration:
#     python manage.py migrate kam

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        # Change '0001_initial' to the name of the last migration in apps/kam/migrations/
        ("kam", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="customer",
            name="name",
            field=models.CharField(
                max_length=255,
                unique=True,
                help_text="Customer name — must be unique across all records.",
            ),
        ),
    ]