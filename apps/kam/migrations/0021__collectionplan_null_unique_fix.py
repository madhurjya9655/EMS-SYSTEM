# FILE: apps/kam/migrations/XXXX_collectionplan_null_unique_fix.py
# Run after generating: python manage.py makemigrations kam --empty --name collectionplan_null_unique_fix

from django.db import migrations


class Migration(migrations.Migration):
    """
    PostgreSQL treats NULL != NULL in UNIQUE constraints.
    Django's unique_together on (period_type, period_id, customer) allows
    multiple NULL rows for the same customer, creating duplicates.

    This migration adds a partial unique index that covers the NULL case.
    Two separate partial indexes:
    1. When period_type IS NOT NULL: enforce uniqueness on (period_type, period_id, customer)
       [Django's unique_together already handles this]
    2. When period_type IS NULL (date-range rows): enforce uniqueness on (from_date, to_date, customer)
    """

    dependencies = [
        ("kam", "0001_initial"),  # replace with your actual last migration name
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                CREATE UNIQUE INDEX IF NOT EXISTS
                    kam_collectionplan_daterange_unique
                ON kam_collectionplan (from_date, to_date, customer_id)
                WHERE period_type IS NULL AND from_date IS NOT NULL AND to_date IS NOT NULL;
            """,
            reverse_sql="""
                DROP INDEX IF EXISTS kam_collectionplan_daterange_unique;
            """,
        ),
    ]