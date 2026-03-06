from django.db import migrations, models
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ("kam", "0015_customer_credit_period_days_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="collectionplan",
            name="actual_amount",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Actual amount collected against this plan entry.",
                max_digits=14,
                null=True,
                validators=[django.core.validators.MinValueValidator(0)],
            ),
        ),
        migrations.AddField(
            model_name="collectionplan",
            name="collection_date",
            field=models.DateField(
                blank=True,
                help_text="Date the actual collection was recorded.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="collectionplan",
            name="collection_status",
            field=models.CharField(
                choices=[
                    ("OPEN", "Open"),
                    ("PARTIAL", "Partial"),
                    ("COLLECTED", "Collected"),
                ],
                db_index=True,
                default="OPEN",
                max_length=12,
            ),
        ),
        migrations.AddField(
            model_name="collectionplan",
            name="collection_reference",
            field=models.CharField(
                blank=True,
                help_text="Cheque / UTR / reference for the actual collection.",
                max_length=64,
                null=True,
            ),
        ),
    ]