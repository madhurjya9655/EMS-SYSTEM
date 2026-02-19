# File: E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\kam\migrations\0002_target_setting.py
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("kam", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="TargetSetting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("from_date", models.DateField(db_index=True)),
                ("to_date", models.DateField(db_index=True)),
                ("sales_target_mt", models.DecimalField(decimal_places=3, default=0, max_digits=12)),
                ("leads_target_mt", models.DecimalField(decimal_places=3, default=0, max_digits=12)),
                ("collections_target_amount", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("calls_target", models.IntegerField(default=0)),
                ("fixed_sales_mt", models.DecimalField(decimal_places=3, default=0, max_digits=12)),
                ("fixed_leads_mt", models.DecimalField(decimal_places=3, default=0, max_digits=12)),
                ("fixed_collections_amount", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("fixed_calls", models.IntegerField(default=0)),
                ("locked_at", models.DateTimeField(blank=True, null=True)),
                (
                    "kam",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="target_settings",
                        to="auth.user",
                    ),
                ),
                (
                    "manager",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="target_settings_created",
                        to="auth.user",
                    ),
                ),
            ],
            options={
                "unique_together": {("kam", "from_date", "to_date")},
                "indexes": [
                    models.Index(fields=["kam", "from_date", "to_date"], name="kam_targets_kam_id_1d8b43_idx"),
                    models.Index(fields=["from_date", "to_date"], name="kam_targets_from_da_7d1772_idx"),
                ],
            },
        ),
    ]
