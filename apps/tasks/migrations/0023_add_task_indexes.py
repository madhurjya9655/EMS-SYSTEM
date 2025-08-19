# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\migrations\0023_add_task_indexes.py
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Adds high-value indexes for Checklist, Delegation, and HelpTicket.
    Depends on 0022 to ensure all models/fields already exist.
    """

    dependencies = [
        ("tasks", "0022_alter_delegation_planned_date"),
    ]

    operations = [
        # ------- Checklist indexes -------
        migrations.AddIndex(
            model_name="checklist",
            index=models.Index(
                fields=["status", "planned_date"],
                name="chk_status_planned_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="checklist",
            index=models.Index(
                fields=["assign_to", "planned_date"],
                name="chk_assignto_planned_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="checklist",
            index=models.Index(
                fields=["assign_to", "status"],
                name="chk_assignto_status_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="checklist",
            index=models.Index(
                fields=["priority"],
                name="chk_priority_idx",
            ),
        ),

        # ------- Delegation indexes -------
        migrations.AddIndex(
            model_name="delegation",
            index=models.Index(
                fields=["status", "planned_date"],
                name="dlg_status_planned_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="delegation",
            index=models.Index(
                fields=["assign_to", "planned_date"],
                name="dlg_assignto_planned_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="delegation",
            index=models.Index(
                fields=["priority"],
                name="dlg_priority_idx",
            ),
        ),

        # ------- HelpTicket indexes -------
        migrations.AddIndex(
            model_name="helpticket",
            index=models.Index(
                fields=["status", "planned_date"],
                name="ht_status_planned_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="helpticket",
            index=models.Index(
                fields=["assign_to", "planned_date"],
                name="ht_assignto_planned_idx",
            ),
        ),
    ]
