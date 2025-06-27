# apps/tasks/migrations/0011_add_time_per_task_to_delegation.py
from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0010_checklist_assign_pc_checklist_attachment_mandatory_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='delegation',
            name='time_per_task_minutes',
            field=models.PositiveIntegerField(
                default=0,
                verbose_name='Time per Task (minutes)',
                help_text='Enter how many minutes this delegation takes'
            ),
        ),
    ]
