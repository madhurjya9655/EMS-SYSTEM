# apps/tasks/migrations/0004_merge.py
from django.db import migrations

class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0002_create_helpticket'),
        ('tasks', '0003_checklist_status'),
    ]

    operations = []
