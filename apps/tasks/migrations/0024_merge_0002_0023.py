# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\migrations\0024_merge_0002_0023.py
from django.db import migrations


class Migration(migrations.Migration):
    """
    Merge the old branch (0002_optimize_database) with the new head (0023_add_task_indexes).
    """

    dependencies = [
        ("tasks", "0002_optimize_database"),
        ("tasks", "0023_add_task_indexes"),
    ]

    operations = []
