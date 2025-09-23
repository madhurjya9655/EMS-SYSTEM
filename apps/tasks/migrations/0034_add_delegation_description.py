from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ("tasks", "0034_add_delegation_description.py"), 
    ]

    operations = [
        # If the column exists and is NULL in some rows, make them safe
        migrations.RunSQL(
            "UPDATE tasks_delegation SET description = '' WHERE description IS NULL;",
            reverse_sql=migrations.RunSQL.noop,
        ),
        # Ensure the field exists with a default and is non-nullable in the model
        migrations.AddField(
            model_name="delegation",
            name="description",
            field=models.TextField(default="", blank=True),
        ),
    ]
