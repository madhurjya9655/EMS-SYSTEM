from django.db import migrations

class Migration(migrations.Migration):
    """
    NO-OP replacement for an unsafe SQL migration that attempted to add
    columns already created in 0001_initial. Keep this file so history stays consistent.
    """
    dependencies = [
        ('leave', '0001_initial'),
    ]
    operations = []