# apps/tasks/migrations/0002_create_helpticket.py
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):

    initial = False
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('tasks', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='HelpTicket',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=200)),
                ('description', models.TextField()),
                ('planned_date', models.DateTimeField()),
                ('status', models.CharField(choices=[('Open','Open'),('In Progress','In Progress'),('Closed','Closed')], default='Open', max_length=20)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('assign_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='help_tickets_assigned', to=settings.AUTH_USER_MODEL)),
                ('assign_to', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='help_tickets', to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]
