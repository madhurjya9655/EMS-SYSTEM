# Generated migration for CC Configuration
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('leave', '0003_leavehandover_leave_leave_new_ass_716663_idx'),
    ]

    operations = [
        migrations.CreateModel(
            name='CCConfiguration',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_active', models.BooleanField(default=True, help_text='Whether this user is available for CC selection')),
                ('display_name', models.CharField(blank=True, help_text='Optional display name override', max_length=200)),
                ('department', models.CharField(blank=True, help_text='Department or role for grouping', max_length=100)),
                ('sort_order', models.PositiveIntegerField(default=0, help_text='Display order (lower numbers first)')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('user', models.ForeignKey(help_text='User who can be selected as CC recipient', on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'CC Configuration',
                'verbose_name_plural': 'CC Configurations',
                'ordering': ['sort_order', 'department', 'user__first_name', 'user__last_name'],
            },
        ),
        migrations.AlterUniqueTogether(
            name='ccconfiguration',
            unique_together={('user',)},
        ),
    ]