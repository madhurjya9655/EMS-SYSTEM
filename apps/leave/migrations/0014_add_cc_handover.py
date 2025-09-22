# Generated migration for LeaveCC and LeaveHandover models

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('leave', '0013_fix_missing_approver_id'),  # Updated to your latest migration
    ]

    operations = [
        migrations.CreateModel(
            name='LeaveCC',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('leave_request', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='cc_recipients', to='leave.leaverequest')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'unique_together': {('leave_request', 'user')},
            },
        ),
        migrations.CreateModel(
            name='LeaveHandover',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('original_task_id', models.PositiveIntegerField()),
                ('task_type', models.CharField(choices=[('checklist', 'Checklist'), ('delegation', 'Delegation'), ('help_ticket', 'Help Ticket')], max_length=20)),
                ('task_name', models.CharField(max_length=200)),
                ('handover_message', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('is_active', models.BooleanField(default=True)),
                ('leave_request', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='handovers', to='leave.leaverequest')),
                ('new_assignee', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='handovers_received', to=settings.AUTH_USER_MODEL)),
                ('original_assignee', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='handovers_given', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'unique_together': {('leave_request', 'original_task_id', 'task_type')},
            },
        ),
        migrations.AddIndex(
            model_name='leavecc',
            index=models.Index(fields=['leave_request'], name='leave_leavecc_leave_r_f4b0e1_idx'),
        ),
        migrations.AddIndex(
            model_name='leavehandover',
            index=models.Index(fields=['leave_request'], name='leave_leaveh_leave_r_a8c9f2_idx'),
        ),
        migrations.AddIndex(
            model_name='leavehandover',
            index=models.Index(fields=['new_assignee', 'is_active'], name='leave_leaveh_new_ass_d5e1a3_idx'),
        ),
        migrations.AddIndex(
            model_name='leavehandover',
            index=models.Index(fields=['original_assignee', 'is_active'], name='leave_leaveh_origina_7f9b2c_idx'),
        ),
    ]