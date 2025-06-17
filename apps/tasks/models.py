from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class Checklist(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='checklists_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='checklists')
    planned_date = models.DateTimeField()
    STATUS_CHOICES = [('Pending', 'Pending'), ('Completed', 'Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    priority = models.CharField(max_length=10, choices=[('Low','Low'),('Medium','Medium'),('High','High')])
    attachment_mandatory = models.BooleanField(default=False)
    mode = models.CharField(max_length=10, choices=[('Daily','Daily'),('Weekly','Weekly'),('Monthly','Monthly'),('Yearly','Yearly')])
    frequency = models.PositiveIntegerField(default=1)
    remind_before_days = models.PositiveIntegerField(default=0)
    message = models.TextField(blank=True)
    media_upload = models.FileField(upload_to='checklist_media/', blank=True, null=True)
    assign_pc = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='pc_checklists')
    group_name = models.CharField(max_length=100, blank=True)
    notify_to = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='notify_checklists')
    auditor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='audit_checklists')
    set_reminder = models.BooleanField(default=False)
    reminder_mode = models.CharField(max_length=10, choices=[('Daily','Daily'),('Weekly','Weekly'),('Monthly','Monthly'),('Yearly','Yearly')], blank=True)
    reminder_frequency = models.PositiveIntegerField(default=1, blank=True)
    reminder_before_days = models.PositiveIntegerField(default=0, blank=True)
    reminder_starting_time = models.TimeField(blank=True, null=True)
    checklist_auto_close = models.BooleanField(default=False)
    checklist_auto_close_days = models.PositiveIntegerField(default=0, blank=True)
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class Delegation(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='delegations_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='delegations')
    planned_date = models.DateField()
    priority = models.CharField(max_length=10, choices=[('Low','Low'),('Medium','Medium'),('High','High')])
    attachment_mandatory = models.BooleanField(default=False)
    audio_recording = models.FileField(upload_to='delegation_audio/', blank=True, null=True)
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

class BulkUpload(models.Model):
    FORM_CHOICES = [('checklist','Checklist'),('delegation','Delegation')]
    form_type = models.CharField(max_length=20, choices=FORM_CHOICES)
    csv_file = models.FileField(upload_to='bulk_uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

class FMS(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='fms_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='fms_tasks')
    planned_date = models.DateField()
    STATUS_CHOICES = [('Pending','Pending'),('Completed','Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    delay = models.IntegerField(default=0)
    doer_notes = models.TextField(blank=True, null=True)
    priority = models.CharField(max_length=10, choices=[('Low','Low'),('Medium','Medium'),('High','High')])
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class HelpTicket(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='help_tickets_assigned')
    title = models.CharField(max_length=200)
    description = models.TextField()
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='help_tickets')
    planned_date = models.DateTimeField()
    STATUS_CHOICES = [('Open','Open'),('In Progress','In Progress'),('Closed','Closed')]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Open')
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class WeeklyCommitment(models.Model):
    user       = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='reports_weekly_commitments'
    )
    week_start = models.DateField(help_text="Monday of the week")
    checklist  = models.PositiveIntegerField(default=0)
    delegation = models.PositiveIntegerField(default=0)
    fms        = models.PositiveIntegerField(default=0)
    audit      = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ('user', 'week_start')
        ordering = ['-week_start']

    def __str__(self):
        return f"{self.user.username} – Week of {self.week_start}"