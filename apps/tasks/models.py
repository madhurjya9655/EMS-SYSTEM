# apps/tasks/models.py

from datetime import datetime, timedelta
from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.timesince import timesince

from apps.settings.models import Holiday

User = get_user_model()


def _next_workday_date(d):
    """
    Given a date, returns the next date that is not Sunday and not in Holiday.
    """
    while d.weekday() == 6 or Holiday.objects.filter(date=d).exists():
        d += timedelta(days=1)
    return d


def _next_workday_datetime(dt):
    """
    Given a datetime, returns the next datetime that falls on a workday
    (Mon–Sat excluding Holidays).  Preserves the time-of-day.
    """
    # strip down to date + time
    d = dt.date()
    t = dt.time()
    # bump date forward until valid
    d = _next_workday_date(d)
    new_dt = datetime.combine(d, t)
    # if original was timezone-aware, re-apply timezone
    if timezone.is_aware(dt):
        new_dt = timezone.make_aware(new_dt, timezone.get_current_timezone())
    return new_dt


class Checklist(models.Model):
    assign_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='checklists_assigned'
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='checklists'
    )
    planned_date = models.DateTimeField()
    STATUS_CHOICES = [('Pending','Pending'), ('Completed','Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    priority = models.CharField(
        max_length=10,
        choices=[('Low','Low'), ('Medium','Medium'), ('High','High')]
    )
    attachment_mandatory = models.BooleanField(default=False)
    mode = models.CharField(
        max_length=10,
        choices=[('Daily','Daily'), ('Weekly','Weekly'),
                 ('Monthly','Monthly'), ('Yearly','Yearly')]
    )
    frequency = models.PositiveIntegerField(default=1)
    time_per_task_minutes = models.PositiveIntegerField(
        default=0, help_text='Minutes required each time this checklist runs'
    )
    remind_before_days = models.PositiveIntegerField(default=0)
    message = models.TextField(blank=True)
    media_upload = models.FileField(upload_to='checklist_media/', blank=True, null=True)
    assign_pc = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='pc_checklists'
    )
    group_name = models.CharField(max_length=100, blank=True)
    notify_to = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='notify_checklists'
    )
    auditor = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='audit_checklists'
    )
    set_reminder = models.BooleanField(default=False)
    reminder_mode = models.CharField(
        max_length=10,
        choices=[('Daily','Daily'), ('Weekly','Weekly'),
                 ('Monthly','Monthly'), ('Yearly','Yearly')],
        blank=True
    )
    reminder_frequency = models.PositiveIntegerField(default=1, blank=True)
    reminder_before_days = models.PositiveIntegerField(default=0, blank=True)
    reminder_starting_time = models.TimeField(blank=True, null=True)
    checklist_auto_close = models.BooleanField(default=False)
    checklist_auto_close_days = models.PositiveIntegerField(default=0, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        # if scheduled on Sunday or Holiday, bump to next workday
        if self.planned_date:
            self.planned_date = _next_workday_datetime(self.planned_date)
        super().save(*args, **kwargs)

    @property
    def delay(self):
        if self.completed_at:
            return timesince(self.planned_date, self.completed_at)
        return timesince(self.planned_date)

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


class Delegation(models.Model):
    assign_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='delegations_assigned'
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='delegations'
    )
    planned_date = models.DateField()
    priority = models.CharField(
        max_length=10,
        choices=[('Low','Low'), ('Medium','Medium'), ('High','High')]
    )
    attachment_mandatory = models.BooleanField(default=False)
    audio_recording = models.FileField(upload_to='delegation_audio/', blank=True, null=True)
    time_per_task_minutes = models.PositiveIntegerField(
        default=0,
        verbose_name="Time per Task (minutes)",
        help_text="Enter how many minutes this delegation takes"
    )
    created_at = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        # if scheduled on Sunday or Holiday, bump to next workday
        if self.planned_date:
            self.planned_date = _next_workday_date(self.planned_date)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


class BulkUpload(models.Model):
    FORM_CHOICES = [('checklist','Checklist'), ('delegation','Delegation')]
    form_type = models.CharField(max_length=20, choices=FORM_CHOICES)
    csv_file = models.FileField(upload_to='bulk_uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_form_type_display()} upload @ {self.uploaded_at:%Y-%m-%d}"


class FMS(models.Model):
    assign_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='fms_assigned'
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='fms_tasks'
    )
    planned_date = models.DateField()
    STATUS_CHOICES = [('Pending','Pending'), ('Completed','Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    delay = models.IntegerField(default=0)
    doer_notes = models.TextField(blank=True, null=True)
    priority = models.CharField(
        max_length=10,
        choices=[('Low','Low'), ('Medium','Medium'), ('High','High')]
    )
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


class HelpTicket(models.Model):
    assign_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='help_tickets_assigned'
    )
    title = models.CharField(max_length=200)
    description = models.TextField()
    assign_to = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='help_tickets'
    )
    planned_date = models.DateTimeField()
    STATUS_CHOICES = [('Open','Open'), ('In Progress','In Progress'), ('Closed','Closed')]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Open')
    estimated_minutes = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.title} → {self.assign_to}"
