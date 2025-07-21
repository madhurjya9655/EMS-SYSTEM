from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class WeeklyCommitment(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    week_start = models.DateField()
    checklist = models.IntegerField(default=0)
    checklist_desc = models.TextField(blank=True)
    checklist_ontime = models.IntegerField(default=0)
    checklist_ontime_desc = models.TextField(blank=True)
    delegation = models.IntegerField(default=0)
    delegation_desc = models.TextField(blank=True)
    delegation_ontime = models.IntegerField(default=0)
    delegation_ontime_desc = models.TextField(blank=True)
    fms = models.IntegerField(default=0)
    fms_desc = models.TextField(blank=True)
    audit = models.IntegerField(default=0)
    audit_desc = models.TextField(blank=True)

    class Meta:
        unique_together = ('user', 'week_start')
        verbose_name = 'Weekly Commitment'
        verbose_name_plural = 'Weekly Commitments'

    def __str__(self):
        return f"{self.user} - {self.week_start}"

class Checklist(models.Model):
    # Add any fields as needed for your use case
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='checklist_tasks')
    planned_date = models.DateTimeField(db_index=True)
    status = models.CharField(max_length=20)
    completed_at = models.DateTimeField(null=True, blank=True)
    mode = models.CharField(max_length=20, default='Daily')
    actual_duration_minutes = models.IntegerField(default=0)
    # Add other fields as needed, e.g., title, assign_by, etc.

class Delegation(models.Model):
    # Add any fields as needed for your use case
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='delegation_tasks')
    planned_date = models.DateTimeField(db_index=True)
    status = models.CharField(max_length=20)
    completed_at = models.DateTimeField(null=True, blank=True)
    actual_duration_minutes = models.IntegerField(default=0)
    # Add other fields as needed, e.g., title, assign_by, etc.
