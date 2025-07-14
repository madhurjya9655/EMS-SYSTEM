from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class WeeklyCommitment(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='weekly_commitments')
    week_start = models.DateField(help_text="Monday of the week")
    checklist = models.PositiveIntegerField(default=0)
    checklist_desc = models.TextField(blank=True, default='')
    delegation = models.PositiveIntegerField(default=0)
    delegation_desc = models.TextField(blank=True, default='')
    fms = models.PositiveIntegerField(default=0)
    fms_desc = models.TextField(blank=True, default='')
    audit = models.PositiveIntegerField(default=0)
    audit_desc = models.TextField(blank=True, default='')

    class Meta:
        unique_together = ('user','week_start')
        ordering = ['-week_start']

    def __str__(self):
        return f'{self.user.username} {self.week_start}'

