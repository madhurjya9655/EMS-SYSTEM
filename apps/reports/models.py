from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class WeeklyCommitment(models.Model):
    user        = models.ForeignKey(User, on_delete=models.CASCADE, related_name='weekly_commitments')
    week_start  = models.DateField(help_text="Monday of the week")
    checklist   = models.PositiveIntegerField(default=0)
    delegation  = models.PositiveIntegerField(default=0)
    fms         = models.PositiveIntegerField(default=0)
    audit       = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ('user','week_start')
        ordering = ['-week_start']
