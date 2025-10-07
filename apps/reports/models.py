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
        verbose_name = "Weekly Commitment"
        verbose_name_plural = "Weekly Commitments"
        constraints = [
            models.UniqueConstraint(
                fields=["user", "week_start"],
                name="uniq_weekly_commitment_user_week",
            )
        ]
        indexes = [
            models.Index(fields=["user", "week_start"], name="idx_wc_user_week"),
        ]

    def __str__(self):
        return f"{self.user} - {self.week_start}"


class WeeklyScore(models.Model):
    """
    Stores the last-week completion percentage for a user and marks that we mailed them.
    Used to avoid duplicate 'Congratulations' emails for the same (user, week).
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="weekly_scores")
    week_start = models.DateField()  # Monday of the week just evaluated
    score = models.DecimalField(max_digits=5, decimal_places=2)  # e.g., 92.50
    mailed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Weekly Score"
        verbose_name_plural = "Weekly Scores"
        constraints = [
            models.UniqueConstraint(
                fields=["user", "week_start"],
                name="uniq_weekly_score_user_week",
            )
        ]
        indexes = [
            models.Index(fields=["user", "week_start"], name="idx_ws_user_week"),
            models.Index(fields=["week_start"], name="idx_ws_week"),
        ]

    def __str__(self):
        return f"{self.user} – {self.week_start} – {self.score}%"
