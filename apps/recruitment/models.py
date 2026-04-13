# apps/recruitment/models.py
from __future__ import annotations

from django.conf import settings
from django.db import models


class Employee(models.Model):
    """
    Mirrors an auth User's active status.
    Single source of truth for is_active: User.is_active
    Employee.is_active is a CACHED MIRROR — never set it directly.
    """

    # ── Link to auth User ──────────────────────────────────────────────
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,          # delete Employee when User deleted
        null=True,
        blank=True,
        related_name="employee_record",
        help_text="Linked Django auth user. is_active is synced from here.",
    )

    # ── Identity fields (kept for legacy imports; prefer User fields) ──
    first_name = models.CharField(max_length=50, blank=True, default="")
    last_name = models.CharField(max_length=50, blank=True, default="")
    email = models.EmailField(unique=True)
    phone = models.CharField(max_length=15, blank=True, default="")
    department = models.CharField(max_length=100, blank=True, default="")
    date_joined = models.DateField(auto_now_add=True)

    # ── Status: MIRROR ONLY — synced from User.is_active via signal ───
    is_active = models.BooleanField(
        default=True,
        help_text="Mirror of User.is_active. Do NOT set manually.",
    )

    class Meta:
        ordering = ["last_name", "first_name"]
        indexes = [
            models.Index(fields=["email"]),
            models.Index(fields=["is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.first_name} {self.last_name} <{self.email}>"

    def sync_from_user(self) -> bool:
        """
        Pull is_active from linked User. Returns True if a change was saved.
        Safe to call anytime — no-op if already in sync.
        """
        if not self.user_id:
            return False
        expected = bool(self.user.is_active)
        if self.is_active != expected:
            self.is_active = expected
            self.save(update_fields=["is_active"])
            return True
        return False


class Candidate(models.Model):
    name = models.CharField(max_length=100)
    email = models.EmailField()
    resume = models.FileField(upload_to="resumes/")
    applied_on = models.DateField(auto_now_add=True)
    status = models.CharField(
        max_length=15,
        choices=[
            ("New", "New"),
            ("Shortlisted", "Shortlisted"),
            ("Interviewed", "Interviewed"),
            ("Selected", "Selected"),
            ("Offered", "Offered"),
        ],
        default="New",
    )

    def __str__(self) -> str:
        return f"{self.name} ({self.status})"


class InterviewSchedule(models.Model):
    candidate = models.ForeignKey(Candidate, on_delete=models.CASCADE)
    scheduled_at = models.DateTimeField()
    interviewer = models.ManyToManyField(
        settings.AUTH_USER_MODEL, related_name="interviews"
    )
    location = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


class InterviewFeedback(models.Model):
    interview = models.ForeignKey(InterviewSchedule, on_delete=models.CASCADE)
    reviewer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    feedback = models.TextField()
    rating = models.IntegerField()
    submitted_at = models.DateTimeField(auto_now_add=True)