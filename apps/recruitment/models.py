# apps/recruitment/models.py
from __future__ import annotations

from django.conf import settings
from django.db import models


class Employee(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, null=True, blank=True,
        related_name="employee_record",
    )
    reporting_officer = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True, blank=True,
        related_name="employee_master_reports",
        help_text="Reporting manager. Admin-managed single source of truth for leave routing.",
    )
    first_name = models.CharField(max_length=50, blank=True, default="")
    last_name = models.CharField(max_length=50, blank=True, default="")
    email = models.EmailField(unique=True)
    phone = models.CharField(max_length=15, blank=True, default="")
    department = models.CharField(max_length=100, blank=True, default="")
    date_joined = models.DateField(auto_now_add=True)
    is_active = models.BooleanField(default=True, help_text="Mirror of User.is_active. Do NOT set manually.")

    class Meta:
        ordering = ["last_name", "first_name"]
        indexes = [models.Index(fields=["email"]), models.Index(fields=["is_active"]), models.Index(fields=["reporting_officer"])]

    def __str__(self):
        return f"{self.first_name} {self.last_name} <{self.email}>"

    def sync_from_user(self):
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
    status = models.CharField(max_length=15, choices=[("New","New"),("Shortlisted","Shortlisted"),("Interviewed","Interviewed"),("Selected","Selected"),("Offered","Offered")], default="New")
    def __str__(self): return f"{self.name} ({self.status})"


class InterviewSchedule(models.Model):
    candidate = models.ForeignKey(Candidate, on_delete=models.CASCADE)
    scheduled_at = models.DateTimeField()
    interviewer = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name="interviews")
    location = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


class InterviewFeedback(models.Model):
    interview = models.ForeignKey(InterviewSchedule, on_delete=models.CASCADE)
    reviewer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    feedback = models.TextField()
    rating = models.IntegerField()
    submitted_at = models.DateTimeField(auto_now_add=True)