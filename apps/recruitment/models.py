from django.db import models
from django.conf import settings

class Employee(models.Model):
    first_name = models.CharField(max_length=50)
    last_name = models.CharField(max_length=50)
    email = models.EmailField(unique=True)
    phone = models.CharField(max_length=15, blank=True)
    department = models.CharField(max_length=100)
    date_joined = models.DateField(auto_now_add=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.first_name} {self.last_name}"

class Candidate(models.Model):
    name = models.CharField(max_length=100)
    email = models.EmailField()
    resume = models.FileField(upload_to='resumes/')
    applied_on = models.DateField(auto_now_add=True)
    status = models.CharField(
        max_length=15,
        choices=[
            ('New', 'New'),
            ('Shortlisted', 'Shortlisted'),
            ('Interviewed', 'Interviewed'),
            ('Selected', 'Selected'),
            ('Offered', 'Offered'),
        ],
        default='New',
    )

class InterviewSchedule(models.Model):
    candidate = models.ForeignKey(Candidate, on_delete=models.CASCADE)
    scheduled_at = models.DateTimeField()
    interviewer = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name='interviews')
    location = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

class InterviewFeedback(models.Model):
    interview = models.ForeignKey(InterviewSchedule, on_delete=models.CASCADE)
    reviewer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    feedback = models.TextField()
    rating = models.IntegerField()
    submitted_at = models.DateTimeField(auto_now_add=True)
