from django.db import models
from django.conf import settings
from django.utils import timezone

class LeaveType(models.Model):
    name = models.CharField(max_length=50)
    default_days = models.PositiveIntegerField()

class LeaveRequest(models.Model):
    STATUS_CHOICES = [
        ('PM','Pending Manager'),
        ('PH','Pending HR'),
        ('A','Approved'),
        ('R','Rejected'),
    ]
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    leave_type = models.ForeignKey(LeaveType, on_delete=models.PROTECT)
    start_date = models.DateField()
    end_date = models.DateField()
    reason = models.TextField()
    status = models.CharField(max_length=2, choices=STATUS_CHOICES, default='PM')
    manager_comment = models.TextField(blank=True, null=True)
    hr_comment = models.TextField(blank=True, null=True)
    applied_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    @property
    def days(self):
        return (self.end_date - self.start_date).days + 1
