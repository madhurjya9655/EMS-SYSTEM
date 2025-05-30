from django.db import models
from django.conf import settings
from django.utils import timezone

class Reimbursement(models.Model):
    STATUS_CHOICES = [
        ('PM','Pending Manager'),
        ('PF','Pending Finance'),
        ('A','Approved'),
        ('R','Rejected'),
    ]
    CATEGORY_CHOICES = [
        ('travel','Travel'),
        ('meal','Meal'),
        ('office','Office Supplies'),
        ('other','Other'),
    ]
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    bill = models.FileField(upload_to='bills/')
    submitted_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(max_length=2, choices=STATUS_CHOICES, default='PM')
    manager_comment = models.TextField(blank=True, null=True)
    finance_comment = models.TextField(blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
