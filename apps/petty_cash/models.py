from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class PettyCashRequest(models.Model):
    URGENCY_CHOICES = [
        ('Low', 'Low'),
        ('Medium', 'Medium'),
        ('High', 'High'),
    ]
    STATUS_CHOICES = [
        ('Pending', 'Pending'),
        ('Manager Approved', 'Manager Approved'),
        ('Manager Rejected', 'Manager Rejected'),
        ('Finance Approved', 'Finance Approved'),
        ('Finance Rejected', 'Finance Rejected'),
    ]

    requester         = models.ForeignKey(User, on_delete=models.CASCADE)
    reason            = models.TextField()
    amount            = models.DecimalField(max_digits=10, decimal_places=2)
    urgency           = models.CharField(max_length=10, choices=URGENCY_CHOICES, default='Low')
    status            = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Pending')
    manager_comment   = models.TextField(blank=True, null=True)
    finance_comment   = models.TextField(blank=True, null=True)
    created_at        = models.DateTimeField(auto_now_add=True)
    updated_at        = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.requester.username} – {self.amount} ({self.status})"