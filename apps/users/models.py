from django.db import models
from django.contrib.auth.models import User

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    phone = models.CharField(max_length=10, unique=True)
    role = models.CharField(max_length=50, choices=[
        ('Admin', 'Admin'),
        ('Manager', 'Manager'),
        ('HR', 'HR'),
        ('Finance', 'Finance'),
        ('Sales Executive', 'Sales Executive'),
        ('Employee', 'Employee'),
        ('EA', 'EA'),
        ('CEO', 'CEO'),
    ])
    branch = models.CharField(max_length=100, blank=True)
    department = models.CharField(max_length=100, blank=True)
    team_leader = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='team_members'
    )
    permissions = models.JSONField(default=list, blank=True)

    def __str__(self):
        return f"{self.user.username} – {self.role}"
