from django.db import models
from django.conf import settings

class PettyCashRequest(models.Model):
    requester = models.ForeignKey(settings.AUTH_USER_MODEL,on_delete=models.CASCADE)
    reason = models.TextField()
    amount = models.DecimalField(max_digits=10,decimal_places=2)
    urgency = models.CharField(max_length=10,choices=(('Normal','Normal'),('Urgent','Urgent')))
    status = models.CharField(max_length=10,choices=(('Pending','Pending'),('Approved','Approved'),('Rejected','Rejected')),default='Pending')
    manager_comment = models.TextField(blank=True,null=True)
    finance_comment = models.TextField(blank=True,null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
