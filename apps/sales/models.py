from django.db import models
from django.conf import settings

class SalesKPI(models.Model):
    METRIC_CHOICES = [
        ('sales','Sales'),
        ('collection','Collection'),
        ('calls','Calls'),
        ('visits','Visits'),
        ('enquiries','Enquiries'),
    ]
    PERIOD_CHOICES = [
        ('weekly','Weekly'),
        ('monthly','Monthly'),
    ]
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    metric = models.CharField(max_length=20, choices=METRIC_CHOICES)
    period_type = models.CharField(max_length=10, choices=PERIOD_CHOICES)
    period_start = models.DateField()
    period_end = models.DateField()
    target = models.DecimalField(max_digits=12, decimal_places=2)
    actual = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
