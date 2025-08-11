from django.db import models
from django.conf import settings

class Customer(models.Model):
    name = models.CharField(max_length=255)
    assigned_to = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name="customers")

    def __str__(self):
        return self.name

class SalesKPI(models.Model):
    METRIC_CHOICES = [
        ('sales', 'Sales'),
        ('collection', 'Collection'),
        ('calls', 'Calls'),
        ('visits', 'Visits'),
        ('enquiries', 'Enquiries'),
        ('nbd', 'NBD'),
    ]
    PERIOD_CHOICES = [
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
    ]
    MONTH_CHOICES = [
        ('January', 'January'),
        ('February', 'February'),
        ('March', 'March'),
        ('April', 'April'),
        ('May', 'May'),
        ('June', 'June'),
        ('July', 'July'),
        ('August', 'August'),
        ('September', 'September'),
        ('October', 'October'),
        ('November', 'November'),
        ('December', 'December'),
    ]
    WEEK_CHOICES = [
        ('Week-1', 'Week-1'),
        ('Week-2', 'Week-2'),
        ('Week-3', 'Week-3'),
        ('Week-4', 'Week-4'),
        ('Week-5', 'Week-5'),
    ]

    # Excel/Sheet-aligned fields
    month = models.CharField(max_length=16, choices=MONTH_CHOICES, blank=True)
    week = models.CharField(max_length=16, choices=WEEK_CHOICES, blank=True)
    location = models.CharField(max_length=255, blank=True)
    kam = models.CharField(max_length=255, blank=True)
    wire_rod = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    wr_actual = models.DecimalField("WR Actual", max_digits=12, decimal_places=2, default=0)
    round_bar = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_plan = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    # Main fields
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    customer = models.ForeignKey(Customer, on_delete=models.SET_NULL, null=True, blank=True)
    metric = models.CharField(max_length=20, choices=METRIC_CHOICES)
    period_type = models.CharField(max_length=10, choices=PERIOD_CHOICES)
    period_start = models.DateField()
    period_end = models.DateField()
    target = models.DecimalField(max_digits=12, decimal_places=2)
    actual = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.customer} - {self.month} {self.week}"
