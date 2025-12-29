from django.db import models
from django.conf import settings


class Customer(models.Model):
    # --- Minimum existing fields (kept) ---
    name = models.CharField(max_length=255)
    assigned_to = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name="customers", blank=True)

    # --- Fields required for Customer_Master sheet (all optional to avoid breaking anything) ---
    kam_name = models.CharField(max_length=255, blank=True)          # Primary KAM name (denormalized for quick sync)
    address = models.TextField(blank=True)
    email = models.EmailField(blank=True)
    mobile_no = models.CharField(max_length=50, blank=True)
    person_name = models.CharField(max_length=255, blank=True)
    pincode = models.CharField(max_length=20, blank=True)
    type = models.CharField(max_length=100, blank=True)
    gst_number = models.CharField(max_length=50, blank=True)
    credit_limit = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    agreed_credit_period = models.PositiveIntegerField(null=True, blank=True)  # days
    total_exposure = models.DecimalField(max_digits=16, decimal_places=2, null=True, blank=True)
    overdues = models.DecimalField(max_digits=16, decimal_places=2, null=True, blank=True)
    nbd_flag = models.BooleanField(default=False)

    location = models.CharField(max_length=255, blank=True)  # used also by visits/calls

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

    # Excel/Sheet-aligned fields (kept as-is for your existing pages)
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


# --- NEW: Sales_Data sheet mapping (live upsert on create/update) ---
class SalesInvoice(models.Model):
    """
    Represents a sales line that mirrors to Google Sheet 'Sales_Data'.
    """
    kam_name = models.CharField(max_length=255)  # denormalized for sheet
    invoice_date = models.DateField()
    quantity_mt = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    revenue_inr_with_gst = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    # Optional links
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    customer = models.ForeignKey(Customer, on_delete=models.SET_NULL, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["kam_name", "invoice_date"]),
        ]

    def __str__(self):
        return f"{self.kam_name} • {self.invoice_date} • {self.quantity_mt} MT"


# --- NEW: Leads_Data sheet mapping (live upsert on create/update) ---
class Lead(models.Model):
    """
    Mirrors to Google Sheet 'Leads_Data'.
    """
    date_of_enquiry = models.DateField()
    kam_name = models.CharField(max_length=255)
    customer_name = models.CharField(max_length=255)
    quantity = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    status = models.CharField(max_length=100, blank=True)
    remarks = models.TextField(blank=True)
    grade = models.CharField(max_length=100, blank=True)
    size = models.CharField(max_length=100, blank=True)

    # Derived fields persisted for quick reporting
    month_text = models.CharField(max_length=20, blank=True)  # e.g., "December"
    week_text = models.CharField(max_length=20, blank=True)   # e.g., "Week-1"

    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    customer = models.ForeignKey(Customer, on_delete=models.SET_NULL, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Lead {self.customer_name} • {self.date_of_enquiry}"


# --- NEW: Targets_Plan storage (read-only from sheet; managers edit in sheet) ---
class TargetsPlan(models.Model):
    kam_name = models.CharField(max_length=255)
    week = models.CharField(max_length=20)  # "Week-1" .. "Week-5"

    sales_target_mt = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    calls_target = models.PositiveIntegerField(default=24)     # fixed default per spec
    visits_target = models.PositiveIntegerField(default=6)     # fixed default per spec
    leads_target_mt = models.DecimalField(max_digits=12, decimal_places=2, default=250)
    nbd_target_monthly = models.PositiveIntegerField(default=0)  # manager override monthly, stored per week for convenience

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("kam_name", "week")
        indexes = [
            models.Index(fields=["kam_name", "week"]),
        ]

    def __str__(self):
        return f"{self.kam_name} – {self.week}"
