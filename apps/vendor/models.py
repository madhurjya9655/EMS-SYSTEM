#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\vendor\models.py
from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()


class Vendor(models.Model):
    VENDOR_TYPE_CHOICES = [
        ('supplier', 'Supplier'),
        ('contractor', 'Contractor'),
        ('service', 'Service Provider'),
        ('logistics', 'Logistics'),
        ('office_expenses', 'Office Expenses'),
        ('other', 'Other'),
    ]

    name = models.CharField(max_length=200)
    type = models.CharField(max_length=50, choices=VENDOR_TYPE_CHOICES)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class VendorPaymentRequest(models.Model):
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('submitted', 'Submitted'),
        ('finance_approved', 'Finance Approved'),
        ('rejected', 'Rejected'),
        ('final_approved', 'Final Approved'),
    ]

    BILL_TYPE_CHOICES = [
        ('gst', 'GST'),
        ('non_gst', 'Non-GST'),
    ]

    request_id = models.CharField(max_length=20, unique=True, blank=True)

    # Proper Vendor Master link.
    # We keep null=True for old records and deletion safety.
    vendor = models.ForeignKey(
        Vendor,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='payment_requests',
    )

    # Kept only for backward compatibility with old records.
    # Do not show this field in the new request form.
    vendor_name_manual = models.CharField(max_length=200, blank=True)

    # Snapshot of Vendor.type at the time of request.
    # User should not manually select this in payment request form.
    vendor_type = models.CharField(
        max_length=50,
        choices=Vendor.VENDOR_TYPE_CHOICES,
        blank=True,
    )

    invoice_date = models.DateField()
    invoice_number = models.CharField(max_length=100)

    base_amount = models.DecimalField(max_digits=12, decimal_places=2)
    gst_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        editable=False,
        default=0,
    )

    bill_type = models.CharField(max_length=10, choices=BILL_TYPE_CHOICES)
    description = models.TextField()

    # Existing invoice attachment.
    attachment = models.FileField(
        upload_to='vendor_payments/invoices/%Y/%m/',
        blank=True,
        null=True,
    )

    # New bank details attachment.
    bank_attachment = models.FileField(
        upload_to='vendor_payments/bank_details/%Y/%m/',
        blank=True,
        null=True,
    )

    # New written bank details.
    bank_details_text = models.TextField(
        blank=True,
        help_text='Account Holder Name, Bank Name, Account Number, IFSC',
    )

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')

    created_by = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name='vendor_requests_created',
    )

    finance_approved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='vendor_requests_finance_approved',
    )

    final_approved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='vendor_requests_final_approved',
    )

    remarks = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def save(self, *args, **kwargs):
        # Always calculate total on backend.
        self.total_amount = (self.base_amount or 0) + (self.gst_amount or 0)

        # ERP safety rule:
        # Vendor type must come from Vendor Master only.
        if self.vendor_id and self.vendor:
            self.vendor_type = self.vendor.type
            self.vendor_name_manual = ''

        super().save(*args, **kwargs)

        # Generate request ID after first save because pk is needed.
        if not self.request_id:
            self.request_id = f'Vendor-{self.pk:03d}'
            VendorPaymentRequest.objects.filter(pk=self.pk).update(
                request_id=self.request_id
            )

    def __str__(self):
        return self.request_id or f'VPR-{self.pk}'

    @property
    def vendor_display_name(self):
        if self.vendor_id and self.vendor:
            return self.vendor.name
        return self.vendor_name_manual or '—'

    @property
    def vendor_type_display_safe(self):
        if self.vendor_id and self.vendor:
            return self.vendor.get_type_display()
        if self.vendor_type:
            return self.get_vendor_type_display()
        return '—'

    def get_status_badge(self):
        mapping = {
            'draft': 'draft',
            'submitted': 'pending',
            'finance_approved': 'in-progress',
            'rejected': 'rejected',
            'final_approved': 'completed',
        }
        return mapping.get(self.status, 'draft')


class VendorApprovalConfig(models.Model):
    # System users.
    finance_users = models.ManyToManyField(
        User,
        blank=True,
        related_name='vendor_finance_approver',
    )

    senior_authority = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='vendor_senior_approver',
    )

    mumbai_accounts = models.ManyToManyField(
        User,
        blank=True,
        related_name='vendor_mumbai_accounts',
    )

    # Manual external emails.
    finance_manual_emails = models.TextField(
        blank=True,
        help_text='Comma-separated external email addresses for finance approvers not in the system.',
    )

    mumbai_manual_emails = models.TextField(
        blank=True,
        help_text='Comma-separated external email addresses for Mumbai accounts not in the system.',
    )

    # CC always goes to these.
    cc_emails = models.TextField(
        blank=True,
        help_text='Comma-separated email addresses always CC\'d on final approval emails.',
    )

    class Meta:
        verbose_name = 'Vendor Approval Config'

    def __str__(self):
        return 'Vendor Approval Config'

    @classmethod
    def get_config(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    def get_finance_email_list(self):
        emails = list(
            self.finance_users.filter(is_active=True)
            .values_list('email', flat=True)
        )

        for e in self.finance_manual_emails.split(','):
            e = e.strip()
            if e and e not in emails:
                emails.append(e)

        return [e for e in emails if e]

    def get_mumbai_email_list(self):
        emails = list(
            self.mumbai_accounts.filter(is_active=True)
            .values_list('email', flat=True)
        )

        for e in self.mumbai_manual_emails.split(','):
            e = e.strip()
            if e and e not in emails:
                emails.append(e)

        return [e for e in emails if e]

    def get_cc_email_list(self):
        return [e.strip() for e in self.cc_emails.split(',') if e.strip()]