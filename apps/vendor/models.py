# apps/vendor/models.py
from __future__ import annotations

from decimal import Decimal

from django.db import models
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.utils import timezone

User = get_user_model()


class Vendor(models.Model):
    VENDOR_TYPE_CHOICES = [
        ("supplier", "Supplier"),
        ("contractor", "Contractor"),
        ("service", "Service Provider"),
        ("logistics", "Logistics"),
        ("office_expenses", "Office Expenses"),
        ("other", "Other"),
    ]

    name = models.CharField(max_length=200)
    type = models.CharField(max_length=50, choices=VENDOR_TYPE_CHOICES)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name


class VendorPaymentRequest(models.Model):
    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        SUBMITTED = "submitted", "Submitted"
        FINANCE_APPROVED = "finance_approved", "Finance Approved"
        REJECTED = "rejected", "Rejected"
        FINAL_APPROVED = "final_approved", "Final Approved"
        PAID = "paid", "Paid"

    class BillType(models.TextChoices):
        GST = "gst", "GST"
        NON_GST = "non_gst", "Non-GST"

    # Backward-compatible constants.
    # Existing views/templates using VendorPaymentRequest.STATUS_CHOICES still work.
    STATUS_CHOICES = Status.choices
    BILL_TYPE_CHOICES = BillType.choices

    request_id = models.CharField(max_length=20, unique=True, blank=True)

    # Proper Vendor Master link.
    # We keep null=True for old records and deletion safety.
    vendor = models.ForeignKey(
        Vendor,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="payment_requests",
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

    bill_type = models.CharField(
        max_length=10,
        choices=BILL_TYPE_CHOICES,
    )

    description = models.TextField()

    # Existing invoice attachment.
    attachment = models.FileField(
        upload_to="vendor_payments/invoices/%Y/%m/",
        blank=True,
        null=True,
    )

    # Copy of cancelled cheque / bank proof attachment.
    bank_attachment = models.FileField(
        upload_to="vendor_payments/bank_details/%Y/%m/",
        blank=True,
        null=True,
    )

    # Written bank details.
    bank_details_text = models.TextField(
        blank=True,
        help_text="Account Holder Name, Bank Name, Account Number, IFSC",
    )

    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=Status.DRAFT,
        db_index=True,
    )

    created_by = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name="vendor_requests_created",
    )

    finance_approved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vendor_requests_finance_approved",
    )

    final_approved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vendor_requests_final_approved",
    )

    # Payment tracking.
    # Needed because business requirement says Google Sheet must update when Paid.
    paid_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vendor_requests_paid",
    )
    paid_at = models.DateTimeField(null=True, blank=True)
    payment_reference = models.CharField(max_length=255, blank=True)

    remarks = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["created_by", "status"]),
            models.Index(fields=["vendor", "status"]),
            models.Index(fields=["request_id"]),
        ]

    def clean(self):
        super().clean()

        if self.base_amount is not None and self.base_amount < Decimal("0"):
            raise ValidationError({"base_amount": "Base amount cannot be negative."})

        if self.gst_amount is not None and self.gst_amount < Decimal("0"):
            raise ValidationError({"gst_amount": "GST amount cannot be negative."})

        if self.status == self.Status.PAID:
            if not self.paid_at:
                raise ValidationError(
                    {"paid_at": "Paid timestamp is required when status is Paid."}
                )

            if not (self.payment_reference or "").strip():
                raise ValidationError(
                    {
                        "payment_reference": (
                            "Payment reference is required when status is Paid."
                        )
                    }
                )

    def save(self, *args, **kwargs):
        # Always calculate total on backend.
        self.total_amount = (self.base_amount or Decimal("0")) + (
            self.gst_amount or Decimal("0")
        )

        # ERP safety rule:
        # Vendor type must come from Vendor Master only.
        if self.vendor_id and self.vendor:
            self.vendor_type = self.vendor.type
            self.vendor_name_manual = ""

        # If request is marked paid directly and paid_at was not set,
        # stamp it safely.
        if self.status == self.Status.PAID and not self.paid_at:
            self.paid_at = timezone.now()

        super().save(*args, **kwargs)

        # Generate request ID after first save because pk is needed.
        # Use update() to avoid recursive save.
        if not self.request_id:
            generated = f"Vendor-{self.pk:03d}"
            VendorPaymentRequest.objects.filter(pk=self.pk).update(
                request_id=generated
            )
            self.request_id = generated

    def __str__(self):
        return self.request_id or f"VPR-{self.pk}"

    @property
    def vendor_display_name(self):
        if self.vendor_id and self.vendor:
            return self.vendor.name
        return self.vendor_name_manual or "—"

    @property
    def vendor_type_display_safe(self):
        if self.vendor_id and self.vendor:
            return self.vendor.get_type_display()
        if self.vendor_type:
            return self.get_vendor_type_display()
        return "—"

    @property
    def is_paid(self) -> bool:
        return self.status == self.Status.PAID

    @property
    def is_final(self) -> bool:
        return self.status in {
            self.Status.REJECTED,
            self.Status.PAID,
        }

    def can_mark_paid(self, reference: str = "") -> tuple[bool, str]:
        if self.status == self.Status.PAID:
            return True, "Already paid."

        if self.status != self.Status.FINAL_APPROVED:
            return False, "Vendor payment can be marked Paid only after Final Approval."

        if not (reference or "").strip():
            return False, "Payment reference is required."

        return True, ""

    def mark_paid(self, *, actor=None, reference: str, remarks: str = "") -> None:
        ok, msg = self.can_mark_paid(reference)

        if not ok:
            raise ValidationError(msg)

        if self.status == self.Status.PAID:
            return

        self.status = self.Status.PAID
        self.paid_by = actor if getattr(actor, "pk", None) else None
        self.paid_at = timezone.now()
        self.payment_reference = reference.strip()

        if remarks:
            self.remarks = remarks.strip()

        self.save(
            update_fields=[
                "status",
                "paid_by",
                "paid_at",
                "payment_reference",
                "remarks",
                "total_amount",
                "vendor_type",
                "vendor_name_manual",
                "updated_at",
            ]
        )

    def get_status_badge(self):
        mapping = {
            self.Status.DRAFT: "draft",
            self.Status.SUBMITTED: "pending",
            self.Status.FINANCE_APPROVED: "in-progress",
            self.Status.REJECTED: "rejected",
            self.Status.FINAL_APPROVED: "completed",
            self.Status.PAID: "completed",
        }
        return mapping.get(self.status, "draft")


class VendorApprovalConfig(models.Model):
    # System users.
    finance_users = models.ManyToManyField(
        User,
        blank=True,
        related_name="vendor_finance_approver",
    )

    senior_authority = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vendor_senior_approver",
    )

    mumbai_accounts = models.ManyToManyField(
        User,
        blank=True,
        related_name="vendor_mumbai_accounts",
    )

    # Manual external emails.
    finance_manual_emails = models.TextField(
        blank=True,
        help_text=(
            "Comma-separated external email addresses for finance approvers "
            "not in the system."
        ),
    )

    mumbai_manual_emails = models.TextField(
        blank=True,
        help_text=(
            "Comma-separated external email addresses for Mumbai accounts "
            "not in the system."
        ),
    )

    # CC always goes to these.
    cc_emails = models.TextField(
        blank=True,
        help_text="Comma-separated email addresses always CC'd on final approval emails.",
    )

    class Meta:
        verbose_name = "Vendor Approval Config"

    def __str__(self):
        return "Vendor Approval Config"

    @classmethod
    def get_config(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    def get_finance_email_list(self):
        emails = list(
            self.finance_users.filter(is_active=True).values_list("email", flat=True)
        )

        for email in self.finance_manual_emails.split(","):
            email = email.strip()

            if email and email not in emails:
                emails.append(email)

        return [email for email in emails if email]

    def get_mumbai_email_list(self):
        emails = list(
            self.mumbai_accounts.filter(is_active=True).values_list("email", flat=True)
        )

        for email in self.mumbai_manual_emails.split(","):
            email = email.strip()

            if email and email not in emails:
                emails.append(email)

        return [email for email in emails if email]

    def get_cc_email_list(self):
        return [email.strip() for email in self.cc_emails.split(",") if email.strip()]