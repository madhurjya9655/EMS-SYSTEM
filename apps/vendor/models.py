# apps/vendor/models.py
from __future__ import annotations

from decimal import Decimal

from django.db import models
from django.db.models import Sum
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
    # Kept nullable for old records and deletion safety.
    vendor = models.ForeignKey(
        Vendor,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="payment_requests",
    )

    # Kept only for backward compatibility with old records.
    # New requests should always use vendor FK from Vendor Master.
    vendor_name_manual = models.CharField(max_length=200, blank=True)

    # Snapshot of Vendor.type at the time of request.
    # User should not manually select this in payment request form.
    vendor_type = models.CharField(
        max_length=50,
        choices=Vendor.VENDOR_TYPE_CHOICES,
        blank=True,
    )

    # ---------------------------------------------------------------------
    # LEGACY SINGLE-INVOICE FIELDS
    # ---------------------------------------------------------------------
    # These fields existed in the old design where:
    # 1 VendorPaymentRequest = 1 Invoice
    #
    # They are intentionally kept so existing approved/history records,
    # old Google Sheet sync references, and old templates do not crash.
    #
    # New records must store invoices in VendorPaymentInvoice child rows.
    # ---------------------------------------------------------------------
    invoice_date = models.DateField(null=True, blank=True)
    invoice_number = models.CharField(max_length=100, blank=True)

    base_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
    )

    gst_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        null=True,
        blank=True,
    )

    # Legacy single-invoice total.
    # For new requests, use grand_total.
    total_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        editable=False,
        default=0,
    )

    bill_type = models.CharField(
        max_length=10,
        choices=BILL_TYPE_CHOICES,
        blank=True,
    )

    description = models.TextField(blank=True)

    # Legacy invoice attachment.
    # For new requests, use VendorPaymentInvoice.invoice_attachment.
    attachment = models.FileField(
        upload_to="vendor_payments/invoices/%Y/%m/",
        blank=True,
        null=True,
    )

    # ---------------------------------------------------------------------
    # NEW REQUEST-LEVEL TOTAL
    # ---------------------------------------------------------------------
    # This is the total of all child invoices.
    # Example:
    # INV001 = 10000
    # INV002 = 20000
    # INV003 = 30000
    # grand_total = 60000
    # ---------------------------------------------------------------------
    grand_total = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        editable=False,
        db_index=True,
    )

    # Copy of cancelled cheque / bank proof attachment.
    # Bank details belong to the parent request, not each invoice.
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
            models.Index(fields=["grand_total"]),
        ]

    def clean(self):
        super().clean()

        # Legacy field validation.
        # These fields may be blank for new multi-invoice requests.
        if self.base_amount is not None and self.base_amount < Decimal("0"):
            raise ValidationError({"base_amount": "Base amount cannot be negative."})

        if self.gst_amount is not None and self.gst_amount < Decimal("0"):
            raise ValidationError({"gst_amount": "GST amount cannot be negative."})

        if self.total_amount is not None and self.total_amount < Decimal("0"):
            raise ValidationError({"total_amount": "Total amount cannot be negative."})

        if self.grand_total is not None and self.grand_total < Decimal("0"):
            raise ValidationError({"grand_total": "Grand total cannot be negative."})

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
        # Keep old single-invoice total calculation working.
        # New request total is calculated from child invoices using recalculate_grand_total().
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
    def invoice_count(self) -> int:
        """
        Number of child invoice rows.

        Legacy fallback:
        If an old request has no child rows but has invoice_number on parent,
        treat it as one invoice.
        """
        if self.pk and hasattr(self, "invoices"):
            count = self.invoices.count()
            if count:
                return count

        return 1 if self.invoice_number else 0

    @property
    def payment_total(self):
        """
        Safe display total.

        New requests use grand_total.
        Old records can fall back to total_amount.
        """
        if self.grand_total:
            return self.grand_total
        return self.total_amount or Decimal("0")

    @property
    def is_paid(self) -> bool:
        return self.status == self.Status.PAID

    @property
    def is_final(self) -> bool:
        return self.status in {
            self.Status.REJECTED,
            self.Status.PAID,
        }

    @property
    def invoices_locked(self) -> bool:
        """
        Lock invoice editing after approval begins.

        Current rule:
        - Draft and Rejected can be edited/resubmitted.
        - Submitted/Finance Approved/Final Approved/Paid are locked.

        If business wants editing while Submitted, remove SUBMITTED from
        VendorPaymentInvoice.locked_statuses below.
        """
        return self.status not in {
            self.Status.DRAFT,
            self.Status.REJECTED,
        }

    def recalculate_grand_total(self, save: bool = True):
        """
        Recalculate grand_total from child invoices.

        This should be called after invoice create/update/delete.
        """
        total = Decimal("0")

        if self.pk and hasattr(self, "invoices"):
            total = (
                self.invoices.aggregate(total=Sum("total_amount")).get("total")
                or Decimal("0")
            )

        # Legacy fallback for old single-invoice records.
        if total == Decimal("0") and self.total_amount:
            total = self.total_amount

        self.grand_total = total

        if save and self.pk:
            VendorPaymentRequest.objects.filter(pk=self.pk).update(
                grand_total=self.grand_total,
                updated_at=timezone.now(),
            )

        return self.grand_total

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

        self.recalculate_grand_total(save=False)

        self.save(
            update_fields=[
                "status",
                "paid_by",
                "paid_at",
                "payment_reference",
                "remarks",
                "total_amount",
                "grand_total",
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


class VendorPaymentInvoice(models.Model):
    """
    Child invoice table.

    Business rule:
    One VendorPaymentRequest can contain many VendorPaymentInvoice rows.

    Example:
    VendorPaymentRequest: Vendor-001 / ABC Logistics / grand_total 60000

    Child rows:
    - INV001 / 10000
    - INV002 / 20000
    - INV003 / 30000
    """

    class BillType(models.TextChoices):
        GST = "gst", "GST"
        NON_GST = "non_gst", "Non-GST"

    payment_request = models.ForeignKey(
        VendorPaymentRequest,
        on_delete=models.CASCADE,
        related_name="invoices",
    )

    invoice_date = models.DateField()
    invoice_number = models.CharField(max_length=100)

    bill_type = models.CharField(
        max_length=10,
        choices=BillType.choices,
    )

    base_amount = models.DecimalField(max_digits=12, decimal_places=2)
    gst_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    total_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        editable=False,
        default=0,
    )

    description = models.TextField(blank=True)

    invoice_attachment = models.FileField(
        upload_to="vendor_payments/invoices/%Y/%m/",
        blank=True,
        null=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["invoice_date", "id"]
        indexes = [
            models.Index(fields=["payment_request"]),
            models.Index(fields=["invoice_number"]),
            models.Index(fields=["invoice_date"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["payment_request", "invoice_number"],
                name="unique_invoice_per_vendor_payment_request",
            )
        ]

    def clean(self):
        super().clean()

        locked_statuses = {
            VendorPaymentRequest.Status.SUBMITTED,
            VendorPaymentRequest.Status.FINANCE_APPROVED,
            VendorPaymentRequest.Status.FINAL_APPROVED,
            VendorPaymentRequest.Status.PAID,
        }

        if self.payment_request_id and self.payment_request.status in locked_statuses:
            raise ValidationError(
                "Invoices cannot be changed after the payment request is submitted/approved."
            )

        if self.base_amount is not None and self.base_amount < Decimal("0"):
            raise ValidationError({"base_amount": "Base amount cannot be negative."})

        if self.gst_amount is not None and self.gst_amount < Decimal("0"):
            raise ValidationError({"gst_amount": "GST amount cannot be negative."})

        if self.bill_type == self.BillType.NON_GST and self.gst_amount:
            # Keep this as validation instead of silently changing GST.
            # It prevents accidental GST amount on Non-GST bills.
            raise ValidationError(
                {"gst_amount": "GST amount must be zero for Non-GST bill type."}
            )

    def save(self, *args, **kwargs):
        self.total_amount = (self.base_amount or Decimal("0")) + (
            self.gst_amount or Decimal("0")
        )

        super().save(*args, **kwargs)

        if self.payment_request_id:
            self.payment_request.recalculate_grand_total(save=True)

    def delete(self, *args, **kwargs):
        payment_request = self.payment_request
        result = super().delete(*args, **kwargs)

        if payment_request and payment_request.pk:
            payment_request.recalculate_grand_total(save=True)

        return result

    def __str__(self):
        request_id = (
            self.payment_request.request_id
            if self.payment_request_id and self.payment_request
            else "Vendor Payment"
        )
        return f"{request_id} - {self.invoice_number}"


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