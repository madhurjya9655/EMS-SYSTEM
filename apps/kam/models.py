# FILE: apps/kam/models.py
from __future__ import annotations

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone

User = get_user_model()


class TimeStamped(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Customer(TimeStamped):
    SOURCE_SHEET = "SHEET"
    SOURCE_MANUAL = "MANUAL"
    SOURCE_CHOICES = [
        (SOURCE_SHEET, "Sheet"),
        (SOURCE_MANUAL, "Manual"),
    ]

    code = models.CharField(max_length=64, blank=True, null=True, db_index=True)
    name = models.CharField(max_length=255)
    gst_number = models.CharField(max_length=32, blank=True, null=True, db_index=True)

    contact_person = models.CharField(max_length=128, blank=True, null=True)
    address = models.TextField(blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    mobile = models.CharField(max_length=32, blank=True, null=True)
    pincode = models.CharField(max_length=12, blank=True, null=True)
    type = models.CharField(max_length=64, blank=True, null=True)
    is_nbd = models.BooleanField(default=False)

    credit_limit = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    agreed_credit_period_days = models.IntegerField(default=0)

    credit_period_days = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    total_exposure = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    current_credit_limit = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    kam = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL,
        related_name="kam_customers",
        help_text="Explicit owner (KAM). Used by Plan Visit strict scoping.",
    )
    source = models.CharField(max_length=10, choices=SOURCE_CHOICES, default=SOURCE_SHEET, db_index=True)
    created_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="customers_created",
    )
    synced_identifier = models.CharField(max_length=128, blank=True, null=True, db_index=True)
    primary_kam = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_customers",
    )

    @property
    def phone(self) -> str:
        return self.mobile or ""

    def sync_owner_fields(self) -> None:
        if self.kam_id and not self.primary_kam_id:
            self.primary_kam_id = self.kam_id
        elif self.primary_kam_id and not self.kam_id:
            self.kam_id = self.primary_kam_id

    def assign_kam(self, kam_user: User | None) -> bool:
        if not kam_user or not getattr(kam_user, "id", None):
            return False
        changed = False
        if self.kam_id != kam_user.id:
            self.kam = kam_user
            changed = True
        if self.primary_kam_id != kam_user.id:
            self.primary_kam = kam_user
            changed = True
        return changed

    def clean(self):
        self.sync_owner_fields()
        if (self.source or "").upper() == self.SOURCE_MANUAL and not self.created_by_id:
            raise ValidationError({"created_by": "created_by is required for MANUAL customers."})
        super().clean()

    def save(self, *args, **kwargs):
        self.sync_owner_fields()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    class Meta:
        permissions = [("access_kam_module", "Can access KAM module")]


class KAMAssignment(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    kam = models.ForeignKey(User, on_delete=models.CASCADE)
    active_from = models.DateField()
    active_to = models.DateField(null=True, blank=True)

    class Meta:
        indexes = [models.Index(fields=["customer", "kam", "active_from"])]


class InvoiceFact(TimeStamped):
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True, null=True, blank=True)
    invoice_date = models.DateField(db_index=True)
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT, null=True, blank=True)

    grade = models.CharField(max_length=64, blank=True, null=True)
    size = models.CharField(max_length=64, blank=True, null=True)

    qty_mt = models.DecimalField(
        max_digits=12,
        decimal_places=3,
        default=0,
        validators=[MinValueValidator(0)],
    )

    invoice_value = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        validators=[MinValueValidator(0)],
    )

    revenue_gst = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        validators=[MinValueValidator(0)],
    )

    invoice_no = models.CharField(max_length=64, blank=True, null=True, db_index=True)
    source_tab = models.CharField(max_length=32, blank=True, null=True, db_index=True)

    # Important for Sales (F) dashboard correctness.
    # Dashboard Sales must count only source_status = "Order Converted".
    source_status = models.CharField(
        max_length=80,
        blank=True,
        null=True,
        db_index=True,
        help_text="Raw status from source sheet, e.g. Order Converted.",
    )

    source_timestamp = models.DateTimeField(
        blank=True,
        null=True,
        db_index=True,
        help_text="Raw timestamp parsed from source sheet.",
    )

    raw_buyer_name = models.CharField(max_length=255, blank=True, null=True)
    rate_mt = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    def save(self, *args, **kwargs):
        if self.invoice_value and not self.revenue_gst:
            self.revenue_gst = self.invoice_value
        elif self.revenue_gst and not self.invoice_value:
            self.invoice_value = self.revenue_gst
        super().save(*args, **kwargs)

    class Meta:
        indexes = [
            models.Index(fields=["invoice_date"]),
            models.Index(fields=["kam", "invoice_date"]),
            models.Index(fields=["customer", "invoice_date"]),
            models.Index(fields=["source_tab"]),
            models.Index(fields=["source_tab", "source_status"]),
            models.Index(fields=["source_tab", "source_status", "invoice_date"]),
            models.Index(fields=["kam", "source_tab", "source_status", "invoice_date"]),
        ]


class LeadFact(TimeStamped):
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True, null=True, blank=True)
    doe = models.DateField(db_index=True, null=True, blank=True)
    kam = models.ForeignKey(User, on_delete=models.PROTECT, null=True, blank=True)
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT, null=True, blank=True)
    qty_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0, validators=[MinValueValidator(0)])
    status = models.CharField(
        max_length=32,
        choices=[("OPEN", "Open"), ("NEGOTIATION", "Negotiation"), ("WON", "Won"), ("LOST", "Lost")],
        default="OPEN",
    )
    grade = models.CharField(max_length=64, blank=True, null=True)
    size = models.CharField(max_length=64, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)
    source_tab = models.CharField(max_length=32, blank=True, null=True, db_index=True)
    enquiry_no = models.CharField(max_length=64, blank=True, null=True, db_index=True)
    revenue_mt = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["doe"]),
            models.Index(fields=["kam", "doe"]),
            models.Index(fields=["source_tab"]),
            models.Index(fields=["enquiry_no"]),
        ]


class OverdueSnapshot(TimeStamped):
    snapshot_date = models.DateField(db_index=True)
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL, related_name="overdue_snapshots")
    exposure = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    overdue = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    overdue_amt = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    ageing_0_30 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_31_60 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_61_90 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_90_plus = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    def save(self, *args, **kwargs):
        if self.overdue_amt is not None and not self.overdue:
            self.overdue = self.overdue_amt
        elif self.overdue and self.overdue_amt is None:
            self.overdue_amt = self.overdue
        super().save(*args, **kwargs)

    class Meta:
        unique_together = ("snapshot_date", "customer")


class KamManagerMapping(TimeStamped):
    kam = models.ForeignKey(User, on_delete=models.CASCADE, related_name="kam_manager_mappings")
    manager = models.ForeignKey(User, on_delete=models.CASCADE, related_name="managed_kam_mappings")
    assigned_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name="kam_manager_assignments")
    assigned_at = models.DateTimeField(auto_now_add=True)
    active = models.BooleanField(default=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["kam", "active"]),
            models.Index(fields=["manager", "active"]),
        ]

    def clean(self):
        if self.kam_id and self.manager_id and self.kam_id == self.manager_id:
            raise ValidationError({"manager": "Manager cannot be the same as KAM."})
        super().clean()

    def __str__(self):
        return f"{self.kam} → {self.manager} ({'ACTIVE' if self.active else 'INACTIVE'})"


class ManagerTargetSetting(TimeStamped):
    MODE_VALUE = "VALUE"
    MODE_PERCENT = "PERCENT"
    COLLECTION_MODE_CHOICES = [
        (MODE_VALUE, "Manual Value"),
        (MODE_PERCENT, "% of Overdue"),
    ]

    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="manager_targets")
    from_date = models.DateField(db_index=True)
    to_date = models.DateField(db_index=True)
    sales_target = models.DecimalField(max_digits=14, decimal_places=3, default=0, validators=[MinValueValidator(0)])
    collection_target = models.DecimalField(max_digits=14, decimal_places=2, default=0, validators=[MinValueValidator(0)])
    collection_mode = models.CharField(max_length=10, choices=COLLECTION_MODE_CHOICES, default=MODE_VALUE)
    collection_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True, validators=[MinValueValidator(0)])
    calls_target = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    visit_target = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    leads_target = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    nbd_target = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    is_fixed = models.BooleanField(default=False)
    fixed_expiry_date = models.DateField(null=True, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name="manager_targets_created")
    is_locked = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=["kam", "from_date", "to_date"]),
            models.Index(fields=["from_date", "to_date"]),
            models.Index(fields=["is_fixed", "is_locked"]),
        ]

    def __str__(self):
        return f"ManagerTargetSetting#{self.id} {self.kam} {self.from_date}..{self.to_date}"

    @classmethod
    def unlock_expired(cls):
        today = timezone.localdate()
        cls.objects.filter(
            is_fixed=True, is_locked=True,
            fixed_expiry_date__isnull=False, fixed_expiry_date__lt=today,
        ).update(is_locked=False)


class TargetSetting(TimeStamped):
    manager = models.ForeignKey(User, on_delete=models.PROTECT, related_name="target_settings_created")
    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="target_settings")
    from_date = models.DateField(db_index=True)
    to_date = models.DateField(db_index=True)
    sales_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    leads_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    collections_target_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    calls_target = models.IntegerField(default=0)
    fixed_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    fixed_leads_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    fixed_collections_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    fixed_calls = models.IntegerField(default=0)
    locked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["kam", "from_date", "to_date"]),
            models.Index(fields=["from_date", "to_date"]),
        ]
        unique_together = ("kam", "from_date", "to_date")

    def __str__(self):
        return f"TargetSetting#{self.id} {self.kam} {self.from_date}..{self.to_date}"


class TargetHeader(TimeStamped):
    PERIOD_WEEK = "WEEK"
    PERIOD_MONTH = "MONTH"
    PERIOD_QUARTER = "QUARTER"
    PERIOD_YEAR = "YEAR"

    period_type = models.CharField(
        max_length=8,
        choices=[
            (PERIOD_WEEK, "Week"),
            (PERIOD_MONTH, "Month"),
            (PERIOD_QUARTER, "Quarter"),
            (PERIOD_YEAR, "Year"),
        ],
    )
    period_id = models.CharField(max_length=10)
    manager = models.ForeignKey(User, on_delete=models.PROTECT, related_name="target_headers")
    locked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("period_type", "period_id")


class TargetLine(TimeStamped):
    header = models.ForeignKey(TargetHeader, on_delete=models.CASCADE, related_name="lines")
    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="kam_targets")
    sales_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    visits_target = models.IntegerField(default=6)
    calls_target = models.IntegerField(default=24)
    leads_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    nbd_target_monthly = models.IntegerField(default=0)
    collections_plan_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    class Meta:
        unique_together = ("header", "kam")


class CollectionPlan(TimeStamped):
    """
    OVERDUE-DRIVEN Collection Tracking Entry.

    DATA FLOW:
      Google Sheet Overdues tab
        → _sync_overdues_to_collection_plan()
        → updates overdue_amount only
        → never overwrites actual_amount / collection_date / payment_details / utr_number

    KAM fills:
      actual_amount
      collection_date
      payment_details
      utr_number

    pending_amount = overdue_amount - actual_amount
    """

    STATUS_OPEN = "OPEN"
    STATUS_PARTIAL = "PARTIAL"
    STATUS_COLLECTED = "COLLECTED"

    STATUS_CHOICES = [
        (STATUS_OPEN, "Open"),
        (STATUS_PARTIAL, "Partial"),
        (STATUS_COLLECTED, "Collected"),
    ]

    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)

    kam = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name="collection_plans",
        help_text="KAM resolved from Overdues tab. Required for role filtering.",
    )

    overdue_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Synced from Google Sheet Overdues tab. Never manually edited.",
    )

    last_synced_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When overdue_amount was last synced from sheet.",
    )

    actual_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="Actual amount collected by KAM.",
    )

    collection_date = models.DateField(
        null=True,
        blank=True,
        help_text="Date actual collection was received.",
    )

    payment_details = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Payment mode/details: NEFT / RTGS / Cheque / Cash / UPI etc.",
    )

    utr_number = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        help_text="UTR / Cheque number.",
    )

    collection_status = models.CharField(
        max_length=12,
        choices=STATUS_CHOICES,
        default=STATUS_OPEN,
        db_index=True,
    )

    # Legacy fields retained for backward compatibility.
    period_type = models.CharField(
        max_length=8,
        choices=[
            ("WEEK", "Week"),
            ("MONTH", "Month"),
            ("QUARTER", "Quarter"),
            ("YEAR", "Year"),
        ],
        blank=True,
        null=True,
    )
    period_id = models.CharField(max_length=10, blank=True, null=True)
    from_date = models.DateField(blank=True, null=True)
    to_date = models.DateField(blank=True, null=True)

    planned_amount = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        validators=[MinValueValidator(0)],
        default=0,
        help_text="[DEPRECATED] Mirrors overdue_amount for backward compatibility.",
    )

    notes = models.TextField(blank=True, null=True)
    collection_reference = models.CharField(max_length=64, blank=True, null=True)

    @property
    def pending_amount(self):
        overdue = self.overdue_amount or Decimal("0")
        actual = self.actual_amount or Decimal("0")
        pending = overdue - actual
        return pending if pending > 0 else Decimal("0")

    @property
    def shortfall(self):
        return self.pending_amount

    @property
    def achievement_pct(self):
        overdue = self.overdue_amount or Decimal("0")
        actual = self.actual_amount or Decimal("0")
        if overdue <= 0:
            return Decimal("0")
        return (actual / overdue) * Decimal("100")

    def save(self, *args, **kwargs):
        # Backward compatibility: old templates/reports may still read planned_amount.
        self.planned_amount = self.overdue_amount or Decimal("0")

        # Keep legacy reference and new UTR aligned.
        if self.utr_number and not self.collection_reference:
            self.collection_reference = self.utr_number
        elif self.collection_reference and not self.utr_number:
            self.utr_number = self.collection_reference

        overdue = self.overdue_amount or Decimal("0")
        actual = self.actual_amount or Decimal("0")

        if overdue > 0:
            if actual >= overdue:
                self.collection_status = self.STATUS_COLLECTED
            elif actual > 0:
                self.collection_status = self.STATUS_PARTIAL
            else:
                self.collection_status = self.STATUS_OPEN
        else:
            self.collection_status = self.STATUS_OPEN

        super().save(*args, **kwargs)

    def __str__(self):
        return (
            f"CollectionPlan#{self.id} {self.customer.name} "
            f"overdue=₹{self.overdue_amount} actual=₹{self.actual_amount or 0}"
        )

    class Meta:
        indexes = [
            models.Index(fields=["customer", "kam"]),
            models.Index(fields=["kam"]),
            models.Index(fields=["collection_status"]),
            models.Index(fields=["overdue_amount"]),
            models.Index(fields=["last_synced_at"]),
            models.Index(fields=["from_date", "to_date", "customer"]),
        ]

    def save(self, *args, **kwargs):
        if self.actual_amount is not None and self.planned_amount:
            if self.actual_amount >= self.planned_amount:
                self.collection_status = self.STATUS_COLLECTED
            elif self.actual_amount > 0:
                self.collection_status = self.STATUS_PARTIAL
            else:
                self.collection_status = self.STATUS_OPEN
        super().save(*args, **kwargs)

    @property
    def shortfall(self):
        planned = self.planned_amount or 0
        actual = self.actual_amount or 0
        diff = planned - actual
        return diff if diff > 0 else 0

    def __str__(self):
        return f"CollectionPlan#{self.id} {self.customer} {self.planned_amount}"


class VisitBatch(TimeStamped):
    DRAFT = "DRAFT"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    CAT_VENDOR = "VENDOR"
    CAT_CUSTOMER = "CUSTOMER"
    CAT_SUPPLIER = "SUPPLIER"
    CAT_WAREHOUSE = "WAREHOUSE"

    VISIT_CATEGORY_CHOICES = [
        (CAT_VENDOR, "Vendor Visit"),
        (CAT_CUSTOMER, "Customer Visit"),
        (CAT_SUPPLIER, "Supplier Visit"),
        (CAT_WAREHOUSE, "Warehouse Visit"),
    ]
    APPROVAL_STATUS_CHOICES = [
        (DRAFT, "Draft"),
        (PENDING_APPROVAL, "Pending Approval"),
        (PENDING, "Pending (Legacy)"),
        (APPROVED, "Approved"),
        (REJECTED, "Rejected"),
    ]

    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="visit_batches")
    from_date = models.DateField()
    to_date = models.DateField()
    visit_category = models.CharField(max_length=16, choices=VISIT_CATEGORY_CHOICES)
    purpose = models.TextField(blank=True, null=True)
    approval_status = models.CharField(
        max_length=20, default=PENDING_APPROVAL, choices=APPROVAL_STATUS_CHOICES, db_index=True,
    )
    submitted_at = models.DateTimeField(null=True, blank=True)
    approved_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="approved_batches",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    rejected_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="rejected_batches",
    )
    rejected_at = models.DateTimeField(null=True, blank=True)
    rejection_reason = models.TextField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=["kam", "from_date", "to_date"]),
            models.Index(fields=["approval_status"]),
        ]

    def __str__(self):
        return f"Batch#{self.id} {self.kam} {self.from_date}..{self.to_date} {self.visit_category}"


class VisitPlan(TimeStamped):
    """
    Represents a single planned visit.

    STATUS STATE MACHINE:
    ┌─────────────────────────────────────────────────────────┐
    │  draft ──► pending_manager_approval ──► approved        │
    │                        │                                │
    │                        └──────────────► rejected        │
    │                                            │            │
    │  (rejected → editable → resubmit)          │            │
    │  approved ──► completed (post-visit done)  │            │
    └─────────────────────────────────────────────────────────┘

    LOCK RULE:
    - draft:                    EDITABLE by KAM
    - pending_manager_approval: LOCKED (not editable)
    - approved:                 LOCKED (post-visit allowed)
    - rejected:                 EDITABLE again (KAM can fix & resubmit)
    - completed:                LOCKED (visit fully done)

    APPROVAL AUDIT:
    - submitted_at: when KAM clicked "Submit to Manager"
    - approved_at / approved_by: manager approval details
    - rejected_at / rejected_by: manager rejection details
    - rejection_reason: required text when manager rejects
    """

    PLANNED = "PLANNED"
    UNPLANNED = "UNPLANNED"

    # Status constants — aligned with VisitBatch for consistency
    DRAFT = "DRAFT"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"

    CAT_VENDOR = VisitBatch.CAT_VENDOR
    CAT_CUSTOMER = VisitBatch.CAT_CUSTOMER
    CAT_SUPPLIER = VisitBatch.CAT_SUPPLIER
    CAT_WAREHOUSE = VisitBatch.CAT_WAREHOUSE

    APPROVAL_STATUS_CHOICES = [
        (DRAFT, "Draft"),
        (PENDING_APPROVAL, "Pending Manager Approval"),
        (PENDING, "Pending (Legacy)"),
        (APPROVED, "Approved"),
        (REJECTED, "Rejected"),
        (COMPLETED, "Completed"),
    ]

    batch = models.ForeignKey(
        VisitBatch, null=True, blank=True, on_delete=models.CASCADE, related_name="lines",
    )
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT, null=True, blank=True)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    employee = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="employee_visits",
        help_text="Employee who applied this visit (universal flow). "
                  "Null = old KAM-batch visit.",
    )
    manager = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="managed_visits",
        help_text="Auto-assigned approving manager (from profile.team_leader).",
    )
    visit_date = models.DateField()
    visit_date_to = models.DateField(null=True, blank=True)
    visit_type = models.CharField(
        max_length=12,
        choices=[(PLANNED, "Planned"), (UNPLANNED, "Unplanned")],
        default=PLANNED,
    )
    visit_category = models.CharField(
        max_length=16,
        choices=[
            (CAT_VENDOR, "Vendor Visit"),
            (CAT_CUSTOMER, "Customer Visit"),
            (CAT_SUPPLIER, "Supplier Visit"),
            (CAT_WAREHOUSE, "Warehouse Visit"),
        ],
        default=CAT_CUSTOMER,
    )
    counterparty_name = models.CharField(max_length=255, blank=True, null=True)
    purpose = models.TextField(blank=True, null=True)
    expected_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    expected_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    location = models.TextField(blank=True, null=True)

    # ── Approval workflow fields ─────────────────────────────────────────
    approval_status = models.CharField(
        max_length=20, default=DRAFT, choices=APPROVAL_STATUS_CHOICES, db_index=True,
    )
    submitted_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when KAM submitted this visit to manager for approval.",
    )
    approved_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL,
        related_name="approved_visits",
        help_text="Manager who approved this visit.",
    )
    approved_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when manager approved.",
    )
    rejected_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL,
        related_name="rejected_visits",
        help_text="Manager who rejected this visit.",
    )
    rejected_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when manager rejected.",
    )
    rejection_reason = models.TextField(
        blank=True, null=True,
        help_text="Reason provided by manager when rejecting. Required on rejection.",
    )

    class Meta:
        indexes = [
            models.Index(fields=["kam", "visit_date"]),
            models.Index(fields=["approval_status"]),
            models.Index(fields=["batch"]),
            models.Index(fields=["visit_category"]),
        ]

    def __str__(self):
        base = self.customer.name if self.customer_id else (self.counterparty_name or "N/A")
        return f"{self.visit_date} • {base} • {self.visit_category}"

    @property
    def is_locked(self) -> bool:
        """Returns True if KAM cannot edit this visit."""
        return self.approval_status in (self.PENDING_APPROVAL, self.APPROVED, self.COMPLETED)

    @property
    def can_submit(self) -> bool:
        """Returns True if KAM can submit this visit to manager."""
        return self.approval_status in (self.DRAFT, self.REJECTED)


class VisitActual(TimeStamped):
    plan = models.OneToOneField(VisitPlan, on_delete=models.CASCADE, related_name="actual")
    actual_datetime = models.DateTimeField(null=True, blank=True, default=timezone.now)
    meeting_notes = models.TextField(blank=True, null=True)
    summary = models.TextField(blank=True, null=True)
    successful = models.BooleanField(default=False)
    not_success_reason = models.CharField(
        max_length=32, blank=True, null=True,
        choices=[
            ("PRICE", "Price"),
            ("MILL_NOT_APPROVED", "Mill not approved"),
            ("QUALITY", "Quality"),
            ("CREDIT_TERMS", "Credit / payment terms"),
            ("OTHER", "Other"),
        ],
    )
    actual_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    actual_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    next_action = models.CharField(max_length=255, blank=True, null=True)
    next_action_date = models.DateField(null=True, blank=True)
    reminder_cc_manager = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        if (self.meeting_notes or "").strip() and not (self.summary or "").strip():
            self.summary = self.meeting_notes
        elif (self.summary or "").strip() and not (self.meeting_notes or "").strip():
            self.meeting_notes = self.summary
        super().save(*args, **kwargs)


class CallLog(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    call_datetime = models.DateTimeField(default=timezone.now)
    duration_minutes = models.IntegerField(default=0)
    call_type = models.CharField(max_length=32, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    summary = models.TextField(blank=True, null=True)
    outcome = models.CharField(max_length=64, blank=True, null=True)

    def save(self, *args, **kwargs):
        if (self.notes or "").strip() and not (self.summary or "").strip():
            self.summary = self.notes
        elif (self.summary or "").strip() and not (self.notes or "").strip():
            self.notes = self.summary
        super().save(*args, **kwargs)

    class Meta:
        indexes = [models.Index(fields=["kam", "call_datetime"])]


class CollectionTxn(TimeStamped):
    # ── Source constants ───────────────────────────────────────────────────
    SOURCE_SHEET = "GOOGLE_SHEET"
    SOURCE_ERP   = "ERP"
    SOURCE_CHOICES = [
        (SOURCE_SHEET, "Google Sheet (Historical)"),
        (SOURCE_ERP,   "ERP System Entry"),
    ]
    # ───────────────────────────────────────────────────────────────────────
 
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    txn_datetime = models.DateTimeField(default=timezone.now)
    amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)])
    mode = models.CharField(max_length=32, blank=True, null=True)
    reference = models.CharField(max_length=64, blank=True, null=True)
    reference_no = models.CharField(max_length=64, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
 
    # ── NEW FIELDS (Issue 2 fix) ───────────────────────────────────────────
    source = models.CharField(
        max_length=16,
        choices=SOURCE_CHOICES,
        default=SOURCE_ERP,       # all existing entries are ERP by default
        db_index=True,
        help_text="GOOGLE_SHEET = synced from sheet; ERP = entered in system",
    )
    row_uuid = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
        null=True,
        blank=True,
        help_text="Deterministic hash for idempotent sheet sync. NULL for ERP entries.",
    )
    # ───────────────────────────────────────────────────────────────────────
 
    def save(self, *args, **kwargs):
        if (self.reference_no or "").strip() and not (self.reference or "").strip():
            self.reference = self.reference_no
        elif (self.reference or "").strip() and not (self.reference_no or "").strip():
            self.reference_no = self.reference
        super().save(*args, **kwargs)
 
    class Meta:
        indexes = [
            models.Index(fields=["kam", "txn_datetime"]),
            models.Index(fields=["source"]),               # NEW: fast filter by source
            models.Index(fields=["source", "txn_datetime"]),  # NEW: dashboard aggregate
        ]


class KpiSnapshotDaily(TimeStamped):
    snapshot_date = models.DateField(db_index=True)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    sales_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    collection_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    visits_planned = models.IntegerField(default=0)
    visits_actual = models.IntegerField(default=0)
    calls = models.IntegerField(default=0)
    leads_total_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    leads_won_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    nbd_won_count = models.IntegerField(default=0)
    overdue = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    exposure = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    credit_limit = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    class Meta:
        unique_together = ("snapshot_date", "kam")


class VisitApprovalAudit(TimeStamped):
    ACTION_APPROVE = "APPROVE"
    ACTION_REJECT = "REJECT"
    ACTION_DELETE = "DELETE"
    ACTION_SUBMIT = "SUBMIT"

    plan = models.ForeignKey(
        VisitPlan, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True,
    )
    batch = models.ForeignKey(
        VisitBatch, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True,
    )
    actor = models.ForeignKey(User, on_delete=models.PROTECT, related_name="visit_approval_actions")
    action = models.CharField(
        max_length=16,
        choices=[
            (ACTION_APPROVE, "Approve"),
            (ACTION_REJECT, "Reject"),
            (ACTION_DELETE, "Delete"),
            (ACTION_SUBMIT, "Submit"),
        ],
    )
    note = models.CharField(max_length=255, blank=True, null=True)
    actor_ip = models.GenericIPAddressField(blank=True, null=True)

    class Meta:
        indexes = [models.Index(fields=["created_at"])]


class SyncIntent(TimeStamped):
    STATUS_PENDING = "PENDING"
    STATUS_RUNNING = "RUNNING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_ERROR = "ERROR"
    SCOPE_SELF = "SELF"
    SCOPE_TEAM = "TEAM"

    token = models.CharField(max_length=64, unique=True, db_index=True)
    created_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name="kam_sync_intents")
    scope = models.CharField(
        max_length=8, choices=[(SCOPE_SELF, "Self"), (SCOPE_TEAM, "Team")], default=SCOPE_SELF,
    )
    status = models.CharField(
        max_length=10,
        choices=[
            (STATUS_PENDING, "Pending"),
            (STATUS_RUNNING, "Running"),
            (STATUS_SUCCESS, "Success"),
            (STATUS_ERROR, "Error"),
        ],
        default=STATUS_PENDING,
    )
    last_customer_cursor = models.CharField(max_length=128, blank=True, null=True)
    last_invoice_cursor = models.CharField(max_length=128, blank=True, null=True)
    last_lead_cursor = models.CharField(max_length=128, blank=True, null=True)
    last_overdue_cursor = models.CharField(max_length=128, blank=True, null=True)
    cursor_position = models.IntegerField(default=0)
    step_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, null=True)

    class Meta:
        indexes = [models.Index(fields=["status", "created_at"])]