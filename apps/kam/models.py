# FILE: apps/kam/models.py
from __future__ import annotations

from django.db import models
from django.contrib.auth import get_user_model
from django.core.validators import MinValueValidator
from django.core.exceptions import ValidationError
from django.utils import timezone

User = get_user_model()


class TimeStamped(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Customer(TimeStamped):
    """
    NOTE:
    - Keep legacy fields intact (sync + existing templates depend on them).
    - Add explicit source + ownership fields required by redesigned Plan Visit.
    - Preserve backward compatibility:
        * primary_kam is legacy owner used widely -> keep.
        * kam is the new explicit owner field per requirements -> keep in sync with primary_kam.
    """

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

    # ---------------------------------------------------------------------
    # Ownership / source (REQUIRED by redesign)
    # ---------------------------------------------------------------------
    kam = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="kam_customers",
        help_text="Explicit owner (KAM). Used by Plan Visit strict scoping.",
    )

    source = models.CharField(
        max_length=10,
        choices=SOURCE_CHOICES,
        default=SOURCE_SHEET,
        db_index=True,
    )

    created_by = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="customers_created",
    )

    synced_identifier = models.CharField(max_length=128, blank=True, null=True, db_index=True)

    # ---------------------------------------------------------------------
    # Legacy owner (kept for compatibility)
    # ---------------------------------------------------------------------
    primary_kam = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_customers"
    )

    @property
    def phone(self) -> str:
        return self.mobile or ""

    def clean(self):
        if self.kam_id and not self.primary_kam_id:
            self.primary_kam_id = self.kam_id
        elif self.primary_kam_id and not self.kam_id:
            self.kam_id = self.primary_kam_id

        if (self.source or "").upper() == self.SOURCE_MANUAL and not self.created_by_id:
            raise ValidationError({"created_by": "created_by is required for MANUAL customers."})

        super().clean()

    def save(self, *args, **kwargs):
        if self.kam_id and not self.primary_kam_id:
            self.primary_kam_id = self.kam_id
        elif self.primary_kam_id and not self.kam_id:
            self.kam_id = self.primary_kam_id
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    class Meta:
        permissions = [
            ("access_kam_module", "Can access KAM module"),
        ]


class KAMAssignment(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    kam = models.ForeignKey(User, on_delete=models.CASCADE)
    active_from = models.DateField()
    active_to = models.DateField(null=True, blank=True)

    class Meta:
        indexes = [models.Index(fields=["customer", "kam", "active_from"])]


class InvoiceFact(TimeStamped):
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True)
    invoice_date = models.DateField()
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)

    grade = models.CharField(max_length=64, blank=True, null=True)
    size = models.CharField(max_length=64, blank=True, null=True)

    qty_mt = models.DecimalField(max_digits=12, decimal_places=3, validators=[MinValueValidator(0)])
    revenue_gst = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)])


class LeadFact(TimeStamped):
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True)
    doe = models.DateField()
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT, null=True, blank=True)

    qty_mt = models.DecimalField(max_digits=12, decimal_places=3, validators=[MinValueValidator(0)])
    status = models.CharField(
        max_length=32,
        choices=[("OPEN", "Open"), ("NEGOTIATION", "Negotiation"), ("WON", "Won"), ("LOST", "Lost")],
    )
    grade = models.CharField(max_length=64, blank=True, null=True)
    size = models.CharField(max_length=64, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)


class OverdueSnapshot(TimeStamped):
    snapshot_date = models.DateField(db_index=True)
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)

    exposure = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    overdue = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    ageing_0_30 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_31_60 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_61_90 = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    ageing_90_plus = models.DecimalField(max_digits=14, decimal_places=2, default=0)

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
    collection_target = models.DecimalField(
        max_digits=14, decimal_places=2, default=0, validators=[MinValueValidator(0)]
    )
    collection_mode = models.CharField(max_length=10, choices=COLLECTION_MODE_CHOICES, default=MODE_VALUE)
    collection_percent = models.DecimalField(
        max_digits=6, decimal_places=2, null=True, blank=True, validators=[MinValueValidator(0)]
    )

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
            is_fixed=True,
            is_locked=True,
            fixed_expiry_date__isnull=False,
            fixed_expiry_date__lt=today,
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
    PERIOD_WEEK = TargetHeader.PERIOD_WEEK
    PERIOD_MONTH = TargetHeader.PERIOD_MONTH
    PERIOD_QUARTER = TargetHeader.PERIOD_QUARTER
    PERIOD_YEAR = TargetHeader.PERIOD_YEAR

    period_type = models.CharField(
        max_length=8,
        choices=[
            (PERIOD_WEEK, "Week"),
            (PERIOD_MONTH, "Month"),
            (PERIOD_QUARTER, "Quarter"),
            (PERIOD_YEAR, "Year"),
        ],
        blank=True,
        null=True,
    )
    period_id = models.CharField(max_length=10, blank=True, null=True)

    from_date = models.DateField(blank=True, null=True)
    to_date = models.DateField(blank=True, null=True)

    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)

    planned_amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)], default=0)

    # Optional (kept, even if form doesn’t expose it)
    notes = models.TextField(blank=True, null=True)

    class Meta:
        unique_together = ("period_type", "period_id", "customer")
        indexes = [
            models.Index(fields=["from_date", "to_date", "customer"]),
            models.Index(fields=["kam"]),
        ]


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

    # ✅ Changed to TextField (matches form + views expecting longer remarks)
    purpose = models.TextField(blank=True, null=True)

    approval_status = models.CharField(
        max_length=20,
        default=PENDING_APPROVAL,
        choices=APPROVAL_STATUS_CHOICES,
        db_index=True,
    )
    approved_by = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL, related_name="approved_batches")
    approved_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["kam", "from_date", "to_date"]),
            models.Index(fields=["approval_status"]),
        ]

    def __str__(self):
        return f"Batch#{self.id} {self.kam} {self.from_date}..{self.to_date} {self.visit_category}"


class VisitPlan(TimeStamped):
    PLANNED = "PLANNED"
    UNPLANNED = "UNPLANNED"

    DRAFT = VisitBatch.DRAFT
    PENDING_APPROVAL = VisitBatch.PENDING_APPROVAL
    PENDING = VisitBatch.PENDING
    APPROVED = VisitBatch.APPROVED
    REJECTED = VisitBatch.REJECTED

    CAT_VENDOR = VisitBatch.CAT_VENDOR
    CAT_CUSTOMER = VisitBatch.CAT_CUSTOMER
    CAT_SUPPLIER = VisitBatch.CAT_SUPPLIER
    CAT_WAREHOUSE = VisitBatch.CAT_WAREHOUSE

    APPROVAL_STATUS_CHOICES = [
        (DRAFT, "Draft"),
        (PENDING_APPROVAL, "Pending Approval"),
        (PENDING, "Pending (Legacy)"),
        (APPROVED, "Approved"),
        (REJECTED, "Rejected"),
    ]

    batch = models.ForeignKey(VisitBatch, null=True, blank=True, on_delete=models.CASCADE, related_name="lines")

    customer = models.ForeignKey(Customer, on_delete=models.PROTECT, null=True, blank=True)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)

    visit_date = models.DateField()
    visit_date_to = models.DateField(null=True, blank=True)

    # ✅ Default added so single-save doesn’t break if field missing
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

    # ✅ Changed to TextField to support real remarks length
    purpose = models.TextField(blank=True, null=True)

    expected_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    expected_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    # ✅ Changed to TextField (addresses often exceed 255)
    location = models.TextField(blank=True, null=True)

    approval_status = models.CharField(
        max_length=20,
        default=PENDING_APPROVAL,
        choices=APPROVAL_STATUS_CHOICES,
        db_index=True,
    )
    approved_by = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL, related_name="approved_visits")
    approved_at = models.DateTimeField(null=True, blank=True)

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


class VisitActual(TimeStamped):
    plan = models.OneToOneField(VisitPlan, on_delete=models.CASCADE, related_name="actual")

    actual_datetime = models.DateTimeField(null=True, blank=True, default=timezone.now)

    # New canonical field (UI uses this)
    meeting_notes = models.TextField(blank=True, null=True)

    # Legacy field (kept for compatibility)
    summary = models.TextField(blank=True, null=True)

    successful = models.BooleanField(default=False)

    not_success_reason = models.CharField(
        max_length=32,
        blank=True,
        null=True,
        choices=[
            ("PRICE", "Price"),
            ("MILL_NOT_APPROVED", "Mill not approved"),
            ("QUALITY", "Quality"),
            ("CREDIT_TERMS", "Credit / payment terms"),
            ("OTHER", "Other"),
        ],
    )

    confirmed_location = models.CharField(max_length=255, blank=True, null=True)

    actual_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    actual_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    next_action = models.CharField(max_length=255, blank=True, null=True)
    next_action_date = models.DateField(null=True, blank=True)
    reminder_cc_manager = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        # keep legacy summary in sync with meeting_notes
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

    # Optional new/compat fields (safe to keep even if unused)
    call_type = models.CharField(max_length=32, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)

    # legacy fields kept
    summary = models.TextField(blank=True, null=True)
    outcome = models.CharField(max_length=64, blank=True, null=True)

    def save(self, *args, **kwargs):
        # sync notes <-> summary (compat)
        if (self.notes or "").strip() and not (self.summary or "").strip():
            self.summary = self.notes
        elif (self.summary or "").strip() and not (self.notes or "").strip():
            self.notes = self.summary
        super().save(*args, **kwargs)

    class Meta:
        indexes = [models.Index(fields=["kam", "call_datetime"])]


class CollectionTxn(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)

    txn_datetime = models.DateTimeField(default=timezone.now)
    amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)])

    mode = models.CharField(max_length=32, blank=True, null=True)
    reference = models.CharField(max_length=64, blank=True, null=True)

    # Optional compat fields
    reference_no = models.CharField(max_length=64, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)

    def save(self, *args, **kwargs):
        # reference <-> reference_no sync
        if (self.reference_no or "").strip() and not (self.reference or "").strip():
            self.reference = self.reference_no
        elif (self.reference or "").strip() and not (self.reference_no or "").strip():
            self.reference_no = self.reference
        super().save(*args, **kwargs)

    class Meta:
        indexes = [models.Index(fields=["kam", "txn_datetime"])]


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

    plan = models.ForeignKey(VisitPlan, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True)
    batch = models.ForeignKey(VisitBatch, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True)

    actor = models.ForeignKey(User, on_delete=models.PROTECT, related_name="visit_approval_actions")
    action = models.CharField(
        max_length=16,
        choices=[
            (ACTION_APPROVE, "Approve"),
            (ACTION_REJECT, "Reject"),
            (ACTION_DELETE, "Delete"),
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

    scope = models.CharField(max_length=8, choices=[(SCOPE_SELF, "Self"), (SCOPE_TEAM, "Team")], default=SCOPE_SELF)
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

    step_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, null=True)

    class Meta:
        indexes = [models.Index(fields=["status", "created_at"])]
