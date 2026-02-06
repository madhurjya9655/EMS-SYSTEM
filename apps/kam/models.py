from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.core.validators import MinValueValidator

User = get_user_model()


class TimeStamped(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Customer(TimeStamped):
    # Google Sheet → EMS (SoT: Sheet)
    name = models.CharField(max_length=255)
    gst_number = models.CharField(max_length=32, blank=True, null=True, db_index=True)
    contact_person = models.CharField(max_length=128, blank=True, null=True)  # NEW
    address = models.TextField(blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    mobile = models.CharField(max_length=32, blank=True, null=True)
    pincode = models.CharField(max_length=12, blank=True, null=True)
    type = models.CharField(max_length=64, blank=True, null=True)
    is_nbd = models.BooleanField(default=False)  # NEW: NBD marker from sheet
    credit_limit = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    agreed_credit_period_days = models.IntegerField(default=0)
    # cached primary KAM for speed
    primary_kam = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_customers"
    )

    def __str__(self):
        return self.name

    class Meta:
        # Defines the explicit gate used by views and the admin toggle
        permissions = [
            ("access_kam_module", "Can access KAM module"),
        ]


class KAMAssignment(TimeStamped):
    # history of KAM↔Customer mapping
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    kam = models.ForeignKey(User, on_delete=models.CASCADE)
    active_from = models.DateField()
    active_to = models.DateField(null=True, blank=True)  # null = active

    class Meta:
        indexes = [models.Index(fields=["customer", "kam", "active_from"])]


class InvoiceFact(TimeStamped):
    # Google Sheet row → idempotency by row_uuid
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True)
    invoice_date = models.DateField()
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    grade = models.CharField(max_length=64, blank=True, null=True)  # NEW
    size = models.CharField(max_length=64, blank=True, null=True)   # NEW
    qty_mt = models.DecimalField(max_digits=12, decimal_places=3, validators=[MinValueValidator(0)])
    revenue_gst = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)])


class LeadFact(TimeStamped):
    row_uuid = models.CharField(max_length=64, unique=True, db_index=True)
    doe = models.DateField()  # date of enquiry
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
    period_id = models.CharField(max_length=10)  # e.g. 2026-W05 or 2026-01 or 2026-Q1 or 2026
    manager = models.ForeignKey(User, on_delete=models.PROTECT, related_name="target_headers")
    locked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("period_type", "period_id")


class TargetLine(TimeStamped):
    """
    Per-KAM effective targets for a period.
    """
    header = models.ForeignKey(TargetHeader, on_delete=models.CASCADE, related_name="lines")
    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="kam_targets")
    sales_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    visits_target = models.IntegerField(default=6)      # typical weekly default
    calls_target = models.IntegerField(default=24)      # typical weekly default
    leads_target_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    nbd_target_monthly = models.IntegerField(default=0)
    # Collections plan amount for the period (₹).
    collections_plan_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    class Meta:
        unique_together = ("header", "kam")


class CollectionPlan(TimeStamped):
    """
    Manager-entered, customer-level planned collection for a chosen period
    OR a free date-range (dual mode; validated at form/view layer).
    """
    PERIOD_WEEK = TargetHeader.PERIOD_WEEK
    PERIOD_MONTH = TargetHeader.PERIOD_MONTH
    PERIOD_QUARTER = TargetHeader.PERIOD_QUARTER
    PERIOD_YEAR = TargetHeader.PERIOD_YEAR

    # Existing period-mode fields (kept for backward compatibility)
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
    period_id = models.CharField(max_length=10, blank=True, null=True)  # e.g. 2026-W05 / 2026-01 / 2026-Q1 / 2026

    # NEW: optional free date-range mode
    from_date = models.DateField(blank=True, null=True)
    to_date = models.DateField(blank=True, null=True)

    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)  # denormalized for scoping
    planned_amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)], default=0)

    class Meta:
        # Keep legacy uniqueness for period-mode; range-mode handled in application logic
        unique_together = ("period_type", "period_id", "customer")
        indexes = [
            models.Index(fields=["from_date", "to_date", "customer"]),
            models.Index(fields=["kam"]),
        ]


# ----------------------------
# NEW: Visit Batch (Header)
# ----------------------------
class VisitBatch(TimeStamped):
    """
    Header for multi-customer, multi-date-range visit submissions.
    Approval is non-blocking and is primarily for reporting/audit.
    """
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    # Category is common intent of the batch; lines can override if needed (we'll default from header).
    CAT_CUSTOMER = "CUSTOMER"
    CAT_SUPPLIER = "SUPPLIER"
    CAT_WAREHOUSE = "WAREHOUSE"

    VISIT_CATEGORY_CHOICES = [
        (CAT_CUSTOMER, "Customer Visit"),
        (CAT_SUPPLIER, "Supplier Visit"),
        (CAT_WAREHOUSE, "Warehouse Visit"),
    ]

    kam = models.ForeignKey(User, on_delete=models.PROTECT, related_name="visit_batches")
    from_date = models.DateField()
    to_date = models.DateField()
    visit_category = models.CharField(max_length=16, choices=VISIT_CATEGORY_CHOICES)
    purpose = models.CharField(max_length=128, blank=True, null=True)

    approval_status = models.CharField(
        max_length=12, default=PENDING, choices=[(PENDING, "Pending"), (APPROVED, "Approved"), (REJECTED, "Rejected")]
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
    # Operational type (KEEP AS-IS): planned vs unplanned (used by weekly cap, legacy filters)
    PLANNED = "PLANNED"
    UNPLANNED = "UNPLANNED"

    # Line-level approval (kept for backward compat with single-plan flow)
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    # NEW: business category (distinct from planned/unplanned)
    CAT_CUSTOMER = VisitBatch.CAT_CUSTOMER
    CAT_SUPPLIER = VisitBatch.CAT_SUPPLIER
    CAT_WAREHOUSE = VisitBatch.CAT_WAREHOUSE

    # Optional parent header for multi-customer submissions
    batch = models.ForeignKey(VisitBatch, null=True, blank=True, on_delete=models.CASCADE, related_name="lines")

    # Customer can be null for Supplier/Warehouse category
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT, null=True, blank=True)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)

    visit_date = models.DateField()
    visit_date_to = models.DateField(null=True, blank=True)  # retained: optional range end (legacy)

    # KEEP legacy operational field
    visit_type = models.CharField(max_length=12, choices=[(PLANNED, "Planned"), (UNPLANNED, "Unplanned")])

    # NEW: visit business category
    visit_category = models.CharField(
        max_length=16,
        choices=[
            (CAT_CUSTOMER, "Customer Visit"),
            (CAT_SUPPLIER, "Supplier Visit"),
            (CAT_WAREHOUSE, "Warehouse Visit"),
        ],
        default=CAT_CUSTOMER,
    )

    # For non-customer visits (supplier/warehouse), capture a free-text counterparty
    counterparty_name = models.CharField(max_length=255, blank=True, null=True)

    purpose = models.CharField(max_length=128, blank=True, null=True)

    # Expecteds are part of the plan (kept)
    expected_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    expected_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    # Location (mandatory at form level; model stays nullable to avoid breaking existing rows)
    location = models.CharField(max_length=255, blank=True, null=True)

    # Line-level approval (kept for existing flows)
    approval_status = models.CharField(
        max_length=12, default=PENDING, choices=[(PENDING, "Pending"), (APPROVED, "Approved"), (REJECTED, "Rejected")]
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
    # NEW: post-visit confirmed location (mandatory at form layer)
    confirmed_location = models.CharField(max_length=255, blank=True, null=True)

    # Actuals captured after the visit
    actual_sales_mt = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    actual_collection = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    next_action = models.CharField(max_length=255, blank=True, null=True)
    next_action_date = models.DateField(null=True, blank=True)
    reminder_cc_manager = models.BooleanField(default=True)


class CallLog(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    call_datetime = models.DateTimeField(default=timezone.now)
    duration_minutes = models.IntegerField(default=0)
    summary = models.TextField(blank=True, null=True)
    outcome = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        indexes = [models.Index(fields=["kam", "call_datetime"])]


class CollectionTxn(TimeStamped):
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    kam = models.ForeignKey(User, on_delete=models.PROTECT)
    txn_datetime = models.DateTimeField(default=timezone.now)
    amount = models.DecimalField(max_digits=14, decimal_places=2, validators=[MinValueValidator(0)])
    mode = models.CharField(max_length=32, blank=True, null=True)
    reference = models.CharField(max_length=64, blank=True, null=True)

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

    # Plan-level audit (kept)
    plan = models.ForeignKey(VisitPlan, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True)
    # NEW: optional batch-level audit
    batch = models.ForeignKey(VisitBatch, on_delete=models.CASCADE, related_name="approval_audits", null=True, blank=True)

    actor = models.ForeignKey(User, on_delete=models.PROTECT, related_name="visit_approval_actions")
    action = models.CharField(max_length=16, choices=[(ACTION_APPROVE, "Approve"), (ACTION_REJECT, "Reject")])
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
        choices=[(STATUS_PENDING, "Pending"), (STATUS_RUNNING, "Running"), (STATUS_SUCCESS, "Success"), (STATUS_ERROR, "Error")],
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
