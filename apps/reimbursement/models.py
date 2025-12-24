# apps/reimbursement/models.py
from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from django.conf import settings
from django.core.exceptions import ValidationError as DjangoCoreValidationError
from django.core.validators import validate_email as dj_validate_email
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)

UserModel = settings.AUTH_USER_MODEL

# ---------------------------------------------------------------------------
# Shared choices
# ---------------------------------------------------------------------------

# Category choices (used by legacy + new models)
REIMBURSEMENT_CATEGORY_CHOICES = [
    ("travel", "Travel Expenses"),
    ("meal", "Food Expenses"),          # keep key 'meal' for old data, label changed
    ("yard", "Yard Expenses"),
    ("office", "Office Supplies"),
    ("other", "Other"),
]

# GST bill type choices
GST_TYPE_CHOICES = [
    ("gst", "GST Bill"),
    ("non_gst", "Non GST Bill"),
]


# ---------------------------------------------------------------------------
# File upload helpers + validation
# ---------------------------------------------------------------------------

def receipt_upload_path(instance: models.Model, filename: str) -> str:
    """
    Store receipts under reimbursement/receipts/%Y/%m/%d/<filename>.
    Respects MEDIA_ROOT / MEDIA_URL.
    """
    today = timezone.now()
    return f"reimbursement/receipts/{today:%Y/%m/%d}/{filename}"


def _parse_email_list(raw: str) -> list[str]:
    """
    Split a comma/semicolon-separated string into a deduped, lowercase list of valid emails.
    Invalid addresses are skipped (and logged).
    """
    if not raw:
        return []
    # Normalize separators and split
    parts = [p.strip() for p in str(raw).replace(";", ",").split(",")]
    out: list[str] = []
    seen = set()
    for p in parts:
        if not p:
            continue
        low = p.lower()
        if low in seen:
            continue
        # Validate email format (skip and log if invalid)
        try:
            dj_validate_email(low)
        except Exception:
            logger.warning("Skipping invalid email address in settings: %r", p)
            continue
        seen.add(low)
        out.append(low)
    return out


def validate_receipt_file(value) -> None:
    """
    Server-side validation for receipt uploads:
    - Max size (MB) from settings.REIMBURSEMENT_MAX_RECEIPT_MB (default 8)
    - Allowed extensions from settings.REIMBURSEMENT_ALLOWED_EXTENSIONS
      (default: .jpg, .jpeg, .png, .pdf)
    """
    max_mb = getattr(settings, "REIMBURSEMENT_MAX_RECEIPT_MB", 8)
    default_exts = [".jpg", ".jpeg", ".png", ".pdf"]
    allowed_exts = getattr(
        settings,
        "REIMBURSEMENT_ALLOWED_EXTENSIONS",
        default_exts,
    )
    allowed_exts = [str(e).lower() for e in (allowed_exts or default_exts)]

    name = getattr(value, "name", "") or ""
    size = getattr(value, "size", 0) or 0

    ext = os.path.splitext(name)[1].lower()
    if ext not in allowed_exts:
        raise DjangoCoreValidationError(
            _("Unsupported file type '%(ext)s'. Allowed types: %(types)s"),
            params={"ext": ext, "types": ", ".join(allowed_exts)},
        )

    if size > max_mb * 1024 * 1024:
        raise DjangoCoreValidationError(
            _("File is too large (max %(max_mb)s MB)."),
            params={"max_mb": max_mb},
        )


# ---------------------------------------------------------------------------
# Admin-configurable reimbursement settings (emails, policy flags)
# ---------------------------------------------------------------------------

class ReimbursementSettings(models.Model):
    """
    Admin-configurable settings for the reimbursement module.

    - Configure who gets Admin summary, Finance, Management emails.
    - Control whether management approval is required.
    - Control daily digest behaviour.
    - Configure global approval chain (Level-1 / Level-2 + CC/BCC) for emails.
    """

    admin_emails = models.TextField(
        blank=True,
        default="",
        help_text="Comma-separated admin email addresses for reimbursement summaries.",
    )
    finance_emails = models.TextField(
        blank=True,
        default="",
        help_text="Comma-separated default Finance recipients for reimbursement notifications.",
    )
    management_emails = models.TextField(
        blank=True,
        default="",
        help_text="Comma-separated default Management recipients for reimbursement notifications.",
    )

    require_management_approval = models.BooleanField(
        default=True,
        help_text="If enabled, requests must be approved by Management before Finance can pay.",
    )

    daily_digest_enabled = models.BooleanField(
        default=True,
        help_text="If enabled, send a daily reimbursement summary email to Admin.",
    )
    digest_hour_local = models.PositiveSmallIntegerField(
        default=9,
        help_text="Local hour (0–23, Asia/Kolkata) to send daily digest when enabled.",
    )

    # ---------------- GLOBAL APPROVAL CHAIN FIELDS -----------------

    approver_level1_email = models.EmailField(
        blank=True,
        default="",
        help_text=(
            "Global first approver email for new reimbursement requests "
            "(e.g. vilas@blueoceansteels.com). If blank, the system falls back "
            "to per-employee Manager mapping."
        ),
    )
    # SINGLE primary Level-2 approver (Mumbai Accounts main address)
    approver_level2_email = models.EmailField(
        blank=True,
        default="",
        help_text=(
            "Primary Level-2 approver email after Level-1 approval "
            "(e.g. main Mumbai accounts email)."
        ),
    )
    # CC / BCC for Level-2 mail (can contain multiple addresses)
    approver_cc_emails = models.TextField(
        blank=True,
        default="",
        help_text=(
            "Comma-separated CC emails for the Level-2 notification "
            "(e.g. amreen@blueoceansteels.com, akshay@blueoceansteels.com)."
        ),
    )
    approver_bcc_emails = models.TextField(
        blank=True,
        default="",
        help_text=(
            "Comma-separated BCC emails for the Level-2 notification "
            "(e.g. vilas@blueoceansteels.com)."
        ),
    )

    class Meta:
        verbose_name = "Reimbursement Settings"
        verbose_name_plural = "Reimbursement Settings"

    def __str__(self) -> str:  # pragma: no cover
        return "Reimbursement Settings"

    @classmethod
    def get_solo(cls) -> "ReimbursementSettings":
        """
        Simple singleton pattern: always use pk=1 row.
        """
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    # Parsed helpers
    def admin_email_list(self) -> list[str]:
        return _parse_email_list(self.admin_emails)

    def finance_email_list(self) -> list[str]:
        return _parse_email_list(self.finance_emails)

    def management_email_list(self) -> list[str]:
        return _parse_email_list(self.management_emails)

    # New helpers for approval chain
    def approver_level1(self) -> Optional[str]:
        """
        Global Level-1 approver (e.g. vilas@...). None if not configured.
        """
        val = (self.approver_level1_email or "").strip().lower()
        try:
            if val:
                dj_validate_email(val)
                return val
        except Exception:
            logger.warning("Invalid approver_level1_email configured: %r", self.approver_level1_email)
        return None

    def approver_level2(self) -> Optional[str]:
        """
        Global Level-2 approver (e.g. Mumbai accounts main email).
        """
        val = (self.approver_level2_email or "").strip().lower()
        try:
            if val:
                dj_validate_email(val)
                return val
        except Exception:
            logger.warning("Invalid approver_level2_email configured: %r", self.approver_level2_email)
        return None

    def approver_cc_list(self) -> list[str]:
        """
        CC list for Level-2 notification (e.g. Amreen, Akshay, Sharyu),
        parsed and deduped.
        """
        return _parse_email_list(self.approver_cc_emails)

    def approver_bcc_list(self) -> list[str]:
        """
        BCC list for Level-2 notification (e.g. Vilas),
        parsed and deduped.
        """
        return _parse_email_list(self.approver_bcc_emails)


# ---------------------------------------------------------------------------
# Per-employee approver mapping (Admin-maintained)
# ---------------------------------------------------------------------------

class ReimbursementApproverMapping(models.Model):
    """
    Admin-maintained mapping for reimbursement approvals.

    For each employee, admin can assign:
      - reimbursement manager (first-level approver)
      - reimbursement finance user (finance reviewer)

    We'll expose this via a dedicated Admin page / grid where:
      - All employees are listed in one table.
      - Admin can quickly set the same manager/finance for everyone,
        or override per row.
    """

    employee = models.OneToOneField(
        UserModel,
        on_delete=models.CASCADE,
        related_name="reimbursement_approver_mapping",
        help_text="Employee who will submit reimbursement requests.",
    )
    manager = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursement_manager_for",
        help_text="Manager responsible for first-level reimbursement approval.",
    )
    finance = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursement_finance_for",
        help_text="Finance contact responsible for processing this employee's reimbursements.",
    )

    class Meta:
        verbose_name = "Reimbursement Approver Mapping"
        verbose_name_plural = "Reimbursement Approver Mappings"

    def __str__(self) -> str:  # pragma: no cover
        return f"Reimbursement mapping for {self.employee}"

    @classmethod
    def for_employee(cls, user) -> "ReimbursementApproverMapping | None":
        """
        Convenience helper to fetch mapping for a given user (or None).
        """
        if not user:
            return None
        try:
            return cls.objects.select_related("manager", "finance").get(employee=user)
        except cls.DoesNotExist:
            return None


# ---------------------------------------------------------------------------
# ExpenseItem: staging area for employee expense uploads
# ---------------------------------------------------------------------------

class ExpenseItem(models.Model):
    """
    Temporary personal expense record.
    Employees upload expenses here as they incur them.
    Later, multiple ExpenseItems can be attached to a ReimbursementRequest.

    NOTE: Files uploaded here are stored on the configured MEDIA storage.
    """

    class Status(models.TextChoices):
        SAVED = "saved", _("Saved (Draft)")
        ATTACHED = "attached", _("Attached to Request")
        SUBMITTED = "submitted", _("Submitted")
        VOID = "void", _("Voided")

    created_by = models.ForeignKey(
        UserModel,
        on_delete=models.CASCADE,
        related_name="expense_items",
    )
    date = models.DateField(
        help_text=_("Date the expense was incurred."),
    )
    category = models.CharField(
        max_length=32,
        choices=REIMBURSEMENT_CATEGORY_CHOICES,
    )
    amount = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        help_text=_("Amount must be greater than 0."),
    )
    # Vendor kept in DB for legacy, but no longer shown in forms/UI
    vendor = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text=_("Merchant / vendor name (optional)."),
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("Short description of the expense."),
    )
    gst_type = models.CharField(
        max_length=10,
        choices=GST_TYPE_CHOICES,
        default="non_gst",
        help_text=_("Whether this bill is under GST or not (GST Bill / Non GST Bill)."),
    )
    receipt_file = models.FileField(
        upload_to=receipt_upload_path,
        validators=[validate_receipt_file],
        help_text=_("Upload a jpg, png, or pdf (max size enforced by server)."),
    )
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.SAVED,
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-date", "-created_at"]
        indexes = [
            models.Index(fields=["created_by", "status"]),
            models.Index(fields=["date"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"ExpenseItem #{self.pk} – {self.created_by} – {self.amount}"

    def clean(self) -> None:
        super().clean()
        if self.amount is None or self.amount <= Decimal("0"):
            raise DjangoCoreValidationError({"amount": _("Amount must be greater than 0.")})

    @property
    def is_locked(self) -> bool:
        """
        True if this item is attached/submitted and should not be edited
        except when the owning request is in clarification.
        """
        return self.status in {self.Status.ATTACHED, self.Status.SUBMITTED}


# ---------------------------------------------------------------------------
# ReimbursementRequest + lines
# ---------------------------------------------------------------------------

class ReimbursementRequest(models.Model):
    """
    A submitted reimbursement request composed of one or more ExpenseItems.
    """

    class Status(models.TextChoices):
        DRAFT = "draft", _("Draft")
        PENDING_FINANCE_VERIFY = "pending_finance_verify", _("Pending Finance Verification")
        PENDING_MANAGER = "pending_manager", _("Pending Manager Approval")
        PENDING_MANAGEMENT = "pending_management", _("Pending Management Approval")
        PENDING_FINANCE = "pending_finance", _("Pending Finance Review")
        CLARIFICATION_REQUIRED = "clarification_required", _("Clarification Required")
        REJECTED = "rejected", _("Rejected")
        APPROVED = "approved", _("Approved (Ready to Pay)")
        PAID = "paid", _("Paid")

    created_by = models.ForeignKey(
        UserModel,
        on_delete=models.CASCADE,
        related_name="reimbursement_requests",
        help_text=_("Employee who submitted the reimbursement."),
    )
    submitted_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Timestamp when the request was first submitted."),
    )
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.PENDING_FINANCE_VERIFY,
        db_index=True,
    )
    total_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=Decimal("0.00"),
        help_text=_("Cached total of all included lines at submit time."),
    )

    # Routed approvers
    manager = models.ForeignKey(
        UserModel,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reimbursements_as_manager",
        help_text=_("Line manager / reporting person for first-level approval."),
    )
    management = models.ForeignKey(
        UserModel,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reimbursements_as_management",
        help_text=_("Higher-level management approver (second-level)."),
    )

    # Manager review
    manager_decision = models.CharField(
        max_length=16,
        blank=True,
        default="",
        help_text=_("Manager decision: approved/rejected/clarification."),
    )
    manager_comment = models.TextField(
        blank=True,
        default="",
        help_text=_("Manager remarks visible to employee."),
    )
    manager_decided_at = models.DateTimeField(null=True, blank=True)

    # Management review
    management_decision = models.CharField(
        max_length=16,
        blank=True,
        default="",
        help_text=_("Management decision: approved/rejected/clarification."),
    )
    management_comment = models.TextField(
        blank=True,
        default="",
        help_text=_("Management remarks visible to employee."),
    )
    management_decided_at = models.DateTimeField(null=True, blank=True)

    # Finance verification + processing
    verified_by = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursements_verified",
        help_text=_("Finance user who verified the reimbursement."),
    )
    verified_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("When Finance marked this request as Verified."),
    )
    finance_note = models.TextField(
        blank=True,
        default="",
        help_text=_("Internal finance notes."),
    )
    finance_payment_reference = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text=_("External payment reference / transaction ID."),
    )
    paid_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("When Finance marked this request as Paid."),
    )

    # Administrative
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_notified_admin_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Last time an admin summary was sent for this request."),
    )

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["created_by", "status"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"ReimbursementRequest #{self.pk} – {self.created_by} – {self.total_amount}"

    # ---- Status helpers -----------------------------------------------------

    @classmethod
    def final_statuses(cls) -> set[str]:
        return {cls.Status.REJECTED, cls.Status.PAID}

    @property
    def is_final(self) -> bool:
        return self.status in self.final_statuses()

    @property
    def is_paid(self) -> bool:
        return self.status == self.Status.PAID

    # ---- Amount & lines -----------------------------------------------------

    def recalc_total(self, save: bool = True) -> Decimal:
        """
        Recalculate total_amount from included lines.
        """
        total = (
            self.lines.filter(status=ReimbursementLine.Status.INCLUDED)
            .aggregate(sum=models.Sum("amount"))
            .get("sum")
            or Decimal("0.00")
        )
        self.total_amount = total
        if save:
            self.save(update_fields=["total_amount", "updated_at"])
        return total

    # ---- Transition validation ---------------------------------------------

    def _is_manager_approved(self) -> bool:
        return (self.manager_decision or "").lower() == "approved" and bool(self.manager_decided_at)

    def _is_management_approved(self) -> bool:
        return (self.management_decision or "").lower() == "approved" and bool(self.management_decided_at)

    def _validate_transition(self, old: str, new: str) -> None:
        """
        Enforce core business rules so statuses can't skip steps.
        Raises DjangoCoreValidationError on invalid transitions.
        """
        if not old or old == new:
            return

        require_mgmt = ReimbursementSettings.get_solo().require_management_approval

        # PENDING_MANAGER requires finance verification set
        if new == self.Status.PENDING_MANAGER and not self.verified_by_id:
            raise DjangoCoreValidationError(_("Cannot move to Manager review before Finance verifies."))

        # PENDING_MANAGEMENT requires manager approved
        if new == self.Status.PENDING_MANAGEMENT and not self._is_manager_approved():
            raise DjangoCoreValidationError(_("Cannot move to Management before Manager approval."))

        # PENDING_FINANCE requires final approvals depending on org policy
        if new == self.Status.PENDING_FINANCE:
            if require_mgmt:
                if not self._is_management_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Management approval."))
            else:
                if not self._is_manager_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Manager approval."))

        # APPROVED only from PENDING_FINANCE
        if new == self.Status.APPROVED and old != self.Status.PENDING_FINANCE:
            raise DjangoCoreValidationError(_("Only Finance can set Approved after Finance review."))

        # PAID only from APPROVED and needs payment reference
        if new == self.Status.PAID:
            if old != self.Status.APPROVED:
                raise DjangoCoreValidationError(_("Cannot mark Paid before Approved."))
            if not (self.finance_payment_reference or "").strip():
                raise DjangoCoreValidationError(_("Payment reference is required to mark Paid."))

    def save(self, *args, **kwargs):
        """
        Custom save:

        - Ensure submitted_at is set when status first moves out of DRAFT.
        - Enforce status transition rules.
        - Auto-log STATUS_CHANGED in audit log.
        """
        # Load current DB state for comparison
        old_status = None
        if self.pk:
            try:
                old_status = type(self).objects.only("status").get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        # Auto-set submitted_at once it leaves Draft
        if self.submitted_at is None and self.status != self.Status.DRAFT:
            self.submitted_at = timezone.now()

        # Validate transition (if changed)
        if old_status and self.status != old_status:
            self._validate_transition(old_status, self.status)

        super().save(*args, **kwargs)

        # Audit log for status change
        if old_status and self.status != old_status:
            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=None,
                message="System status transition",
                from_status=old_status,
                to_status=self.status,
            )

    # ---- Finance helpers ----------------------------------------------------

    def mark_verified(
        self,
        *,
        actor: Optional[models.Model] = None,
        note: str = "",
    ) -> None:
        """
        Mark as verified by Finance and log.
        """
        from_status = self.status
        self.status = self.Status.PENDING_MANAGER
        self.verified_by = actor if isinstance(actor, models.Model) else None
        self.verified_at = timezone.now()
        if note:
            if self.finance_note:
                self.finance_note = f"{self.finance_note}\n{note}"
            else:
                self.finance_note = note
        self.save(
            update_fields=[
                "status",
                "verified_by",
                "verified_at",
                "finance_note",
                "updated_at",
            ]
        )
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.VERIFIED,
            actor=actor,
            message="Finance verified the reimbursement.",
            from_status=from_status,
            to_status=self.status,
        )

    def mark_paid(
        self,
        reference: str,
        *,
        actor: Optional[models.Model] = None,
        note: str = "",
    ) -> None:
        """
        Helper for Finance to mark the request as Paid and log it.
        Enforces that current status is APPROVED and a reference is provided.
        """
        if self.status != self.Status.APPROVED:
            raise DjangoCoreValidationError(_("Cannot mark Paid before Approved."))
        if not (reference or "").strip():
            raise DjangoCoreValidationError(_("Payment reference is required to mark Paid."))

        from_status = self.status
        self.status = self.Status.PAID
        self.finance_payment_reference = reference
        self.paid_at = timezone.now()
        if note:
            if self.finance_note:
                self.finance_note = f"{self.finance_note}\n{note}"
            else:
                self.finance_note = note
        self.save(
            update_fields=[
                "status",
                "finance_payment_reference",
                "paid_at",
                "finance_note",
                "updated_at",
            ]
        )
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.PAID,
            actor=actor,
            message=f"Marked paid with reference {reference!r}",
            from_status=from_status,
            to_status=self.status,
        )


class ReimbursementLine(models.Model):
    """
    Line items attached to a ReimbursementRequest, each referencing an ExpenseItem.
    """

    class Status(models.TextChoices):
        INCLUDED = "included", _("Included")
        REMOVED = "removed", _("Removed")

    request = models.ForeignKey(
        ReimbursementRequest,
        on_delete=models.CASCADE,
        related_name="lines",
    )
    expense_item = models.ForeignKey(
        ExpenseItem,
        on_delete=models.PROTECT,
        related_name="request_lines",
        help_text=_("Source ExpenseItem used for this line."),
    )
    amount = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        help_text=_("Captured at time of submission (copied from ExpenseItem)."),
    )
    description = models.TextField(
        blank=True,
        default="",
        help_text=_("Captured at time of submission (copied from ExpenseItem)."),
    )
    # Optional copy/alias of the receipt file at submission time
    receipt_file = models.FileField(
        upload_to=receipt_upload_path,
        validators=[validate_receipt_file],
        blank=True,
        null=True,
        help_text=_("Snapshot of the receipt file when submitted (optional)."),
    )
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.INCLUDED,
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]
        indexes = [
            models.Index(fields=["request", "status"]),
            models.Index(fields=["expense_item"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"ReimbursementLine #{self.pk} – req={self.request_id} – item={self.expense_item_id}"

    def clean(self) -> None:
        """
        Enforce that an ExpenseItem cannot be reused in another *open* request.
        """
        super().clean()
        if self.expense_item_id:
            qs = ReimbursementLine.objects.filter(
                expense_item_id=self.expense_item_id,
                request__status__in={
                    ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
                    ReimbursementRequest.Status.PENDING_MANAGER,
                    ReimbursementRequest.Status.PENDING_MANAGEMENT,
                    ReimbursementRequest.Status.PENDING_FINANCE,
                    ReimbursementRequest.Status.CLARIFICATION_REQUIRED,
                    ReimbursementRequest.Status.APPROVED,
                },
            )
            if self.pk:
                qs = qs.exclude(pk=self.pk)
            if qs.exists():
                raise DjangoCoreValidationError(
                    {
                        "expense_item": _(
                            "This expense is already used in another open reimbursement request."
                        )
                    }
                )

    def save(self, *args, **kwargs):
        """
        On first save, default amount / description / receipt_file from the ExpenseItem
        if not explicitly set.
        """
        creating = self.pk is None
        if creating and self.expense_item_id:
            if not self.amount:
                self.amount = self.expense_item.amount
            if not self.description:
                self.description = self.expense_item.description
            if not self.receipt_file:
                # Reuse underlying file reference instead of copying data
                self.receipt_file = self.expense_item.receipt_file
        super().save(*args, **kwargs)


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------

class ReimbursementLog(models.Model):
    """
    Simple audit log for major state transitions and notifications.
    """

    class Action(models.TextChoices):
        CREATED = "created", _("Created")
        SUBMITTED = "submitted", _("Submitted")
        VERIFIED = "verified", _("Verified by Finance")
        MANAGER_APPROVED = "manager_approved", _("Approved by Manager")
        STATUS_CHANGED = "status_changed", _("Status Changed")
        COMMENTED = "commented", _("Comment Added")
        CLARIFICATION_REQUESTED = "clarification_requested", _("Clarification Requested")
        PAID = "paid", _("Marked Paid")
        EMAIL_SENT = "email_sent", _("Email Sent")

    request = models.ForeignKey(
        ReimbursementRequest,
        on_delete=models.CASCADE,
        related_name="logs",
    )
    actor = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursement_logs",
    )
    action = models.CharField(
        max_length=32,
        choices=Action.choices,
        db_index=True,
    )
    from_status = models.CharField(max_length=32, blank=True, default="")
    to_status = models.CharField(max_length=32, blank=True, default="")
    message = models.TextField(blank=True, default="")
    extra = models.JSONField(blank=True, default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["request", "action"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"ReimbursementLog #{self.pk} – req={self.request_id} – {self.action}"

    @classmethod
    def log(
        cls,
        request: ReimbursementRequest,
        action: str,
        *,
        actor: Optional[models.Model] = None,
        message: str = "",
        from_status: str = "",
        to_status: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> "ReimbursementLog":
        extra_data = extra or {}
        obj = cls.objects.create(
            request=request,
            actor=actor,
            action=action,
            from_status=from_status or "",
            to_status=to_status or "",
            message=message or "",
            extra=extra_data,
        )
        logger.info(
            "ReimbursementLog: req=%s action=%s from=%s to=%s actor=%s",
            getattr(request, "id", None),
            action,
            from_status,
            to_status,
            getattr(actor, "id", None) if actor else None,
        )
        return obj


# ---------------------------------------------------------------------------
# LEGACY SIMPLE MODEL (kept for backwards compatibility)
# ---------------------------------------------------------------------------

class Reimbursement(models.Model):
    """
    Legacy/simple reimbursement model currently used by existing views.

    NOTE:
    New flows should use ExpenseItem + ReimbursementRequest + ReimbursementLine.
    This model remains to keep existing migrations and any legacy data intact.
    """

    STATUS_CHOICES = [
        ("PM", "Pending Manager"),
        ("PF", "Pending Finance"),
        ("A", "Approved"),
        ("R", "Rejected"),
    ]
    CATEGORY_CHOICES = REIMBURSEMENT_CATEGORY_CHOICES

    employee = models.ForeignKey(UserModel, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    bill = models.FileField(upload_to="bills/")
    submitted_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(max_length=2, choices=STATUS_CHOICES, default="PM")
    manager_comment = models.TextField(blank=True, null=True)
    finance_comment = models.TextField(blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-submitted_at"]
        verbose_name = "Legacy Reimbursement"
        verbose_name_plural = "Legacy Reimbursements"

    def __str__(self) -> str:  # pragma: no cover
        return f"Legacy Reimbursement #{self.pk} – {self.employee} – {self.amount}"
