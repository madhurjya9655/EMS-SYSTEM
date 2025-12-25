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

REIMBURSEMENT_CATEGORY_CHOICES = [
    ("travel", "Travel Expenses"),
    ("meal", "Food Expenses"),     # keep key 'meal' for old data
    ("yard", "Yard Expenses"),
    ("office", "Office Supplies"),
    ("other", "Other"),
]

GST_TYPE_CHOICES = [
    ("gst", "GST Bill"),
    ("non_gst", "Non GST Bill"),
]

# ---------------------------------------------------------------------------
# File upload helpers + validation
# ---------------------------------------------------------------------------

def receipt_upload_path(instance: models.Model, filename: str) -> str:
    today = timezone.now()
    return f"reimbursement/receipts/{today:%Y/%m/%d}/{filename}"

def _parse_email_list(raw: str) -> list[str]:
    if not raw:
        return []
    parts = [p.strip() for p in str(raw).replace(";", ",").split(",")]
    out: list[str] = []
    seen = set()
    for p in parts:
        if not p:
            continue
        low = p.lower()
        if low in seen:
            continue
        try:
            dj_validate_email(low)
        except Exception:
            logger.warning("Skipping invalid email address in settings: %r", p)
            continue
        seen.add(low)
        out.append(low)
    return out

def validate_receipt_file(value) -> None:
    max_mb = getattr(settings, "REIMBURSEMENT_MAX_RECEIPT_MB", 8)
    default_exts = [".jpg", ".jpeg", ".png", ".pdf"]
    allowed_exts = getattr(settings, "REIMBURSEMENT_ALLOWED_EXTENSIONS", default_exts)
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
# Admin-configurable reimbursement settings
# ---------------------------------------------------------------------------

class ReimbursementSettings(models.Model):
    admin_emails = models.TextField(blank=True, default="")
    finance_emails = models.TextField(blank=True, default="")
    management_emails = models.TextField(blank=True, default="")

    require_management_approval = models.BooleanField(default=True)

    daily_digest_enabled = models.BooleanField(default=True)
    digest_hour_local = models.PositiveSmallIntegerField(default=9)

    # Global approval chain
    approver_level1_email = models.EmailField(blank=True, default="")
    approver_level2_email = models.EmailField(blank=True, default="")
    approver_cc_emails = models.TextField(blank=True, default="")
    approver_bcc_emails = models.TextField(blank=True, default="")

    class Meta:
        verbose_name = "Reimbursement Settings"
        verbose_name_plural = "Reimbursement Settings"

    def __str__(self) -> str:  # pragma: no cover
        return "Reimbursement Settings"

    @classmethod
    def get_solo(cls) -> "ReimbursementSettings":
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    # Parsed helpers
    def admin_email_list(self) -> list[str]:
        return _parse_email_list(self.admin_emails)

    def finance_email_list(self) -> list[str]:
        return _parse_email_list(self.finance_emails)

    def management_email_list(self) -> list[str]:
        return _parse_email_list(self.management_emails)

    def approver_level1(self) -> Optional[str]:
        val = (self.approver_level1_email or "").strip().lower()
        try:
            if val:
                dj_validate_email(val)
                return val
        except Exception:
            logger.warning("Invalid approver_level1_email configured: %r", self.approver_level1_email)
        return None

    def approver_level2(self) -> Optional[str]:
        val = (self.approver_level2_email or "").strip().lower()
        try:
            if val:
                dj_validate_email(val)
                return val
        except Exception:
            logger.warning("Invalid approver_level2_email configured: %r", self.approver_level2_email)
        return None

    def approver_cc_list(self) -> list[str]:
        return _parse_email_list(self.approver_cc_emails)

    def approver_bcc_list(self) -> list[str]:
        return _parse_email_list(self.approver_bcc_emails)

# ---------------------------------------------------------------------------
# Per-employee approver mapping
# ---------------------------------------------------------------------------

class ReimbursementApproverMapping(models.Model):
    employee = models.OneToOneField(
        UserModel,
        on_delete=models.CASCADE,
        related_name="reimbursement_approver_mapping",
    )
    manager = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursement_manager_for",
    )
    finance = models.ForeignKey(
        UserModel,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reimbursement_finance_for",
    )

    class Meta:
        verbose_name = "Reimbursement Approver Mapping"
        verbose_name_plural = "Reimbursement Approver Mappings"

    def __str__(self) -> str:  # pragma: no cover
        return f"Reimbursement mapping for {self.employee}"

    @classmethod
    def for_employee(cls, user) -> "ReimbursementApproverMapping | None":
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
    class Status(models.TextChoices):
        SAVED = "saved", _("Saved (Draft)")
        ATTACHED = "attached", _("Attached to Request")
        SUBMITTED = "submitted", _("Submitted")
        VOID = "void", _("Voided")

    created_by = models.ForeignKey(UserModel, on_delete=models.CASCADE, related_name="expense_items")
    date = models.DateField()
    category = models.CharField(max_length=32, choices=REIMBURSEMENT_CATEGORY_CHOICES)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    vendor = models.CharField(max_length=255, blank=True, default="")
    description = models.TextField(blank=True, default="")
    gst_type = models.CharField(max_length=10, choices=GST_TYPE_CHOICES, default="non_gst")
    receipt_file = models.FileField(upload_to=receipt_upload_path, validators=[validate_receipt_file])
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.SAVED, db_index=True)
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
        return self.status in {self.Status.ATTACHED, self.Status.SUBMITTED}

# ---------------------------------------------------------------------------
# ReimbursementRequest + lines
# ---------------------------------------------------------------------------

class ReimbursementRequest(models.Model):
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
        UserModel, on_delete=models.CASCADE, related_name="reimbursement_requests"
    )
    submitted_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING_FINANCE_VERIFY, db_index=True)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    # Routed approvers
    manager = models.ForeignKey(UserModel, null=True, blank=True, on_delete=models.SET_NULL, related_name="reimbursements_as_manager")
    management = models.ForeignKey(UserModel, null=True, blank=True, on_delete=models.SET_NULL, related_name="reimbursements_as_management")

    # Manager review
    manager_decision = models.CharField(max_length=16, blank=True, default="")
    manager_comment = models.TextField(blank=True, default="")
    manager_decided_at = models.DateTimeField(null=True, blank=True)

    # Management review
    management_decision = models.CharField(max_length=16, blank=True, default="")
    management_comment = models.TextField(blank=True, default="")
    management_decided_at = models.DateTimeField(null=True, blank=True)

    # Finance verification + processing
    verified_by = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="reimbursements_verified")
    verified_at = models.DateTimeField(null=True, blank=True)
    finance_note = models.TextField(blank=True, default="")
    finance_payment_reference = models.CharField(max_length=255, blank=True, default="")
    paid_at = models.DateTimeField(null=True, blank=True)

    # Administrative
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_notified_admin_at = models.DateTimeField(null=True, blank=True)

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
        if not old or old == new:
            return

        require_mgmt = ReimbursementSettings.get_solo().require_management_approval

        if new == self.Status.PENDING_MANAGER and not self.verified_by_id:
            raise DjangoCoreValidationError(_("Cannot move to Manager review before Finance verifies."))

        if new == self.Status.PENDING_MANAGEMENT and not self._is_manager_approved():
            raise DjangoCoreValidationError(_("Cannot move to Management before Manager approval."))

        if new == self.Status.PENDING_FINANCE:
            if require_mgmt:
                if not self._is_management_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Management approval."))  # noqa: E501
            else:
                if not self._is_manager_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Manager approval."))

        if new == self.Status.APPROVED and old != self.Status.PENDING_FINANCE:
            raise DjangoCoreValidationError(_("Only Finance can set Approved after Finance review."))

        if new == self.Status.PAID:
            if old != self.Status.APPROVED:
                raise DjangoCoreValidationError(_("Cannot mark Paid before Approved."))
            if not (self.finance_payment_reference or "").strip():
                raise DjangoCoreValidationError(_("Payment reference is required to mark Paid."))

    # ---- Invariants ---------------------------------------------------------

    def clean(self) -> None:
        """
        Strong invariant to catch raw updates:
        If status == PAID -> both paid_at and finance_payment_reference must exist.
        """
        super().clean()
        if self.status == self.Status.PAID:
            if not self.paid_at or not (self.finance_payment_reference or "").strip():
                raise DjangoCoreValidationError(
                    _("Cannot set status to Paid unless payment reference and paid timestamp are recorded.")
                )

    # ---- Save ---------------------------------------------------------------

    def save(self, *args, **kwargs):
        old_status = None
        if self.pk:
            try:
                old_status = type(self).objects.only("status").get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        if self.submitted_at is None and self.status != self.Status.DRAFT:
            self.submitted_at = timezone.now()

        if old_status and self.status != old_status:
            self._validate_transition(old_status, self.status)
            # run invariants on status change
            self.full_clean()

        # NOTE: Removed automatic "System status transition" audit logging here.
        # All audit entries must be created by explicit, role-checked actions (views/services).
        super().save(*args, **kwargs)

    # ---- Finance helpers ----------------------------------------------------

    def mark_verified(self, *, actor: Optional[models.Model] = None, note: str = "") -> None:
        """
        Single-source-of-truth to verify by finance.
        Emits ONE audit log with the correct actor.
        """
        from_status = self.status
        self.status = self.Status.PENDING_MANAGER
        self.verified_by = actor if isinstance(actor, models.Model) else None
        self.verified_at = timezone.now()
        if note:
            self.finance_note = f"{self.finance_note}\n{note}" if self.finance_note else note
        self.save(update_fields=["status", "verified_by", "verified_at", "finance_note", "updated_at"])
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.VERIFIED,
            actor=actor,
            message="Finance verified the reimbursement.",
            from_status=from_status,
            to_status=self.status,
        )

    def mark_paid(self, reference: str, *, actor: Optional[models.Model] = None, note: str = "") -> None:
        """
        Single-source-of-truth to mark as paid.
        Emits ONE audit log with the correct actor.
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
            self.finance_note = f"{self.finance_note}\n{note}" if self.finance_note else note
        self.save(update_fields=["status", "finance_payment_reference", "paid_at", "finance_note", "updated_at"])
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.PAID,
            actor=actor,
            message=f"Marked paid with reference {reference!r}",
            from_status=from_status,
            to_status=self.status,
        )

class ReimbursementLine(models.Model):
    class Status(models.TextChoices):
        INCLUDED = "included", _("Included")
        REMOVED = "removed", _("Removed")

    request = models.ForeignKey(ReimbursementRequest, on_delete=models.CASCADE, related_name="lines")
    expense_item = models.ForeignKey(ExpenseItem, on_delete=models.PROTECT, related_name="request_lines")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    description = models.TextField(blank=True, default="")
    receipt_file = models.FileField(upload_to=receipt_upload_path, validators=[validate_receipt_file], blank=True, null=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.INCLUDED, db_index=True)
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
                    {"expense_item": _("This expense is already used in another open reimbursement request.")}
                )

    def save(self, *args, **kwargs):
        creating = self.pk is None
        if creating and self.expense_item_id:
            if not self.amount:
                self.amount = self.expense_item.amount
            if not self.description:
                self.description = self.expense_item.description
            if not self.receipt_file:
                self.receipt_file = self.expense_item.receipt_file
        super().save(*args, **kwargs)

# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------

class ReimbursementLog(models.Model):
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

    request = models.ForeignKey(ReimbursementRequest, on_delete=models.CASCADE, related_name="logs")
    actor = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="reimbursement_logs")
    action = models.CharField(max_length=32, choices=Action.choices, db_index=True)
    from_status = models.CharField(max_length=32, blank=True, default="")
    to_status = models.CharField(maxlength=32, blank=True, default="") if False else models.CharField(max_length=32, blank=True, default="")  # lint helper
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
