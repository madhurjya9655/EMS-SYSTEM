from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from django.conf import settings
from django.core.exceptions import ValidationError as DjangoCoreValidationError
from django.core.validators import validate_email as dj_validate_email
from django.db import models, transaction
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
    """
    Server-side validator for uploaded receipts/bills.

    IMPORTANT: Keep this in sync with forms._allowed_exts()
    """
    max_mb = getattr(settings, "REIMBURSEMENT_MAX_RECEIPT_MB", 8)

    # Default list now matches the forms (images, PDF, Excel)
    default_exts = [".jpg", ".jpeg", ".png", ".pdf", ".xls", ".xlsx"]

    # Allow override via settings if provided
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
        # An expense item is editable by employee if it is NOT attached or has been rejected by finance for its line.
        # We keep simple rules here; the service will flip status back to SAVED on reject to unlock editing.
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
        PARTIAL_HOLD = "partial_hold", _("Partial Hold (Finance)")
        PARTIALLY_REJECTED = "partially_rejected", _("Partially Rejected (Back to Employee)")
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

    # ---- Derived status from child bills -----------------------------------

    def derive_status_from_bills(self) -> str:
        """
        Pure function: derive parent status from child bill_statuses.
        Rules (aligned with example workflow):
          - If ANY bill is FINANCE_REJECTED or EMPLOYEE_RESUBMITTED => PARTIAL_HOLD (Finance)
          - Else if ALL included bills are FINANCE_APPROVED => PENDING_MANAGER
          - Else => PARTIAL_HOLD (Finance)
        """
        qs = self.lines.all().values_list("bill_status", flat=True)
        statuses = set(qs)
        if not statuses:
            return self.Status.DRAFT

        # Any rejection or resubmission keeps it in Partial Hold (Finance)
        if (ReimbursementLine.BillStatus.FINANCE_REJECTED in statuses
            or ReimbursementLine.BillStatus.EMPLOYEE_RESUBMITTED in statuses):
            return self.Status.PARTIAL_HOLD

        # only when every included line reached finance-approved
        included_qs = self.lines.filter(status=ReimbursementLine.Status.INCLUDED)
        if included_qs.exists() and not included_qs.exclude(bill_status=ReimbursementLine.BillStatus.FINANCE_APPROVED).exists():
            return self.Status.PENDING_MANAGER

        # Mixed submitted/approved etc. => Partial Hold
        return self.Status.PARTIAL_HOLD

    def apply_derived_status_from_bills(self, *, actor: Optional[models.Model] = None, reason: str = "") -> None:
        """
        Set parent status strictly according to child bill statuses.
        Emits an audit log only if the status actually changes.
        """
        new_status = self.derive_status_from_bills()
        if new_status == self.status:
            return
        old = self.status
        self.status = new_status
        # Submitted_at remains first submit time; do not mutate.
        self.save(update_fields=["status", "updated_at"])
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=reason or "Derived status recalculated from bill-level changes.",
            from_status=old,
            to_status=new_status,
            extra={"type": "derived_status_from_bills"},
        )

    # ---- Centralized transition eligibility (read-only checks) --------------

    def can_mark_paid(self, reference: str = "") -> Tuple[bool, str]:
        """
        Read-only guard used by forms/views to decide whether "Mark Paid" is allowed.
        This mirrors the checks enforced by mark_paid().
        """
        # ✅ Allow settlement from Pending Finance Review OR Approved (legacy).
        if self.status not in {self.Status.PENDING_FINANCE, self.Status.APPROVED}:
            return False, _("Cannot mark Paid before Finance review stage after approvals.")
        if not (reference or "").strip():
            return False, _("Payment reference is required to mark Paid.")
        return True, ""

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
            # Note: request may reach PENDING_MANAGER through bill-level approvals (no verified_by).
            # We do NOT enforce verified_by here for derived transitions; FinanceVerifyView sets verified_by.
            pass

        if new == self.Status.PENDING_MANAGEMENT and not self._is_manager_approved():
            raise DjangoCoreValidationError(_("Cannot move to Management before Manager approval."))

        if new == self.Status.PENDING_FINANCE:
            if require_mgmt:
                if not self._is_management_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Management approval." ))
            else:
                if not self._is_manager_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Manager approval."))

        if new == self.Status.APPROVED and old != self.Status.PENDING_FINANCE:
            raise DjangoCoreValidationError(_("Only Finance can set Approved after Finance review."))

        if new == self.Status.PAID:
            # ✅ Allow transition to PAID from either APPROVED or PENDING_FINANCE.
            if old not in {self.Status.APPROVED, self.Status.PENDING_FINANCE}:
                raise DjangoCoreValidationError(_("Cannot mark Paid before Finance review stage after approvals."))
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

        # NOTE: No implicit audit here; all audit entries via explicit helpers/services.
        super().save(*args, **kwargs)

    # ---- Finance helpers ----------------------------------------------------

    def mark_verified(self, *, actor: Optional[models.Model] = None, note: str = "") -> None:
        """
        Request-level verify by finance (used when finance treats whole request).
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
        if self.status not in {self.Status.PENDING_FINANCE, self.Status.APPROVED}:
            raise DjangoCoreValidationError(_("Cannot mark Paid before Finance review stage after approvals."))
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

    # ---- Employee resubmit helper ------------------------------------------

    def employee_resubmit(self, *, actor: Optional[models.Model], note: str = "") -> None:
        """
        Employee resubmits a previously REJECTED reimbursement.
        """
        if self.status != self.Status.REJECTED:
            raise DjangoCoreValidationError(_("Only rejected requests can be resubmitted."))

        from_status = self.status

        with transaction.atomic():
            self.status = self.Status.PENDING_FINANCE_VERIFY
            self.submitted_at = timezone.now()

            self.verified_by = None
            self.verified_at = None

            self.manager_decision = ""
            self.manager_comment = self.manager_comment or ""
            self.manager_decided_at = None

            self.management_decision = ""
            self.management_comment = self.management_comment or ""
            self.management_decided_at = None

            if note:
                tag = f"[RESUBMIT] {note.strip()}"
                self.finance_note = (self.finance_note + ("\n" if self.finance_note else "") + tag).strip()

            self.save(update_fields=[
                "status",
                "submitted_at",
                "verified_by",
                "verified_at",
                "manager_decision",
                "manager_comment",
                "manager_decided_at",
                "management_decision",
                "management_comment",
                "management_decided_at",
                "finance_note",
                "updated_at",
            ])

            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=actor,
                message="Employee corrected and resubmitted the reimbursement.",
                from_status=from_status,
                to_status=self.status,
                extra={"type": "employee_resubmit"},
            )

    # ---- Admin override helpers (explicit, auditable, no auto-transitions) ----

    def reverse_to_finance_verification(self, *, actor: Optional[models.Model], reason: str) -> None:
        if not reason or not reason.strip():
            raise DjangoCoreValidationError(_("Reversal reason is required."))

        if self.status == self.Status.PENDING_FINANCE_VERIFY:
            raise DjangoCoreValidationError(_("The request is already pending Finance Verification."))

        if self.status == self.Status.REJECTED:
            raise DjangoCoreValidationError(_("Cannot reverse a Rejected request. Use resend to Finance if appropriate."))

        from_status = self.status

        with transaction.atomic():
            self.status = self.Status.PENDING_FINANCE_VERIFY

            self.verified_by = None
            self.verified_at = None

            self.manager_decision = ""
            self.manager_comment = self.manager_comment or ""
            self.manager_decided_at = None

            self.management_decision = ""
            self.management_comment = self.management_comment or ""
            self.management_decided_at = None

            note_line = f"[REVERSAL] Sent back to Finance Verification. Reason: {reason.strip()}"
            self.finance_note = (self.finance_note + ("\n" if self.finance_note else "") + note_line).strip()

            self.save(update_fields=[
                "status",
                "verified_by",
                "verified_at",
                "manager_decision",
                "manager_comment",
                "manager_decided_at",
                "management_decision",
                "management_comment",
                "management_decided_at",
                "finance_note",
                "updated_at",
            ])

            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.REVERSED,
                actor=actor,
                message=note_line,
                from_status=from_status,
                to_status=self.status,
                extra={"type": "reverse_to_finance_verification"},
            )

    def resend_to_finance(self, *, actor: Optional[models.Model], reason: str = "") -> None:
        from_status = self.status
        if self.status == self.Status.PENDING_FINANCE_VERIFY:
            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=actor,
                message="Admin re-sent to Finance Verification (already there). " + (reason or ""),
                from_status=from_status,
                to_status=self.status,
                extra={"type": "resend_to_finance"},
            )
            return
        self.reverse_to_finance_verification(actor=actor, reason=reason or "Admin resend to Finance Verification")

    def resend_to_manager(self, *, actor: Optional[models.Model], reason: str = "") -> None:
        if not self.verified_by_id:
            raise DjangoCoreValidationError(_("Cannot resend to Manager before Finance verifies."))
        from_status = self.status
        self.status = self.Status.PENDING_MANAGER
        note = f"Admin re-sent to Manager. {('Reason: ' + reason) if reason else ''}".strip()
        self.save(update_fields=["status", "updated_at"])
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=note,
            from_status=from_status,
            to_status=self.status,
            extra={"type": "resend_to_manager"},
        )

    def admin_force_move(self, target_status: str, *, actor: Optional[models.Model], reason: str = "") -> None:
        valid = {c[0] for c in self.Status.choices}
        if target_status not in valid:
            raise DjangoCoreValidationError(_("Invalid target status."))
        if target_status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Admin cannot directly set Paid."))

        from_status = self.status
        if from_status == target_status:
            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=actor,
                message=f"Admin attempted force move but status unchanged ({target_status}). {reason}".strip(),
                from_status=from_status,
                to_status=self.status,
                extra={"type": "admin_force_move_noop"},
            )
            return

        self.status = target_status
        self.save(update_fields=["status", "updated_at"])
        ReimbursementLog.log(
            self,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=f"Admin force-moved. {('Reason: ' + reason) if reason else ''}".strip(),
            from_status=from_status,
            to_status=self.status,
            extra={"type": "admin_force_move"},
        )

    def delete_with_audit(self, *, actor: Optional[models.Model], reason: str) -> None:
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be deleted."))
        if not reason or not reason.strip():
            raise DjangoCoreValidationError(_("A reason is required to delete a reimbursement."))

        with transaction.atomic():
            for line in list(self.lines.select_related("expense_item")):
                exp = line.expense_item
                line.delete()
                exp.status = ExpenseItem.Status.SAVED
                exp.save(update_fields=["status", "updated_at"])

            ReimbursementLog.log(
                self,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=actor,
                message=f"Admin deleted reimbursement. Reason: {reason.strip()}",
                from_status=self.status,
                to_status="deleted",
                extra={"type": "admin_delete_with_audit"},
            )
            super().delete()

class ReimbursementLine(models.Model):
    class Status(models.TextChoices):
        INCLUDED = "included", _("Included")
        REMOVED = "removed", _("Removed")

    class BillStatus(models.TextChoices):
        DRAFT = "draft", _("Draft")
        SUBMITTED = "submitted", _("Submitted")
        FINANCE_APPROVED = "finance_approved", _("Finance Approved")
        FINANCE_REJECTED = "finance_rejected", _("Finance Rejected")
        EMPLOYEE_RESUBMITTED = "employee_resubmitted", _("Employee Resubmitted")
        MANAGER_APPROVED = "manager_approved", _("Manager Approved")
        PAID = "paid", _("Paid")

    request = models.ForeignKey(ReimbursementRequest, on_delete=models.CASCADE, related_name="lines")
    expense_item = models.ForeignKey(ExpenseItem, on_delete=models.PROTECT, related_name="request_lines")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    description = models.TextField(blank=True, default="")
    receipt_file = models.FileField(upload_to=receipt_upload_path, validators=[validate_receipt_file], blank=True, null=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.INCLUDED, db_index=True)

    # --- NEW bill-level workflow fields ---
    bill_status = models.CharField(
        max_length=32,
        choices=BillStatus.choices,
        default=BillStatus.SUBMITTED,
        db_index=True,
    )
    finance_rejection_reason = models.TextField(blank=True, default="")
    rejected_by = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="finance_rejected_bills")
    rejected_at = models.DateTimeField(null=True, blank=True)
    last_modified_by = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="modified_reimbursement_lines")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)  # <-- added so update_fields including 'updated_at' are valid

    class Meta:
        ordering = ["id"]
        indexes = [
            models.Index(fields=["request", "status"]),
            models.Index(fields=["request", "bill_status"]),
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
                    ReimbursementRequest.Status.PARTIAL_HOLD,
                    ReimbursementRequest.Status.PARTIALLY_REJECTED,
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

    # ----- Bill-level state transitions (strict) -----------------------------

    def approve_by_finance(self, *, actor: Optional[models.Model]) -> None:
        """
        Approve ONLY this bill, leaving siblings untouched.
        """
        if self.bill_status == self.BillStatus.FINANCE_APPROVED:
            return
        prev = self.bill_status
        self.bill_status = self.BillStatus.FINANCE_APPROVED
        self.finance_rejection_reason = ""
        self.rejected_by = None
        self.rejected_at = None
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=[
            "bill_status", "finance_rejection_reason", "rejected_by", "rejected_at", "last_modified_by", "updated_at"
        ])
        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=f"Finance approved bill line #{self.pk}.",
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "type": "bill_finance_approved"},
        )

    def reject_by_finance(self, *, actor: Optional[models.Model], reason: str) -> None:
        """
        Reject ONLY this bill; unlocks the underlying expense for employee edits.
        Also emails the employee about this specific rejected bill.
        """
        if not reason or not reason.strip():
            raise DjangoCoreValidationError(_("Rejection reason is required."))
        prev = self.bill_status
        self.bill_status = self.BillStatus.FINANCE_REJECTED
        self.finance_rejection_reason = reason.strip()
        self.rejected_by = actor if isinstance(actor, models.Model) else None
        self.rejected_at = timezone.now()
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=[
            "bill_status", "finance_rejection_reason", "rejected_by", "rejected_at", "last_modified_by", "updated_at"
        ])

        # Unlock the expense for employee to edit/replace
        exp = self.expense_item
        exp.status = ExpenseItem.Status.SAVED
        exp.save(update_fields=["status", "updated_at"])

        # Notify employee about the rejected bill (bill-specific email)
        try:
            from .emails import send_bill_rejected_by_finance
            send_bill_rejected_by_finance(self.request, self)
        except Exception:
            logger.exception("Failed to send bill-rejected email for req=%s line=%s", self.request_id, self.pk)

        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=f"Finance rejected bill line #{self.pk}.",
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "reason": self.finance_rejection_reason, "type": "bill_finance_rejected"},
        )

    def employee_resubmit_bill(self, *, actor: Optional[models.Model]) -> None:
        """
        Employee corrected the rejected bill and re-submitted it for finance.
        Also emails finance about the resubmission.
        """
        if self.bill_status != self.BillStatus.FINANCE_REJECTED:
            raise DjangoCoreValidationError(_("Only finance-rejected bills can be resubmitted by employee."))
        prev = self.bill_status
        self.bill_status = self.BillStatus.EMPLOYEE_RESUBMITTED
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=["bill_status", "last_modified_by", "updated_at"])

        # Notify finance about this bill resubmission
        try:
            from .emails import send_bill_resubmitted
            send_bill_resubmitted(self.request, self, actor=actor)
        except Exception:
            logger.exception("Failed to send bill-resubmitted email for req=%s line=%s", self.request_id, self.pk)

        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=f"Employee resubmitted bill line #{self.pk} after correction.",
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "type": "bill_employee_resubmitted"},
        )

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
        # NEW explicit, auditable reversal action
        REVERSED = "reversed", _("Reversed to Finance Verification")

    request = models.ForeignKey(ReimbursementRequest, on_delete=models.CASCADE, related_name="logs")
    actor = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="reimbursement_logs")
    action = models.CharField(max_length=32, choices=Action.choices, db_index=True)
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
