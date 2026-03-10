# FILE: apps/reimbursement/models.py
# PURPOSE: Fix rejected/resubmitted bill amount syncing + exclude rejected lines from forward totals
# UPDATED: 2026-03-10
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from django.conf import settings
from django.core.exceptions import ValidationError as DjangoCoreValidationError
from django.core.validators import validate_email as dj_validate_email
from django.db import models, transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)

UserModel = settings.AUTH_USER_MODEL

# ---------------------------------------------------------------------------
# Shared choices
# ---------------------------------------------------------------------------

REIMBURSEMENT_CATEGORY_CHOICES = [
    ("travel", "Travel Expenses"),
    ("meal", "Food Expenses"),  # keep key 'meal' for old data
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
    default_exts = [".jpg", ".jpeg", ".png", ".pdf", ".xls", ".xlsx"]
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
# ExpenseItem
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

    def rejected_lines_qs(self):
        from .models import ReimbursementLine  # local import to avoid circulars
        return self.request_lines.filter(
            bill_status__in=[
                ReimbursementLine.BillStatus.FINANCE_REJECTED,
                ReimbursementLine.BillStatus.MANAGER_REJECTED,
            ],
            status=ReimbursementLine.Status.INCLUDED,
        )

    def has_finance_rejected_lines(self) -> bool:
        return self.rejected_lines_qs().exists()

    def resubmit_rejected_lines(self, *, actor: Optional[models.Model]) -> int:
        from .models import ReimbursementLine, ReimbursementRequest
        count = 0
        touched_requests: set[int] = set()

        for line in self.rejected_lines_qs().select_related("request"):
            line.employee_resubmit_bill(actor=actor)
            touched_requests.add(line.request_id)
            count += 1

        if count:
            self.status = ExpenseItem.Status.SUBMITTED
            self.save(update_fields=["status", "updated_at"])

            for req_id in touched_requests:
                try:
                    req = ReimbursementRequest.objects.get(pk=req_id)
                    req.apply_derived_status_from_bills(actor=actor, reason="Employee resubmitted corrected bill(s).")
                except ReimbursementRequest.DoesNotExist:
                    continue

        return count

    def save(self, *args, **kwargs):
        """
        IMPORTANT FIX (Issue 3):
        Propagate ExpenseItem edits into any rejected/resubmitted ReimbursementLine rows
        so Finance/Manager queues always display the latest amount and description.

        NOTE: We use QuerySet.update() intentionally here to avoid triggering
        ReimbursementLine.save() auto-population (which would be circular). This
        means the Sheets sync signal will not fire for those lines — acceptable,
        since the parent request's next save will trigger the sync.
        """
        creating = self.pk is None
        old_amount = None
        old_description = None
        old_receipt_name = None

        if not creating:
            try:
                prev = type(self).objects.only("amount", "description", "receipt_file").get(pk=self.pk)
                old_amount = prev.amount
                old_description = prev.description
                old_receipt_name = getattr(prev.receipt_file, "name", None)
            except type(self).DoesNotExist:
                creating = True

        super().save(*args, **kwargs)

        if creating:
            return

        new_receipt_name = getattr(self.receipt_file, "name", None)
        changed = (
            (old_amount is not None and self.amount != old_amount)
            or ((old_description or "") != (self.description or ""))
            or ((old_receipt_name or "") != (new_receipt_name or ""))
        )
        if not changed:
            return

        try:
            from .models import ReimbursementLine  # local import to avoid circulars

            # Sync into rejected/resubmitted lines so Finance UI always shows latest values.
            sync_statuses = [
                ReimbursementLine.BillStatus.FINANCE_REJECTED,
                ReimbursementLine.BillStatus.MANAGER_REJECTED,
                ReimbursementLine.BillStatus.EMPLOYEE_RESUBMITTED,
            ]

            qs = ReimbursementLine.objects.filter(
                expense_item_id=self.pk,
                status=ReimbursementLine.Status.INCLUDED,
                bill_status__in=sync_statuses,
            )

            qs.update(
                amount=self.amount,
                description=self.description,
                receipt_file=self.receipt_file,
                updated_at=timezone.now(),
            )
        except Exception:
            logger.exception(
                "Failed to propagate edited ExpenseItem #%s into ReimbursementLine rows (rejected/resubmitted).",
                self.pk,
            )


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
        PARTIAL_HOLD = "partial_hold", _("Partial Hold (Finance)")  # deprecated: no longer used
        PARTIALLY_REJECTED = "partially_rejected", _("Partially Rejected (Back to Employee)")  # deprecated
        CLARIFICATION_REQUIRED = "clarification_required", _("Clarification Required")
        REJECTED = "rejected", _("Rejected")
        APPROVED = "approved", _("Approved (Ready to Pay)")
        PAID = "paid", _("Paid")  # (= Settled)

    ACTIVE_STATUSES: set[str] = {
        Status.PENDING_FINANCE_VERIFY,
        Status.PENDING_MANAGER,
    }

    created_by = models.ForeignKey(
        UserModel, on_delete=models.CASCADE, related_name="reimbursement_requests"
    )
    submitted_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING_FINANCE_VERIFY, db_index=True)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    manager = models.ForeignKey(UserModel, null=True, blank=True, on_delete=models.SET_NULL, related_name="reimbursements_as_manager")
    management = models.ForeignKey(UserModel, null=True, blank=True, on_delete=models.SET_NULL, related_name="reimbursements_as_management")

    manager_decision = models.CharField(max_length=16, blank=True, default="")
    manager_comment = models.TextField(blank=True, default="")
    manager_decided_at = models.DateTimeField(null=True, blank=True)

    management_decision = models.CharField(max_length=16, blank=True, default="")
    management_comment = models.TextField(blank=True, default="")
    management_decided_at = models.DateTimeField(null=True, blank=True)

    verified_by = models.ForeignKey(UserModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="reimbursements_verified")
    verified_at = models.DateTimeField(null=True, blank=True)
    finance_note = models.TextField(blank=True, default="")
    finance_payment_reference = models.CharField(max_length=255, blank=True, default="")
    paid_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_notified_admin_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["created_by", "status"]),
        ]
        constraints = [
            models.CheckConstraint(
                name="rr_paid_requires_reference_and_timestamp",
                check=(~Q(status="paid") | (Q(paid_at__isnull=False) & ~Q(finance_payment_reference=""))),
            ),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"ReimbursementRequest #{self.pk} – {self.created_by} – {self.total_amount}"

    @classmethod
    def final_statuses(cls) -> set[str]:
        return {cls.Status.REJECTED, cls.Status.PAID}

    @property
    def is_final(self) -> bool:
        return self.status in self.final_statuses()

    @property
    def is_paid(self) -> bool:
        return self.status == self.Status.PAID

    def recalc_total(self, save: bool = True) -> Decimal:
        """
        BUSINESS RULE FIX (Issue 2):
        total_amount must reflect ONLY lines that remain in the forward workflow.
        Finance/Manager rejected lines are returned to the employee and MUST NOT be included.
        PAID lines remain included so the total stays accurate after settlement.
        """
        L = self.lines.model  # ReimbursementLine

        excluded_statuses = [
            L.BillStatus.FINANCE_REJECTED,
            L.BillStatus.MANAGER_REJECTED,
        ]

        total = (
            self.lines.filter(status=L.Status.INCLUDED)
            .exclude(bill_status__in=excluded_statuses)
            .aggregate(sum=models.Sum("amount"))
            .get("sum")
            or Decimal("0.00")
        )
        self.total_amount = total
        if save:
            self.save(update_fields=["total_amount", "updated_at"])
        return total

    def derive_status_from_bills(self) -> str:
        """
        FIX (Issue #2): Extended to handle ALL bill statuses, not just the first
        four. The original fell through to PENDING_FINANCE_VERIFY for
        MANAGER_PENDING, MANAGER_APPROVED, MANAGER_REJECTED, and PAID bills,
        which could silently regress workflow state.

        Priority order (highest first):
          1. Finance still needs to verify something          → PENDING_FINANCE_VERIFY
          2. Manager still needs to act on approved bills     → PENDING_MANAGER
          3. No active forward bills remain                   → REJECTED
          4. All active bills paid                            → PAID
          5. All active bills manager-approved                → PENDING_FINANCE
          6. Fallback                                         → PENDING_FINANCE_VERIFY
        """
        L = ReimbursementLine
        bill_statuses = list(
            self.lines.filter(status=L.Status.INCLUDED)
            .values_list("bill_status", flat=True)
        )

        if not bill_statuses:
            return self.Status.DRAFT

        statuses = set(bill_statuses)

        # 1. Finance verification needed — blocks everything else.
        #    MANAGER_REJECTED also returns the bill to Finance (employee must resubmit).
        if statuses & {L.BillStatus.SUBMITTED, L.BillStatus.EMPLOYEE_RESUBMITTED, L.BillStatus.MANAGER_REJECTED}:
            return self.Status.PENDING_FINANCE_VERIFY

        # 2. Manager action needed on finance-approved or already-queued bills.
        if statuses & {L.BillStatus.FINANCE_APPROVED, L.BillStatus.MANAGER_PENDING}:
            return self.Status.PENDING_MANAGER

        # Strip "returned-to-employee" bills from active consideration.
        active = statuses - {L.BillStatus.FINANCE_REJECTED}

        # 3. Nothing active in the forward workflow → all rejected.
        if not active:
            return self.Status.REJECTED

        # 4. Every active bill is already paid.
        if active <= {L.BillStatus.PAID}:
            return self.Status.PAID

        # 5. Remaining active bills are manager-approved (awaiting Finance settlement).
        if active <= {L.BillStatus.MANAGER_APPROVED, L.BillStatus.PAID}:
            return self.Status.PENDING_FINANCE

        # Fallback — should not normally be reached.
        return self.Status.PENDING_FINANCE_VERIFY

    _STATUS_ORDER = {
        Status.DRAFT: 0,
        Status.PENDING_FINANCE_VERIFY: 1,
        Status.PENDING_MANAGER: 2,
        Status.CLARIFICATION_REQUIRED: 2,
        Status.PENDING_MANAGEMENT: 3,
        Status.PENDING_FINANCE: 4,
        Status.APPROVED: 5,
        Status.PAID: 6,
        Status.REJECTED: 6,
        Status.PARTIAL_HOLD: 1,
        Status.PARTIALLY_REJECTED: 1,
    }

    def _status_rank(self, status: str) -> int:
        return self._STATUS_ORDER.get(status, 0)

    def apply_derived_status_from_bills(self, *, actor: Optional[models.Model] = None, reason: str = "") -> None:
        if self.is_final:
            return
        new_status = self.derive_status_from_bills()
        if new_status == self.status:
            return

        current_rank = self._status_rank(self.status)
        new_rank = self._status_rank(new_status)

        if new_rank < current_rank:
            return

        old = self.status
        self.status = new_status
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

    def bill_mix_summary(self) -> dict:
        L = ReimbursementLine
        inc = self.lines.filter(status=L.Status.INCLUDED)
        return {
            "total": inc.count(),
            "pending": inc.filter(bill_status__in=[L.BillStatus.SUBMITTED, L.BillStatus.EMPLOYEE_RESUBMITTED]).count(),
            "approved": inc.filter(bill_status=L.BillStatus.FINANCE_APPROVED).count(),
            "rejected": inc.filter(bill_status=L.BillStatus.FINANCE_REJECTED).count(),
            "paid": inc.filter(bill_status=L.BillStatus.PAID).count(),
        }

    def can_mark_paid(self, reference: str = "") -> Tuple[bool, str]:
        """
        FIX (Issue #3): The original excluded only FINANCE_APPROVED from the
        "not yet approved" check. In the per-bill payment flow individual lines
        can already be PAID before the request-level mark_paid is called.
        Those PAID lines must also be accepted as settled, otherwise the gate
        falsely blocks a fully-paid request.
        """
        if self.status == self.Status.PAID:
            return True, _("Already paid")

        if self.status not in {self.Status.PENDING_FINANCE, self.Status.APPROVED}:
            return False, _("Cannot mark Paid before Finance settlement after approvals.")

        if not (reference or "").strip():
            return False, _("Payment reference is required to mark Paid.")

        L = self.lines.model
        inc = self.lines.filter(status=L.Status.INCLUDED)
        if not inc.exists():
            return False, _("No included bills to pay.")

        # Accept both FINANCE_APPROVED and already-PAID lines.
        unsettled = inc.exclude(bill_status__in=[L.BillStatus.FINANCE_APPROVED, L.BillStatus.PAID])
        if unsettled.exists():
            return False, _("Some bills are not Finance-Approved or Paid yet.")

        return True, ""

    def _is_manager_approved(self) -> bool:
        return (self.manager_decision or "").lower() == "approved" and bool(self.manager_decided_at)

    def _is_management_approved(self) -> bool:
        return (self.management_decision or "").lower() == "approved" and bool(self.management_decided_at)

    def _validate_transition(self, old: str, new: str) -> None:
        if not old or old == new:
            return

        if old == self.Status.PAID and new != self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements are immutable and cannot change status."))

        require_mgmt = ReimbursementSettings.get_solo().require_management_approval

        # NOTE (Fix #12): Moving to PENDING_MANAGER without verified_by is intentionally
        # allowed here because apply_derived_status_from_bills() advances the status when
        # all finance-approved bills exist, and the calling view then stamps verified_by
        # immediately after. The two-step is by design; verified_by is not a pre-condition
        # enforced at the model transition layer.
        if new == self.Status.PENDING_MANAGER and not self.verified_by_id:
            pass  # intentional — verified_by set by calling view post-transition

        if new == self.Status.PENDING_MANAGEMENT and not self._is_manager_approved():
            raise DjangoCoreValidationError(_("Cannot move to Management before Manager approval."))

        if new == self.Status.PENDING_FINANCE:
            if require_mgmt:
                if not self._is_management_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Management approval."))
            else:
                if not self._is_manager_approved():
                    raise DjangoCoreValidationError(_("Cannot move to Finance review before Manager approval."))

        if new == self.Status.APPROVED and old != self.Status.PENDING_FINANCE:
            raise DjangoCoreValidationError(_("Only Finance can set Approved after Finance review."))

        if new == self.Status.PAID:
            if old not in {self.Status.APPROVED, self.Status.PENDING_FINANCE}:
                raise DjangoCoreValidationError(_("Cannot mark Paid before Finance review stage after approvals."))
            if not (self.finance_payment_reference or "").strip():
                raise DjangoCoreValidationError(_("Payment reference is required to mark Paid."))

    def clean(self) -> None:
        super().clean()
        if self.status == self.Status.PAID:
            if not self.paid_at or not (self.finance_payment_reference or "").strip():
                raise DjangoCoreValidationError(
                    _("Cannot set status to Paid unless payment reference and paid timestamp are recorded.")
                )

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
            self.full_clean()

        super().save(*args, **kwargs)

    def mark_verified(self, *, actor: Optional[models.Model] = None, note: str = "") -> None:
        if self.is_final:
            raise DjangoCoreValidationError(_("Cannot verify a finalized reimbursement."))
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
        ok, msg = self.can_mark_paid(reference)
        if not ok:
            raise DjangoCoreValidationError(msg)

        if self.status == self.Status.PAID:
            return

        from_status = self.status

        # FIX (Issue #8): Replace per-line loop with a single bulk UPDATE.
        # The original iterated and saved each line individually causing N+1 queries.
        # Signals and inline save() logic are intentionally bypassed here — the
        # request itself is being stamped PAID immediately after, making
        # re-derivation from each individual line update unnecessary.
        L = self.lines.model
        now = timezone.now()
        self.lines.filter(status=L.Status.INCLUDED).update(
            bill_status=L.BillStatus.PAID,
            payment_reference=reference.strip(),
            paid_at=now,
            updated_at=now,
        )

        self.status = self.Status.PAID
        self.finance_payment_reference = reference.strip()
        self.paid_at = now
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

    def employee_resubmit(self, *, actor: Optional[models.Model], note: str = "") -> None:
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be resubmitted."))
        if self.status != self.Status.REJECTED:
            raise DjangoCoreValidationError(_("Only rejected requests can be resubmitted."))

        from_status = self.status

        with transaction.atomic():
            self.status = self.Status.PENDING_FINANCE_VERIFY
            self.submitted_at = timezone.now()

            # Clear the entire approval chain so reviewers start fresh.
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

    def reverse_to_finance_verification(self, *, actor: Optional[models.Model], reason: str) -> None:
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be reversed."))
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
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be resent to Finance."))
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
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be resent to Manager."))
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
        if self.status == self.Status.PAID:
            raise DjangoCoreValidationError(_("Paid reimbursements cannot be force-moved."))
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
            to_status=target_status,
            extra={"type": "admin_force_move"},
        )


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
        MANAGER_PENDING = "manager_pending", _("Pending Manager Approval")
        MANAGER_REJECTED = "manager_rejected", _("Manager Rejected")
        MANAGER_APPROVED = "manager_approved", _("Manager Approved")
        PAID = "paid", _("Paid")

    request = models.ForeignKey(ReimbursementRequest, on_delete=models.CASCADE, related_name="lines")
    expense_item = models.ForeignKey(ExpenseItem, on_delete=models.PROTECT, related_name="request_lines")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    description = models.TextField(blank=True, default="")
    receipt_file = models.FileField(upload_to=receipt_upload_path, validators=[validate_receipt_file], blank=True, null=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.INCLUDED, db_index=True)

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

    payment_reference = models.CharField(max_length=255, blank=True, default="")
    paid_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["id"]
        indexes = [
            models.Index(fields=["request", "status"]),
            models.Index(fields=["request", "bill_status"]),
            models.Index(fields=["expense_item"]),
            models.Index(fields=["bill_status", "updated_at"]),
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

        if self.status == self.Status.INCLUDED:
            desc = (self.description or "").strip()
            exp_desc = (self.expense_item.description or "").strip() if self.expense_item_id else ""
            if not (desc or exp_desc):
                raise DjangoCoreValidationError({"description": _("Bill description is required for every bill.")})

    def save(self, *args, **kwargs):
        """
        FIX (Issues #5, #7, #10):

        #5 — Field auto-population from ExpenseItem was running unconditionally,
             including on targeted `save(update_fields=[...])` calls. This mutated
             the in-memory object (amount/description/receipt_file) without
             persisting those changes, leaving callers with a stale object.
             Now guarded behind `not updating_specific_fields`.

        #7 — The original called apply_derived_status_from_bills() BOTH here
             (synchronously) AND in the post_save signal (on_commit). This caused
             double derivation and meant recalc_total was only called on commit,
             leaving total_amount stale within the transaction.
             Now: recalc_total + apply_derived_status_from_bills run inline here.
             The post_save signal (_recalc_parent_on_line_change) is reduced to
             a no-op; Sheets sync is handled by the ReimbursementRequest post_save
             signal triggered when the parent is saved.

        #10 — full_clean() was called on every save(), including status-only
              update_fields saves. Each full_clean() fires a cross-table duplicate-
              expense-item DB query. Now skipped for targeted field updates.
        """
        updating_specific_fields = kwargs.get("update_fields") is not None
        update_fields_set = set(kwargs["update_fields"]) if updating_specific_fields else None
        creating = self.pk is None

        # --- Auto-populate from ExpenseItem (full saves only) ---
        if self.expense_item_id and not updating_specific_fields:
            if not self.amount:
                self.amount = self.expense_item.amount
            if not (self.description or "").strip():
                self.description = self.expense_item.description
            if not self.receipt_file:
                self.receipt_file = self.expense_item.receipt_file

        # --- Validation (skip on targeted field-only saves) ---
        if not updating_specific_fields:
            self.full_clean()

        # --- Determine whether parent re-derivation is needed ---
        # For targeted updates: re-derive only if bill_status or status is in the fields being updated.
        # For full saves: compare against prior DB state to detect changes.
        status_trigger_fields = {"bill_status", "status"}
        if updating_specific_fields:
            should_rederive = bool(update_fields_set & status_trigger_fields)
        else:
            old_bill_status = None
            old_line_status = None
            if not creating:
                try:
                    prev = type(self).objects.only("bill_status", "status").get(pk=self.pk)
                    old_bill_status = prev.bill_status
                    old_line_status = prev.status
                except type(self).DoesNotExist:
                    creating = True
            should_rederive = creating or (
                (old_bill_status is not None and self.bill_status != old_bill_status)
                or (old_line_status is not None and self.status != old_line_status)
            )

        super().save(*args, **kwargs)

        # --- Synchronously re-derive parent status and recalc totals ---
        try:
            if should_rederive and self.request_id:
                parent = self.request
                if not parent.is_final:
                    parent.recalc_total(save=True)
                    parent.apply_derived_status_from_bills(
                        actor=self.last_modified_by,
                        reason="Auto-derive after bill line change.",
                    )
        except Exception:
            logger.exception(
                "Failed to auto-derive parent status on line save (line_id=%s, request_id=%s).",
                self.pk, self.request_id,
            )

    # ---- Finance actions ----
    def approve_by_finance(self, *, actor: Optional[models.Model]) -> None:
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

        exp = self.expense_item
        exp.status = ExpenseItem.Status.SAVED
        exp.save(update_fields=["status", "updated_at"])

        try:
            try:
                from .services import notifications as _notif
                if hasattr(_notif, "send_bill_rejected_by_finance"):
                    _notif.send_bill_rejected_by_finance(self.request, self)
                else:
                    raise ImportError
            except Exception:
                from .emails import send_bill_rejected_by_finance as _legacy_send_bill_rejected_by_finance
                _legacy_send_bill_rejected_by_finance(self.request, self)
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

    # ---- Employee actions ----
    def employee_resubmit_bill(self, *, actor: Optional[models.Model]) -> None:
        """
        When employee resubmits a corrected bill, the line record must reflect the
        latest ExpenseItem values (amount/description/receipt), otherwise UI keeps
        showing stale line.amount.
        """
        if self.bill_status not in (self.BillStatus.FINANCE_REJECTED, self.BillStatus.MANAGER_REJECTED):
            raise DjangoCoreValidationError(_("Only rejected bills can be resubmitted by employee."))
        prev = self.bill_status

        exp = self.expense_item

        # Always sync latest editable fields from ExpenseItem into the SAME line record.
        if exp:
            self.amount = exp.amount
            self.description = exp.description
            if getattr(exp, "receipt_file", None):
                self.receipt_file = exp.receipt_file

        self.bill_status = self.BillStatus.EMPLOYEE_RESUBMITTED
        self.last_modified_by = actor if isinstance(actor, models.Model) else None

        update_fields = ["bill_status", "last_modified_by", "updated_at", "amount", "description"]
        if exp and getattr(exp, "receipt_file", None):
            update_fields.append("receipt_file")

        self.save(update_fields=update_fields)

        try:
            if exp and exp.status != ExpenseItem.Status.SUBMITTED:
                exp.status = ExpenseItem.Status.SUBMITTED
                exp.save(update_fields=["status", "updated_at"])
        except Exception:
            pass

        try:
            try:
                from .services import notifications as _notif
                if hasattr(_notif, "send_bill_resubmitted"):
                    _notif.send_bill_resubmitted(self.request, self, actor=actor)
                else:
                    raise ImportError
            except Exception:
                from .emails import send_bill_resubmitted as _legacy_send_bill_resubmitted
                _legacy_send_bill_resubmitted(self.request, self, actor=actor)
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

    def manager_approve(self, *, actor: Optional[models.Model]) -> None:
        if self.bill_status != self.BillStatus.MANAGER_PENDING:
            raise DjangoCoreValidationError(_("Only Manager-Pending bills can be approved by Manager."))
        prev = self.bill_status
        self.bill_status = self.BillStatus.MANAGER_APPROVED
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=["bill_status", "last_modified_by", "updated_at"])

        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.MANAGER_APPROVED,
            actor=actor,
            message=f"Manager approved bill line #{self.pk}.",
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "type": "bill_manager_approved"},
        )

    def manager_reject(self, *, actor: Optional[models.Model], reason: str = "") -> None:
        if self.bill_status != self.BillStatus.MANAGER_PENDING:
            raise DjangoCoreValidationError(_("Only Manager-Pending bills can be rejected by Manager."))
        prev = self.bill_status
        self.bill_status = self.BillStatus.MANAGER_REJECTED
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=["bill_status", "last_modified_by", "updated_at"])

        try:
            exp = self.expense_item
            exp.status = ExpenseItem.Status.SAVED
            exp.save(update_fields=["status", "updated_at"])
        except Exception:
            pass

        try:
            from .emails import send_bill_rejected_by_manager
            send_bill_rejected_by_manager(self.request, self, reason=reason)
        except Exception:
            logger.exception("Failed to send bill-rejected-by-manager email: req=%s line=%s", self.request_id, self.pk)

        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=actor,
            message=f"Manager rejected bill line #{self.pk}. {('Reason: ' + reason) if reason else ''}".strip(),
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "type": "bill_manager_rejected"},
        )

    def mark_paid(self, reference: str, *, actor: Optional[models.Model] = None) -> None:
        if self.bill_status not in (self.BillStatus.FINANCE_APPROVED, self.BillStatus.MANAGER_APPROVED):
            raise DjangoCoreValidationError(_("Only Finance-Approved bills can be marked Paid."))
        if not (reference or "").strip():
            raise DjangoCoreValidationError(_("Payment reference is required to mark Paid."))

        prev = self.bill_status
        self.bill_status = self.BillStatus.PAID
        self.payment_reference = reference.strip()
        self.paid_at = timezone.now()
        self.last_modified_by = actor if isinstance(actor, models.Model) else None
        self.save(update_fields=["bill_status", "payment_reference", "paid_at", "last_modified_by", "updated_at"])

        try:
            from .emails import send_bill_paid
            send_bill_paid(self.request, self)
        except Exception:
            logger.exception("Failed to send bill-paid email: req=%s line=%s", self.request_id, self.pk)

        ReimbursementLog.log(
            self.request,
            ReimbursementLog.Action.PAID,
            actor=actor,
            message=f"Bill line #{self.pk} marked Paid with reference {self.payment_reference!r}",
            from_status=prev,
            to_status=self.bill_status,
            extra={"line_id": self.pk, "type": "bill_paid"},
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
        request: "ReimbursementRequest",
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
        verbose_name_plural = "Legacy Legacy Reimbursements"

    def __str__(self) -> str:  # pragma: no cover
        return f"Legacy Reimbursement #{self.pk} – {self.employee} – {self.amount}"