# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reimbursement\signals.py
from __future__ import annotations

import logging

from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver

from .integrations.sheets import sync_request
from .models import ReimbursementRequest, ReimbursementLine

logger = logging.getLogger(__name__)


@receiver(post_save, sender=ReimbursementRequest)
def _sync_req_on_save(sender, instance: ReimbursementRequest, created, **kwargs):
    """
    Sync to Google Sheets after committed DB changes.
    Read-only: NO mutations, NO status recalculation, NO audit writing.
    """
    def _do():
        try:
            req = (
                ReimbursementRequest.objects.select_related(
                    "created_by", "manager", "management", "verified_by"
                )
                .prefetch_related("lines__expense_item")
                .get(pk=instance.pk)
            )
            sync_request(req)  # strictly export; no DB writes back
        except Exception:
            logger.exception("Sheets sync scheduling failed for ReimbursementRequest %s", instance.pk)

    transaction.on_commit(_do)


@receiver(post_save, sender=ReimbursementLine)
def _recalc_parent_on_line_change(sender, instance: ReimbursementLine, created, **kwargs):
    """
    Whenever a bill line changes (including bill_status transitions),
    recompute the parent's derived status from bills.
    If that recalculation makes the parent reach PENDING_MANAGER
    (i.e., all bills finance-approved), automatically send the
    manager approval email with Approve/Reject buttons.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)

            # Re-derive parent status from bill-level changes
            prev_status = req.status
            req.apply_derived_status_from_bills(
                actor=getattr(instance, "last_modified_by", None),
                reason=f"Bill line #{instance.pk} updated; re-deriving parent status.",
            )

            # If we just transitioned to Pending Manager, trigger manager email.
            if prev_status != req.status and req.status == ReimbursementRequest.Status.PENDING_MANAGER:
                try:
                    from .services import notifications
                    # Send the same "verify → approver" mail used by request-level verify,
                    # which includes Approve / Reject buttons (no login).
                    notifications.send_reimbursement_finance_verified(req)
                except Exception:
                    logger.exception("Failed to send manager approval email for request %s after all bills approved.", req.pk)

        except Exception:
            logger.exception("Unable to derive parent status for request %s after line %s save.",
                             instance.request_id, instance.pk)

    transaction.on_commit(_do)
