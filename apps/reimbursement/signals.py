# -*- coding: utf-8 -*-
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
            sync_request(req)
        except Exception:
            logger.exception("Sheets sync scheduling failed for ReimbursementRequest %s", instance.pk)

    transaction.on_commit(_do)


@receiver(post_save, sender=ReimbursementLine)
def _recalc_parent_on_line_change(sender, instance: ReimbursementLine, created, **kwargs):
    """
    Keep parent status in sync with bill changes, but DO NOT auto-send to
    Manager here. Escalation happens from the FinanceVerifyView when the
    user clicks Approve/Finalize.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)
            prev_status = req.status
            # Re-derive only; let the view decide whether to email/escalate.
            req.apply_derived_status_from_bills(
                actor=getattr(instance, "last_modified_by", None),
                reason=f"Bill line #{instance.pk} updated; re-deriving parent status.",
            )
            if prev_status != req.status:
                logger.info(
                    "Reimbursement %s status adjusted from %s to %s after line update.",
                    req.pk, prev_status, req.status
                )
        except Exception:
            logger.exception("Unable to derive parent status for request %s after line %s save.",
                             instance.request_id, instance.pk)

    transaction.on_commit(_do)
