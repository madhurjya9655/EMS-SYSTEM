# apps/reimbursement/signals.py
from __future__ import annotations

import logging

from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver

from .integrations.sheets import sync_request
from .models import ReimbursementRequest

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
