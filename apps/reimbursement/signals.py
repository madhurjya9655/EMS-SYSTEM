# apps/reimbursement/signals.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging

from django.core.cache import cache
from django.db import transaction
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from .models import ReimbursementRequest, ReimbursementLine

logger = logging.getLogger(__name__)

# Debounce window per request for Sheets sync (seconds)
SYNC_DEBOUNCE_SECONDS = 15


def _should_sync_req(req_id: int) -> bool:
    """
    Return True if we should perform a sync for this request id now,
    and set a short-lived cache key to hold future calls.
    """
    key = f"reimb.req.sync.debounce.{req_id}"
    if cache.get(key):
        return False
    cache.set(key, True, timeout=SYNC_DEBOUNCE_SECONDS)
    return True


@receiver(post_save, sender=ReimbursementRequest)
def _sync_req_on_save(sender, instance: ReimbursementRequest, created, **kwargs):
    """
    Side-effect: schedule export to Google Sheets with a debounce.
    No status changes here.
    """
    def _do():
        try:
            if not _should_sync_req(instance.pk):
                return
            from .integrations.sheets import sync_request  # lazy import
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
    Keep the parent request's derived status in sync with bill-level changes.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)
            prev_status = req.status
            try:
                req.recalc_total(save=True)
            except Exception:
                logger.debug("Unable to recalc total for request %s after line %s save.", req.pk, instance.pk)

            req.apply_derived_status_from_bills(
                actor=getattr(instance, "last_modified_by", None),
                reason=f"Bill line #{instance.pk} updated; re-deriving parent status.",
            )

            if prev_status != req.status:
                logger.info(
                    "Reimbursement %s status adjusted from %s to %s after line update.",
                    req.pk, prev_status, req.status
                )

            # Soft-sync parent (debounced). We only sync the request object, not each line.
            if _should_sync_req(req.pk):
                from .integrations.sheets import sync_request  # lazy import
                # Load the full request with lines for a single export
                full = (
                    ReimbursementRequest.objects.select_related(
                        "created_by", "manager", "management", "verified_by"
                    )
                    .prefetch_related("lines__expense_item")
                    .get(pk=req.pk)
                )
                sync_request(full)
        except Exception:
            logger.exception(
                "Unable to derive parent status for request %s after line %s save.",
                instance.request_id, instance.pk
            )

    transaction.on_commit(_do)


@receiver(post_delete, sender=ReimbursementLine)
def _recalc_parent_on_line_delete(sender, instance: ReimbursementLine, **kwargs):
    """
    When a bill line is deleted, make sure the parent total and holding status are updated.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)
        except ReimbursementRequest.DoesNotExist:
            return

        try:
            req.recalc_total(save=True)
        except Exception:
            logger.debug("Unable to recalc total for request %s after line %s delete.", req.pk, instance.pk)

        try:
            req.apply_derived_status_from_bills(
                actor=None,
                reason=f"Bill line #{instance.pk} deleted; re-deriving parent status.",
            )
        except Exception:
            logger.exception(
                "Unable to derive parent status for request %s after line %s delete.",
                req.pk, instance.pk
            )

        # Debounced export
        try:
            if _should_sync_req(req.pk):
                from .integrations.sheets import sync_request  # lazy import
                full = (
                    ReimbursementRequest.objects.select_related(
                        "created_by", "manager", "management", "verified_by"
                    )
                    .prefetch_related("lines__expense_item")
                    .get(pk=req.pk)
                )
                sync_request(full)
        except Exception:
            logger.exception("Sheets sync scheduling failed for ReimbursementRequest %s", req.pk)

    transaction.on_commit(_do)
