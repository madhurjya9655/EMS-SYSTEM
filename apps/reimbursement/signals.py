# FILE: apps/reimbursement/signals.py
# UPDATED: 2026-03-10
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
    and set a short-lived cache key to throttle subsequent calls.
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
    FIX (Issue #7): Parent recalc_total and apply_derived_status_from_bills are now
    called SYNCHRONOUSLY inside ReimbursementLine.save() (for every save that changes
    bill_status or line status). Running the same logic here on_commit was a second
    duplicate execution that:
      - Caused double status derivation per line save.
      - Left total_amount stale within the originating transaction (signal runs after
        commit, so any code in the same transaction reading req.total_amount would
        see the old value).

    The Sheets sync for the parent is handled by _sync_req_on_save which fires
    automatically whenever the parent ReimbursementRequest is saved (which recalc_total
    and apply_derived_status_from_bills both trigger via their own save() calls).

    This handler is therefore intentionally left as a no-op for save events.
    It is kept registered (rather than removed) to make the architecture explicit and
    to act as a hook for future line-specific side-effects that must run post-commit.
    """
    pass


@receiver(post_delete, sender=ReimbursementLine)
def _recalc_parent_on_line_delete(sender, instance: ReimbursementLine, **kwargs):
    """
    When a bill line is deleted, recalc totals and re-derive parent status.

    DELETE does not go through ReimbursementLine.save(), so the inline recalc
    logic in save() is not triggered. We must handle it here on_commit.

    Sheets sync is triggered indirectly: recalc_total and apply_derived_status_from_bills
    both save the parent, which fires _sync_req_on_save.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)
        except ReimbursementRequest.DoesNotExist:
            return

        try:
            req.recalc_total(save=True)
        except Exception:
            logger.debug(
                "Unable to recalc total for request %s after line %s delete.",
                req.pk, instance.pk,
            )

        try:
            req.apply_derived_status_from_bills(
                actor=None,
                reason=f"Bill line #{instance.pk} deleted; re-deriving parent status.",
            )
        except Exception:
            logger.exception(
                "Unable to derive parent status for request %s after line %s delete.",
                req.pk, instance.pk,
            )

    transaction.on_commit(_do)