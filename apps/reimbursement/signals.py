# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reimbursement\signals.py
from __future__ import annotations

import logging

from django.db import transaction
from django.db.utils import OperationalError, ProgrammingError
from django.db.models.signals import post_save
from django.dispatch import receiver

from django.conf import settings
from .integrations.sheets import sync_request
from .models import ReimbursementRequest, ReimbursementLine

logger = logging.getLogger(__name__)

# Optional feature flag to silence Sheets export in dev
SHEETS_ENABLED = getattr(settings, "REIMBURSEMENT_SHEETS_ENABLED", False)


@receiver(post_save, sender=ReimbursementRequest)
def _sync_req_on_save(sender, instance: ReimbursementRequest, created, **kwargs):
    """
    Sync to Google Sheets after committed DB changes.
    Read-only: NO mutations, NO status recalculation, NO audit writing.

    Guarded so missing columns (e.g. users_profile.photo) never crash the app.
    """
    if not SHEETS_ENABLED:
        return

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
        except (OperationalError, ProgrammingError) as db_err:
            logger.error("Sheets sync skipped due to DB schema error: %s", db_err)
        except Exception:
            logger.exception("Sheets sync scheduling failed for ReimbursementRequest %s", instance.pk)

    transaction.on_commit(_do)


@receiver(post_save, sender=ReimbursementLine)
def _recalc_parent_on_line_change(sender, instance: ReimbursementLine, created, **kwargs):
    """
    Whenever a bill line changes (including bill_status transitions),
    recompute the parent's derived status from bills and keep totals fresh.
    """
    def _do():
        try:
            req = ReimbursementRequest.objects.get(pk=instance.request_id)
            # keep totals and derived status consistent, but swallow errors
            try:
                req.recalc_total(save=True)
                req.apply_derived_status_from_bills(
                    actor=getattr(instance, "last_modified_by", None),
                    reason=f"Bill line #{instance.pk} updated; re-deriving parent status.",
                )
            except Exception:
                logger.exception("Post-save derive failed for request %s", req.pk)
        except Exception:
            logger.exception(
                "Unable to derive parent status for request %s after line %s save.",
                instance.request_id, instance.pk
            )

    transaction.on_commit(_do)
