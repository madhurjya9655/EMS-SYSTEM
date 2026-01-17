# apps/reimbursement/services.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from django.db import transaction
from django.db.models import Q, Count

from .models import ReimbursementRequest, ReimbursementLine


@dataclass(frozen=True)
class ServiceResult:
    """Lightweight return wrapper for service operations (optional use)."""
    request_id: int
    status: str


def _all_included_lines_finance_approved(req_id: int) -> bool:
    """
    Helper used by some admin workflows/tests. Kept here, but **do not**
    derive parent status with this; always call the model method for that.
    """
    agg = (
        ReimbursementRequest.objects
        .filter(id=req_id)
        .annotate(
            total=Count("lines", filter=Q(lines__status=ReimbursementLine.Status.INCLUDED)),
            ok=Count("lines", filter=Q(
                lines__status=ReimbursementLine.Status.INCLUDED,
                lines__bill_status=ReimbursementLine.BillStatus.FINANCE_APPROVED,
            )),
        )
        .values("total", "ok")
        .first()
    )
    if not agg:
        return False
    return agg["total"] > 0 and agg["total"] == agg["ok"]


@transaction.atomic
def apply_derived_status_from_bills(request_id: int, *, actor=None, reason: str = "") -> ServiceResult:
    """
    Thin wrapper around the authoritative model method.
    Use this in legacy callers that still import from services.py.
    """
    req = (
        ReimbursementRequest.objects
        .select_for_update()
        .get(id=request_id)
    )
    # Delegates all rules + monotonic guard to the model
    req.apply_derived_status_from_bills(actor=actor, reason=reason or "services.apply_derived_status_from_bills")
    return ServiceResult(request_id=req.id, status=req.status)


@transaction.atomic
def transition_request_status(request_id: int, target_status: str, *, actor=None, reason: str = "") -> ServiceResult:
    """
    Safe transition wrapper for legacy callers.

    - Defers validation to the model (which blocks illegal moves like changing PAID, etc.).
    - Uses `admin_force_move` for explicit admin/ops transitions (except PAID).
    - Immediately re-derives from bills afterward to keep the parent status honest.
    """
    req = ReimbursementRequest.objects.select_for_update().get(id=request_id)

    # Use the model's admin method (it internally validates targets and forbids setting PAID here)
    req.admin_force_move(target_status, actor=actor, reason=reason or "services.transition_request_status")

    # After any explicit move, re-derive so that request matches bill truth
    req.apply_derived_status_from_bills(actor=actor, reason="services.post-transition derive")

    return ServiceResult(request_id=req.id, status=req.status)
