# apps/reimbursement/services.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from django.db import transaction
from django.db.models import Q, Count, Exists, OuterRef, F

from .models import (
    ReimbursementRequest,
    ReimbursementLine,
    ReimbursementSettings,
)


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
            ok=Count(
                "lines",
                filter=Q(
                    lines__status=ReimbursementLine.Status.INCLUDED,
                    lines__bill_status=ReimbursementLine.BillStatus.FINANCE_APPROVED,
                ),
            ),
        )
        .values("total", "ok")
        .first()
    )
    if not agg:
        return False
    return agg["total"] > 0 and agg["total"] == agg["ok"]


@transaction.atomic
def apply_derived_status_from_bills(
    request_id: int, *, actor=None, reason: str = ""
) -> ServiceResult:
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
    req.apply_derived_status_from_bills(
        actor=actor, reason=reason or "services.apply_derived_status_from_bills"
    )
    return ServiceResult(request_id=req.id, status=req.status)


@transaction.atomic
def transition_request_status(
    request_id: int, target_status: str, *, actor=None, reason: str = ""
) -> ServiceResult:
    """
    Safe transition wrapper for legacy callers.

    - Defers validation to the model (which blocks illegal moves like changing PAID, etc.).
    - Uses `admin_force_move` for explicit admin/ops transitions (except PAID).
    - Immediately re-derives from bills afterward to keep the parent status honest.
    """
    req = ReimbursementRequest.objects.select_for_update().get(id=request_id)

    # Use the model's admin method (it internally validates targets and forbids setting PAID here)
    req.admin_force_move(
        target_status, actor=actor, reason=reason or "services.transition_request_status"
    )

    # After any explicit move, re-derive so that request matches bill truth
    req.apply_derived_status_from_bills(
        actor=actor, reason="services.post-transition derive"
    )

    return ServiceResult(request_id=req.id, status=req.status)


# ---------------------------------------------------------------------------
# Canonical queue helpers (align UIs to the written workflow spec)
# ---------------------------------------------------------------------------

def qs_finance_verification_queue():
    """
    Finance — Verification Queue

    Requests that contain at least one INCLUDED bill in SUBMITTED or
    EMPLOYEE_RESUBMITTED. Excludes final (paid/rejected) requests.
    """
    L = ReimbursementLine
    R = ReimbursementRequest

    pending_lines = L.objects.filter(
        request_id=OuterRef("pk"),
        status=L.Status.INCLUDED,
        bill_status__in=[L.BillStatus.SUBMITTED, L.BillStatus.EMPLOYEE_RESUBMITTED],
    )

    return (
        R.objects
        .annotate(_has_pending=Exists(pending_lines))
        .filter(_has_pending=True)
        .exclude(status__in=R.final_statuses())
        .order_by("-created_at")
    )


def qs_finance_rejected_bills_queue():
    """
    Finance — Rejected Bills Queue

    Bills that are FINANCE_REJECTED (still with employee). These should *not*
    be in the active finance verification queue until the employee resubmits.
    """
    L = ReimbursementLine

    return (
        L.objects
        .select_related("request", "expense_item")
        .filter(
            status=L.Status.INCLUDED,
            bill_status=L.BillStatus.FINANCE_REJECTED,
            request__status__in=[
                ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
                ReimbursementRequest.Status.PENDING_MANAGER,  # request may be mixed
            ],
        )
        .order_by("id")
    )


def qs_finance_resubmitted_bills_queue():
    """
    Optional: "Rejected & Resubmitted" view

    Bills that were previously rejected and are now EMPLOYEE_RESUBMITTED,
    i.e., back to finance verification.
    """
    L = ReimbursementLine

    return (
        L.objects
        .select_related("request", "expense_item")
        .filter(
            status=L.Status.INCLUDED,
            bill_status=L.BillStatus.EMPLOYEE_RESUBMITTED,
            request__status__in=[
                ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
                ReimbursementRequest.Status.PENDING_MANAGER,  # request may be mixed
            ],
        )
        .order_by("id")
    )


def qs_finance_settlement_queue():
    """
    Finance — Settlement Queue

    Requests eligible for settlement:
      - All INCLUDED bills are FINANCE_APPROVED
      - Approvals satisfied:
          * If require_management_approval=True: management approved
          * Else: manager approved
      - Not paid yet
    """
    L = ReimbursementLine
    R = ReimbursementRequest
    require_mgmt = ReimbursementSettings.get_solo().require_management_approval

    qs = (
        R.objects
        .annotate(
            _total_included=Count("lines", filter=Q(lines__status=L.Status.INCLUDED)),
            _approved_included=Count(
                "lines",
                filter=Q(
                    lines__status=L.Status.INCLUDED,
                    lines__bill_status=L.BillStatus.FINANCE_APPROVED,
                ),
            ),
        )
        .filter(
            _total_included__gt=0,
            _total_included=F("_approved_included"),
        )
        .exclude(status=R.Status.PAID)
    )

    if require_mgmt:
        qs = qs.filter(
            management_decision__iexact="approved",
            management_decided_at__isnull=False,
        )
    else:
        qs = qs.filter(
            manager_decision__iexact="approved",
            manager_decided_at__isnull=False,
        )

    return qs.order_by("-updated_at")
