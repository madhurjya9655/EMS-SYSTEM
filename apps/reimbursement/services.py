# apps/reimbursement/services.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable, Tuple

from django.db import transaction
from django.db.models import Q, Count, F

from .models import ReimbursementRequest, ReimbursementLine, ReimbursementLog


@dataclass(frozen=True)
class Status:
    PENDING_FINANCE_VERIFY = "pending_finance_verify"
    PENDING_MANAGER = "pending_manager"
    MANAGER_APPROVED = "manager_approved"
    MANAGER_REJECTED = "manager_rejected"
    FINANCE_APPROVED = "finance_approved"   # historically used; we never jump back to this from manager
    PAID = "paid"
    SETTLED = "settled"


# ---- STRICT, NON-NEGOTIABLE RULES ----
# - Finance decisions are bill-level only
# - Request moves to Manager ONLY when all included bills are finance-approved
# - No "partial_hold" at request level
# - Rejected bills do not reject the request; only those bills go back to employee
# - UI must show only actionable items per role

BACKWARD_BLOCKS = {
    # block regressing a manager-eligible/approved request back to finance screens automatically
    ("pending_manager", "pending_finance_verify"),
    ("finance_approved", "pending_manager"),
    ("pending_manager", "pending_finance"),
    ("PENDING_MANAGER_APPROVAL", "PENDING_FINANCE_VERIFICATION"),
    ("FINANCE_APPROVED", "PENDING_MANAGER_APPROVAL"),
}


def _all_included_lines_finance_approved(req_id: int) -> bool:
    agg = (
        ReimbursementRequest.objects
        .filter(id=req_id)
        .annotate(
            total=Count("lines", filter=Q(lines__status="included")),
            ok=Count("lines", filter=Q(lines__status="included", lines__bill_status="finance_approved")),
        )
        .values("total", "ok")
        .first()
    )
    if not agg:
        return False
    return agg["total"] > 0 and agg["total"] == agg["ok"]


@transaction.atomic
def apply_derived_status_from_bills(request_id: int, actor=None, reason: str = "") -> ReimbursementRequest:
    """
    Single source of truth for parent status.
    """
    req = ReimbursementRequest.objects.select_for_update().get(id=request_id)

    # Remove legacy/invalid "partial_hold" at request level
    if req.status == "partial_hold":
        old = req.status
        req.status = Status.PENDING_FINANCE_VERIFY
        req.save(update_fields=["status", "updated_at"])
        ReimbursementLog.objects.create(
            request=req, action="status_changed", from_status=old, to_status=req.status, actor=actor, message=reason
        )

    # If any included bill is not finance_approved -> stay in finance verification
    if not _all_included_lines_finance_approved(req.id):
        if req.status != Status.PENDING_FINANCE_VERIFY:
            old = req.status
            req.status = Status.PENDING_FINANCE_VERIFY
            req.save(update_fields=["status", "updated_at"])
            ReimbursementLog.objects.create(
                request=req, action="status_changed", from_status=old, to_status=req.status, actor=actor, message=reason
            )
        return req

    # All included bills are finance_approved -> request becomes manager-pending
    if req.status != Status.PENDING_MANAGER:
        old = req.status
        req.status = Status.PENDING_MANAGER
        req.save(update_fields=["status", "updated_at"])
        ReimbursementLog.objects.create(
            request=req, action="status_changed", from_status=old, to_status=req.status, actor=actor, message=reason
        )
    return req


@transaction.atomic
def transition_request_status(request_id: int, target_status: str, actor=None, **kwargs) -> ReimbursementRequest:
    """
    Transition guard that blocks known-bad backwards moves and defers to derived status when applicable.
    """
    req = ReimbursementRequest.objects.select_for_update().get(id=request_id)

    # Idempotent
    if req.status == target_status:
        ReimbursementLog.objects.create(
            request=req, action="noop_transition", from_status=req.status, to_status=target_status, actor=actor
        )
        return req

    # Block backward flips
    if (req.status, target_status) in BACKWARD_BLOCKS:
        ReimbursementLog.objects.create(
            request=req, action="blocked_backward", from_status=req.status, to_status=target_status, actor=actor
        )
        return req

    # Manager decisions are request-level and final before settlement
    if target_status in {Status.MANAGER_APPROVED, Status.MANAGER_REJECTED}:
        old = req.status
        req.status = Status.PENDING_FINANCE_VERIFY if target_status == Status.MANAGER_REJECTED else Status.MANAGER_APPROVED
        # Note: after MANAGER_APPROVED, Finance will do settlement and mark PAID/SETTLED
        req.save(update_fields=["status", "updated_at"])
        ReimbursementLog.objects.create(
            request=req, action="manager_decision", from_status=old, to_status=req.status, actor=actor
        )
        return req

    # Otherwise allow, but immediately re-derive from bills so parent status matches the rules
    old = req.status
    req.status = target_status
    req.save(update_fields=["status", "updated_at"])
    ReimbursementLog.objects.create(
        request=req, action="status_changed", from_status=old, to_status=req.status, actor=actor
    )
    return apply_derived_status_from_bills(request_id=req.id, actor=actor, reason="post-transition derive")
