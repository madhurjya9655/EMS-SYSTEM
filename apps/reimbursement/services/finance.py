# apps/reimbursement/services/finance.py
from __future__ import annotations

from dataclasses import dataclass

from django.db import transaction

from apps.reimbursement.models import (
    ExpenseItem,
    ReimbursementLine,
    ReimbursementRequest,
)
from .. import emails as email_svc


@dataclass(frozen=True)
class BillActionResult:
    request_id: int
    line_id: int
    new_bill_status: str
    parent_status_after: str


def _recalc_request_and_audit(req: ReimbursementRequest, *, actor, reason: str) -> None:
    """
    Recalculate derived status from child bills and persist with audit.
    """
    req.apply_derived_status_from_bills(actor=actor, reason=reason)


@transaction.atomic
def finance_approve_bill(line: ReimbursementLine, *, actor) -> BillActionResult:
    """
    Approve only this bill. No changes to siblings.
    - Does not send emails (email is only required on rejection/resubmit per spec).
    - Re-derive parent status.
    """
    req = line.request
    line.approve_by_finance(actor=actor)

    # Keep the ExpenseItem locked once a line exists on a request
    if line.expense_item.status != ExpenseItem.Status.SUBMITTED:
        line.expense_item.status = ExpenseItem.Status.SUBMITTED
        line.expense_item.save(update_fields=["status", "updated_at"])

    _recalc_request_and_audit(req, actor=actor, reason=f"Bill #{line.pk} finance-approved.")
    return BillActionResult(req.id, line.id, line.bill_status, req.status)


@transaction.atomic
def finance_reject_bill(line: ReimbursementLine, *, actor, reason: str) -> BillActionResult:
    """
    Reject only this bill with mandatory reason.
    - Unlock underlying ExpenseItem for employee edits.
    - Email employee ONLY (not manager).
    - Re-derive parent status.
    """
    req = line.request
    line.reject_by_finance(actor=actor, reason=reason)

    # Notify employee about the single-bill rejection
    email_svc.send_bill_rejected_by_finance(req, line)

    _recalc_request_and_audit(req, actor=actor, reason=f"Bill #{line.pk} finance-rejected.")
    return BillActionResult(req.id, line.id, line.bill_status, req.status)


@transaction.atomic
def employee_resubmitted_bill(line: ReimbursementLine, *, actor) -> BillActionResult:
    """
    Employee corrected a previously rejected bill and re-submitted.
    - Email finance team with change summary.
    - Re-derive parent status.
    """
    req = line.request
    line.employee_resubmit_bill(actor=actor)

    # When the employee resubmits, the ExpenseItem becomes SUBMITTED again
    if line.expense_item.status != ExpenseItem.Status.SUBMITTED:
        line.expense_item.status = ExpenseItem.Status.SUBMITTED
        line.expense_item.save(update_fields=["status", "updated_at"])

    email_svc.send_bill_resubmitted(req, line, actor=actor)

    _recalc_request_and_audit(req, actor=actor, reason=f"Bill #{line.pk} employee-resubmitted.")
    return BillActionResult(req.id, line.id, line.bill_status, req.status)
