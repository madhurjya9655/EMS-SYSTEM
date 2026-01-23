# apps/reimbursement/services/__init__.py

from django.db.models import Q
from ..models import ReimbursementRequest, ReimbursementLine

# Re-export notifications so existing imports keep working
try:
    from .notifications import (  # noqa: F401
        send_reimbursement_finance_verify,
        send_reimbursement_finance_verified,
        send_reimbursement_finance_rejected,
        send_reimbursement_submitted,
        send_reimbursement_admin_summary,
        send_reimbursement_final_notification,
        send_reimbursement_manager_action,
        send_reimbursement_management_action,
        send_reimbursement_paid,
        send_bill_rejected_by_finance,
        send_bill_resubmitted,
    )
except Exception:
    pass


def qs_finance_verification_queue():
    """
    Finance — Verification Queue:
    Requests that have at least one INCLUDED bill in SUBMITTED or EMPLOYEE_RESUBMITTED.
    """
    L = ReimbursementLine
    R = ReimbursementRequest
    return (
        R.objects.filter(
            lines__status=L.Status.INCLUDED,
            lines__bill_status__in=[L.BillStatus.SUBMITTED, L.BillStatus.EMPLOYEE_RESUBMITTED],
        )
        .exclude(status=R.Status.PAID)
        .distinct()
        .order_by("-updated_at")
    )


def qs_finance_rejected_bills_queue():
    """
    Finance — Rejected Bills:
    Bill lines that are INCLUDED and FINANCE_REJECTED.
    """
    L = ReimbursementLine
    return (
        L.objects.filter(
            status=L.Status.INCLUDED,
            bill_status=L.BillStatus.FINANCE_REJECTED,
        )
        .select_related("request", "expense_item")
        .order_by("-updated_at")
    )


def qs_finance_resubmitted_bills_queue():
    """
    Finance — Resubmitted Bills:
    Bill lines that are INCLUDED and EMPLOYEE_RESUBMITTED.
    """
    L = ReimbursementLine
    return (
        L.objects.filter(
            status=L.Status.INCLUDED,
            bill_status=L.BillStatus.EMPLOYEE_RESUBMITTED,
        )
        .select_related("request", "expense_item")
        .order_by("-updated_at")
    )


def qs_finance_settlement_queue():
    """
    Finance — Settlement Queue:
    Requests that are in Finance settlement stage after approvals.
    """
    R = ReimbursementRequest
    return (
        R.objects.filter(status__in=[R.Status.PENDING_FINANCE, R.Status.APPROVED])
        .exclude(status=R.Status.PAID)
        .order_by("-updated_at")
    )


__all__ = [
    # Queues
    "qs_finance_verification_queue",
    "qs_finance_rejected_bills_queue",
    "qs_finance_resubmitted_bills_queue",
    "qs_finance_settlement_queue",
    # Notifications (if available)
    "send_reimbursement_finance_verify",
    "send_reimbursement_finance_verified",
    "send_reimbursement_finance_rejected",
    "send_reimbursement_submitted",
    "send_reimbursement_admin_summary",
    "send_reimbursement_final_notification",
    "send_reimbursement_manager_action",
    "send_reimbursement_management_action",
    "send_reimbursement_paid",
    "send_bill_rejected_by_finance",
    "send_bill_resubmitted",
]
