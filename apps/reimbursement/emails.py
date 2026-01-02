# apps/reimbursement/emails.py
from __future__ import annotations

from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.conf import settings

from .models import ReimbursementRequest, ReimbursementLine, ReimbursementLog, ReimbursementSettings


def _send(to_list: list[str], subject: str, template_base: str, context: dict) -> None:
    if not to_list:
        return
    html = render_to_string(f"email/{template_base}.html", context)
    txt = render_to_string(f"email/{template_base}.txt", context)
    msg = EmailMultiAlternatives(subject=subject, body=txt, to=to_list)
    msg.attach_alternative(html, "text/html")
    msg.send(fail_silently=True)


def _employee_email(req: ReimbursementRequest) -> list[str]:
    email = (getattr(req.created_by, "email", "") or "").strip()
    return [email] if email else []


def _finance_emails() -> list[str]:
    return ReimbursementSettings.get_solo().finance_email_list()


def send_bill_rejected_by_finance(req: ReimbursementRequest, line: ReimbursementLine) -> None:
    """
    1️⃣ Finance rejects a single bill — email ONLY the employee with bill details and reason.
    """
    subject = f"Reimbursement #{req.id}: One bill was rejected by Finance"
    ctx = {
        "employee_name": (req.created_by.get_full_name() or req.created_by.username),
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "rejection_reason": line.finance_rejection_reason or "-",
        "detail_url": "",  # fill if you have named route; kept blank to avoid coupling
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }
    _send(_employee_email(req), subject, "reimbursement_bill_rejected_by_finance", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=None,
        message=f"Email: bill #{line.id} rejected by finance sent to employee.",
        extra={"line_id": line.id, "template": "reimbursement_bill_rejected_by_finance"},
    )


def send_bill_resubmitted(req: ReimbursementRequest, line: ReimbursementLine, *, actor) -> None:
    """
    2️⃣ Employee edits/replaces a previously rejected bill — email Finance team.
    """
    subject = f"Reimbursement #{req.id}: Employee resubmitted a corrected bill"
    ctx = {
        "employee_name": (req.created_by.get_full_name() or req.created_by.username),
        "employee_email": getattr(req.created_by, "email", "") or "-",
        "request_id": req.id,
        "bill_id": line.id,
        "bill_amount": f"{line.amount:.2f}",
        "bill_description": line.description or "-",
        "resubmitted_by": getattr(actor, "get_full_name", lambda: "")() or getattr(actor, "username", ""),
        "detail_url": "",
        "status_label": dict(ReimbursementRequest.Status.choices).get(req.status, req.status),
    }
    _send(_finance_emails(), subject, "reimbursement_bill_resubmitted", ctx)
    ReimbursementLog.log(
        req,
        ReimbursementLog.Action.EMAIL_SENT,
        actor=actor,
        message=f"Email: bill #{line.id} resubmitted sent to finance.",
        extra={"line_id": line.id, "template": "reimbursement_bill_resubmitted"},
    )
