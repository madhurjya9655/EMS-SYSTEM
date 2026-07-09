# apps/vendor/services.py
from __future__ import annotations

import logging
import os

from django.conf import settings
from django.core.files.storage import default_storage
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.urls import reverse

from .models import VendorApprovalConfig

logger = logging.getLogger(__name__)


def _absolute_file_url(request, file_field) -> str:
    """
    Build a full public URL for a FileField.

    Example:
    /media/vendor_payments/invoices/file.pdf

    becomes:
    https://ems-system-d26q.onrender.com/media/vendor_payments/invoices/file.pdf
    """
    if not file_field:
        return ""

    try:
        if not getattr(file_field, "name", ""):
            return ""

        return request.build_absolute_uri(file_field.url)

    except Exception:
        logger.exception(
            "Unable to build absolute URL for file field: %s",
            file_field,
        )
        return ""


def _file_display_name(file_field) -> str:
    """
    Return only the uploaded file name from FileField path.

    Example:
    vendor_payments/invoices/2026/06/invoice.pdf

    becomes:
    invoice.pdf
    """
    if not file_field:
        return ""

    file_name = getattr(file_field, "name", "")

    if not file_name:
        return ""

    return file_name.split("/")[-1]


def _user_display_name(user) -> str:
    """
    Return the safest display name for a Django user.
    """
    if not user:
        return ""

    try:
        return user.get_full_name() or user.username or user.email or ""
    except Exception:
        return ""


def _attach_file_if_possible(email, file_field, label: str) -> None:
    """
    Attach file physically to the email when local file path is available.

    This works with local media storage / Render persistent disk.

    If storage is cloud-based later, file_field.path may not be available.
    In that case, the HTML email still provides clickable file URLs.
    """
    if not file_field:
        return

    try:
        file_name = getattr(file_field, "name", "")

        if not file_name:
            return

        if not default_storage.exists(file_name):
            logger.warning(
                "Skipping %s attachment because file does not exist in storage: %s",
                label,
                file_name,
            )
            return

        try:
            file_path = file_field.path
        except Exception:
            file_path = ""

        if file_path and os.path.exists(file_path):
            email.attach_file(file_path)
            return

        logger.warning(
            "Skipping physical email attachment for %s because local file path is unavailable. "
            "Attachment URL will still be included if available. File name: %s",
            label,
            file_name,
        )

    except Exception:
        logger.exception(
            "Unable to physically attach %s to vendor payment email",
            label,
        )


def _get_invoice_queryset(obj):
    """
    Safely return child invoice rows for a VendorPaymentRequest.

    The try/except keeps old deployments safe during staged migration,
    where the relation may not exist yet.
    """
    try:
        return list(obj.invoices.all())
    except Exception:
        return []


def _build_invoice_context(request, obj) -> list[dict]:
    """
    Build invoice context for email templates.

    New multi-invoice path:
    - Uses obj.invoices.all()

    Legacy fallback:
    - If no child invoice rows exist, but old parent invoice fields exist,
      build one invoice-like dictionary from parent fields.
    """
    invoice_rows = _get_invoice_queryset(obj)

    if invoice_rows:
        invoices = []

        for invoice in invoice_rows:
            attachment = getattr(invoice, "invoice_attachment", None)

            invoices.append(
                {
                    "obj": invoice,
                    "invoice_number": getattr(invoice, "invoice_number", "") or "",
                    "invoice_date": getattr(invoice, "invoice_date", None),
                    "bill_type": (
                        invoice.get_bill_type_display()
                        if hasattr(invoice, "get_bill_type_display")
                        else str(getattr(invoice, "bill_type", "") or "")
                    ),
                    "base_amount": getattr(invoice, "base_amount", 0),
                    "gst_amount": getattr(invoice, "gst_amount", 0),
                    "total_amount": getattr(invoice, "total_amount", 0),
                    "description": getattr(invoice, "description", "") or "",
                    "invoice_attachment": attachment,
                    "invoice_url": _absolute_file_url(request, attachment),
                    "invoice_name": _file_display_name(attachment),
                }
            )

        return invoices

    # Legacy fallback for old one-invoice records.
    if getattr(obj, "invoice_number", ""):
        attachment = getattr(obj, "attachment", None)

        return [
            {
                "obj": obj,
                "invoice_number": getattr(obj, "invoice_number", "") or "",
                "invoice_date": getattr(obj, "invoice_date", None),
                "bill_type": (
                    obj.get_bill_type_display()
                    if hasattr(obj, "get_bill_type_display")
                    else str(getattr(obj, "bill_type", "") or "")
                ),
                "base_amount": getattr(obj, "base_amount", 0),
                "gst_amount": getattr(obj, "gst_amount", 0),
                "total_amount": getattr(obj, "total_amount", 0),
                "description": getattr(obj, "description", "") or "",
                "invoice_attachment": attachment,
                "invoice_url": _absolute_file_url(request, attachment),
                "invoice_name": _file_display_name(attachment),
            }
        ]

    return []


def _payment_total(obj):
    """
    Return request total safely.

    New requests:
    - obj.payment_total / obj.grand_total

    Old requests:
    - obj.total_amount
    """
    try:
        return obj.payment_total
    except Exception:
        pass

    return getattr(obj, "grand_total", None) or getattr(obj, "total_amount", 0)


def _invoice_count(obj, invoices: list[dict]) -> int:
    """
    Return invoice count safely.
    """
    try:
        return obj.invoice_count
    except Exception:
        return len(invoices)


def _invoice_amount_totals(invoices: list[dict]) -> dict:
    """
    Calculate display totals from the invoice context already loaded for email.
    """
    base_total = 0
    gst_total = 0
    total_amount = 0

    for invoice in invoices:
        base_total += invoice.get("base_amount") or 0
        gst_total += invoice.get("gst_amount") or 0
        total_amount += invoice.get("total_amount") or 0

    return {
        "base_total": base_total,
        "gst_total": gst_total,
        "invoice_total": total_amount,
    }


def _build_common_email_context(request, obj, *, review_url: str = "") -> dict:
    """
    Build the complete request context used by vendor payment emails.

    This intentionally reads from the original VendorPaymentRequest and its
    related invoice rows instead of reconstructing values from a lossy summary.
    """
    invoices = _build_invoice_context(request, obj)

    bank_attachment = getattr(obj, "bank_attachment", None)
    bank_attachment_url = _absolute_file_url(request, bank_attachment)
    bank_attachment_name = _file_display_name(bank_attachment)

    submitted_by = ""
    if getattr(obj, "created_by_id", None):
        submitted_by = _user_display_name(getattr(obj, "created_by", None))

    finance_verified_by = ""
    if getattr(obj, "finance_approved_by_id", None):
        finance_verified_by = _user_display_name(getattr(obj, "finance_approved_by", None))

    approved_by = ""
    if getattr(obj, "final_approved_by_id", None):
        approved_by = _user_display_name(getattr(obj, "final_approved_by", None))

    totals = _invoice_amount_totals(invoices)
    grand_total = _payment_total(obj)
    invoice_count = _invoice_count(obj, invoices)

    return {
        "obj": obj,
        "review_url": review_url,
        "site_url": request.build_absolute_uri("/"),
        "submitted_by": submitted_by,
        "requested_date": getattr(obj, "created_at", None),
        "invoices": invoices,
        "invoice_count": invoice_count,
        "base_total": totals["base_total"],
        "gst_total": totals["gst_total"],
        "invoice_total": totals["invoice_total"],
        "grand_total": grand_total,
        "bank_attachment": bank_attachment,
        "bank_attachment_url": bank_attachment_url,
        "bank_attachment_available": bool(bank_attachment),
        "bank_attachment_name": bank_attachment_name,
        "finance_verified_by": finance_verified_by,
        "approved_by": approved_by,
        "approved_on": getattr(obj, "updated_at", None),
    }


def _attach_vendor_payment_files(email, context: dict) -> None:
    """
    Attach every uploaded file currently stored on the approved request.

    Current model storage:
    - Child invoice files: VendorPaymentInvoice.invoice_attachment
    - Legacy invoice file: VendorPaymentRequest.attachment via invoice context
    - Bank proof file: VendorPaymentRequest.bank_attachment
    """
    for invoice in context.get("invoices", []):
        _attach_file_if_possible(
            email=email,
            file_field=invoice.get("invoice_attachment"),
            label=f"invoice attachment {invoice.get('invoice_number') or ''}".strip(),
        )

    _attach_file_if_possible(
        email=email,
        file_field=context.get("bank_attachment"),
        label="bank details attachment",
    )


def send_vendor_payment_submission_email(request, obj) -> None:
    """
    Send professional HTML vendor payment approval email to finance approvers.

    Multi-invoice behavior:
    - Sends ONE email for ONE payment request.
    - Shows vendor once.
    - Shows total invoice count.
    - Shows grand total.
    - Shows invoice summary table in HTML template.
    - Shows invoice summary list in TXT template.
    - Attaches all invoice files where local path is available.
    - Keeps bank attachment at parent request level.

    Backward compatibility:
    - If child invoices do not exist yet, old parent invoice fields are used.
    """
    config = VendorApprovalConfig.get_config()
    to_list = config.get_finance_email_list()

    if not to_list:
        logger.warning(
            "Vendor payment submission email skipped for request pk=%s because finance email list is empty",
            getattr(obj, "pk", None),
        )
        return

    subject = f"Vendor Payment Approval Required - {obj.request_id}"

    review_url = request.build_absolute_uri(
        reverse("vendor:detail", kwargs={"pk": obj.pk})
    )

    context = _build_common_email_context(request, obj, review_url=review_url)

    text_body = render_to_string(
        "email/vendor_payment_approval.txt",
        context,
    )

    html_body = render_to_string(
        "email/vendor_payment_approval.html",
        context,
    )

    try:
        email = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_list,
        )

        email.attach_alternative(html_body, "text/html")
        _attach_vendor_payment_files(email, context)

        email.send(fail_silently=False)

        logger.info(
            "Vendor payment submission email sent successfully for request pk=%s request_id=%s invoice_count=%s grand_total=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
            context.get("invoice_count"),
            context.get("grand_total"),
        )

    except Exception:
        logger.exception(
            "Vendor payment submission email failed for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )


def send_vendor_payment_manager_approval_email(request, obj) -> None:
    """
    Notify the assigned manager/senior authority after finance verification.

    This preserves the existing approval workflow and recipients. It only enriches
    the email body with the verified finance manager stored on the request.
    """
    config = VendorApprovalConfig.get_config()

    if not config.senior_authority or not config.senior_authority.email:
        return

    subject = f"Vendor Payment Ready for Final Approval - {obj.request_id}"

    review_url = request.build_absolute_uri(
        reverse("vendor:detail", kwargs={"pk": obj.pk})
    )

    context = _build_common_email_context(request, obj, review_url=review_url)

    text_body = render_to_string(
        "email/vendor_payment_approval.txt",
        context,
    )

    html_body = render_to_string(
        "email/vendor_payment_approval.html",
        context,
    )

    try:
        email = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[config.senior_authority.email],
        )

        email.attach_alternative(html_body, "text/html")
        _attach_vendor_payment_files(email, context)
        email.send(fail_silently=False)

        logger.info(
            "Vendor manager approval email sent successfully for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )

    except Exception:
        logger.exception(
            "Vendor manager approval email failed for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )


def send_vendor_payment_final_approval_email(request, obj) -> None:
    """
    Notify Mumbai/accounts team after final manager approval.

    The email context is built from the original approved VendorPaymentRequest,
    including all related invoice rows and stored attachments.
    """
    config = VendorApprovalConfig.get_config()
    to_list = config.get_mumbai_email_list()
    cc_list = config.get_cc_email_list()

    if not to_list:
        return

    subject = f"Vendor Payment Approved - {obj.request_id}"

    review_url = request.build_absolute_uri(
        reverse("vendor:detail", kwargs={"pk": obj.pk})
    )

    context = _build_common_email_context(request, obj, review_url=review_url)

    text_body = render_to_string(
        "email/vendor_payment_confirmation.txt",
        context,
    )

    html_body = render_to_string(
        "email/vendor_payment_confirmation.html",
        context,
    )

    try:
        email = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_list,
            cc=cc_list,
        )

        email.attach_alternative(html_body, "text/html")
        _attach_vendor_payment_files(email, context)
        email.send(fail_silently=False)

        logger.info(
            "Vendor final approval email sent successfully for request pk=%s request_id=%s invoice_count=%s grand_total=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
            context.get("invoice_count"),
            context.get("grand_total"),
        )

    except Exception:
        logger.exception(
            "Vendor final approval email failed for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )
