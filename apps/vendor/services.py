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


def send_vendor_payment_submission_email(request, obj) -> None:
    """
    Send professional HTML vendor payment approval email to finance approvers.

    Includes:
    - HTML email
    - Plain text fallback
    - Review Request button
    - Invoice attachment button
    - Bank details attachment button
    - Physical file attachments where local path is available
    """
    config = VendorApprovalConfig.get_config()
    to_list = config.get_finance_email_list()

    if not to_list:
        logger.warning(
            "Vendor payment submission email skipped for request pk=%s because finance email list is empty",
            getattr(obj, "pk", None),
        )
        return

    subject = f"Vendor Payment Approval Required – {obj.request_id}"

    review_url = request.build_absolute_uri(
        reverse("vendor:detail", kwargs={"pk": obj.pk})
    )

    invoice_url = _absolute_file_url(request, obj.attachment)
    bank_attachment_url = _absolute_file_url(request, obj.bank_attachment)

    submitted_by = ""
    if getattr(obj, "created_by_id", None):
        submitted_by = obj.created_by.get_full_name() or obj.created_by.username

    context = {
        "obj": obj,
        "review_url": review_url,
        "invoice_url": invoice_url,
        "bank_attachment_url": bank_attachment_url,
        "submitted_by": submitted_by,
        "invoice_available": bool(obj.attachment),
        "bank_attachment_available": bool(obj.bank_attachment),
    }

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

        # Attach actual files to email when local file path is available.
        _attach_file_if_possible(
            email=email,
            file_field=obj.attachment,
            label="invoice attachment",
        )

        _attach_file_if_possible(
            email=email,
            file_field=obj.bank_attachment,
            label="bank details attachment",
        )

        email.send(fail_silently=False)

        logger.info(
            "Vendor payment submission email sent successfully for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )

    except Exception:
        logger.exception(
            "Vendor payment submission email failed for request pk=%s request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )