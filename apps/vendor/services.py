#apps\vendor\services.py
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


def _normalise_email(email: str) -> str:
    """
    Normalise an email address for case-insensitive comparison.
    """
    return str(email or "").strip().lower()


def _unique_email_list(emails, *, exclude=None) -> list[str]:
    """
    Return a clean, case-insensitively de-duplicated email list.

    Addresses in exclude are removed from the result so the same recipient
    is not present in both TO and CC.
    """
    excluded = {
        _normalise_email(email)
        for email in (exclude or [])
        if _normalise_email(email)
    }

    result = []
    seen = set()

    for email in emails or []:
        cleaned = str(email or "").strip()
        normalised = _normalise_email(cleaned)

        if not normalised:
            continue

        if normalised in excluded:
            continue

        if normalised in seen:
            continue

        seen.add(normalised)
        result.append(cleaned)

    return result


def _get_finance_email_list(
    config: VendorApprovalConfig,
) -> list[str]:
    """
    Return the Finance Team configured by an administrator.

    Sources:
    - Existing active system users selected under Finance Users.
    - Full email addresses entered under Finance manual emails.

    No names or email addresses are hardcoded.
    """
    return _unique_email_list(
        config.get_finance_email_list()
    )


def _get_mumbai_email_list(
    config: VendorApprovalConfig,
) -> list[str]:
    """
    Return Mumbai Office recipients configured by an administrator.

    Sources:
    - Existing active system users selected under Mumbai Accounts.
    - Full email addresses entered under Mumbai manual emails.

    No names or email addresses are hardcoded.
    """
    return _unique_email_list(
        config.get_mumbai_email_list()
    )


def _absolute_file_url(request, file_field) -> str:
    """
    Build a complete public URL for a FileField.
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
    Return the uploaded file's base name.
    """
    if not file_field:
        return ""

    file_name = getattr(file_field, "name", "")

    if not file_name:
        return ""

    return file_name.split("/")[-1]


def _user_display_name(user) -> str:
    """
    Return the safest available display name for a Django user.
    """
    if not user:
        return ""

    try:
        return (
            user.get_full_name()
            or user.username
            or user.email
            or ""
        )
    except Exception:
        return ""


def _attach_file_if_possible(
    email,
    file_field,
    label: str,
) -> None:
    """
    Attach a stored file when a local storage path is available.

    Email links remain available when physical attachment is not possible.
    """
    if not file_field:
        return

    try:
        file_name = getattr(file_field, "name", "")

        if not file_name:
            return

        if not default_storage.exists(file_name):
            logger.warning(
                "Skipping %s attachment because file does not exist: %s",
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
            "Skipping physical attachment for %s because the local file "
            "path is unavailable. File URL remains available. File: %s",
            label,
            file_name,
        )

    except Exception:
        logger.exception(
            "Unable to attach %s to Vendor Payment email",
            label,
        )


def _get_invoice_queryset(obj):
    """
    Safely load child invoice rows.
    """
    try:
        return list(obj.invoices.all())
    except Exception:
        return []


def _build_invoice_context(request, obj) -> list[dict]:
    """
    Build invoice context for HTML and plain-text email templates.

    Child invoice rows are used for current requests.
    Legacy parent invoice fields remain supported.
    """
    invoice_rows = _get_invoice_queryset(obj)

    if invoice_rows:
        invoices = []

        for invoice in invoice_rows:
            attachment = getattr(
                invoice,
                "invoice_attachment",
                None,
            )

            invoices.append(
                {
                    "obj": invoice,
                    "invoice_number": (
                        getattr(invoice, "invoice_number", "") or ""
                    ),
                    "invoice_date": getattr(
                        invoice,
                        "invoice_date",
                        None,
                    ),
                    "bill_type": (
                        invoice.get_bill_type_display()
                        if hasattr(
                            invoice,
                            "get_bill_type_display",
                        )
                        else str(
                            getattr(invoice, "bill_type", "") or ""
                        )
                    ),
                    "base_amount": getattr(
                        invoice,
                        "base_amount",
                        0,
                    ),
                    "gst_amount": getattr(
                        invoice,
                        "gst_amount",
                        0,
                    ),
                    "total_amount": getattr(
                        invoice,
                        "total_amount",
                        0,
                    ),
                    "description": (
                        getattr(invoice, "description", "") or ""
                    ),
                    "invoice_attachment": attachment,
                    "invoice_url": _absolute_file_url(
                        request,
                        attachment,
                    ),
                    "invoice_name": _file_display_name(
                        attachment,
                    ),
                }
            )

        return invoices

    if getattr(obj, "invoice_number", ""):
        attachment = getattr(obj, "attachment", None)

        return [
            {
                "obj": obj,
                "invoice_number": (
                    getattr(obj, "invoice_number", "") or ""
                ),
                "invoice_date": getattr(
                    obj,
                    "invoice_date",
                    None,
                ),
                "bill_type": (
                    obj.get_bill_type_display()
                    if hasattr(obj, "get_bill_type_display")
                    else str(
                        getattr(obj, "bill_type", "") or ""
                    )
                ),
                "base_amount": getattr(
                    obj,
                    "base_amount",
                    0,
                ),
                "gst_amount": getattr(
                    obj,
                    "gst_amount",
                    0,
                ),
                "total_amount": getattr(
                    obj,
                    "total_amount",
                    0,
                ),
                "description": (
                    getattr(obj, "description", "") or ""
                ),
                "invoice_attachment": attachment,
                "invoice_url": _absolute_file_url(
                    request,
                    attachment,
                ),
                "invoice_name": _file_display_name(
                    attachment,
                ),
            }
        ]

    return []


def _payment_total(obj):
    """
    Return the current or legacy request total.
    """
    try:
        return obj.payment_total
    except Exception:
        return (
            getattr(obj, "grand_total", None)
            or getattr(obj, "total_amount", 0)
        )


def _invoice_count(
    obj,
    invoices: list[dict],
) -> int:
    """
    Return the current or legacy invoice count.
    """
    try:
        return obj.invoice_count
    except Exception:
        return len(invoices)


def _invoice_amount_totals(
    invoices: list[dict],
) -> dict:
    """
    Calculate email display totals from loaded invoice rows.
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


def _build_common_email_context(
    request,
    obj,
    *,
    review_url: str = "",
    email_stage: str = "",
) -> dict:
    """
    Build shared Vendor Payment email context.
    """
    invoices = _build_invoice_context(request, obj)

    bank_attachment = getattr(
        obj,
        "bank_attachment",
        None,
    )

    bank_attachment_url = _absolute_file_url(
        request,
        bank_attachment,
    )

    bank_attachment_name = _file_display_name(
        bank_attachment,
    )

    submitted_by = ""

    if getattr(obj, "created_by_id", None):
        submitted_by = _user_display_name(
            getattr(obj, "created_by", None)
        )

    finance_verified_by = ""

    if getattr(obj, "finance_approved_by_id", None):
        finance_verified_by = _user_display_name(
            getattr(obj, "finance_approved_by", None)
        )

    approved_by = ""

    if getattr(obj, "final_approved_by_id", None):
        approved_by = _user_display_name(
            getattr(obj, "final_approved_by", None)
        )

    finance_verification_remark = ""

    if email_stage == "final_approval":
        finance_verification_remark = str(
            getattr(obj, "remarks", "") or ""
        ).strip()

    totals = _invoice_amount_totals(invoices)
    grand_total = _payment_total(obj)
    invoice_count = _invoice_count(obj, invoices)

    return {
        "obj": obj,
        "review_url": review_url,
        "site_url": request.build_absolute_uri("/"),
        "submitted_by": submitted_by,
        "requested_date": getattr(
            obj,
            "created_at",
            None,
        ),
        "invoices": invoices,
        "invoice_count": invoice_count,
        "base_total": totals["base_total"],
        "gst_total": totals["gst_total"],
        "invoice_total": totals["invoice_total"],
        "grand_total": grand_total,
        "bank_attachment": bank_attachment,
        "bank_attachment_url": bank_attachment_url,
        "bank_attachment_available": bool(
            bank_attachment
        ),
        "bank_attachment_name": bank_attachment_name,
        "finance_verified_by": finance_verified_by,
        "finance_verification_remark": (
            finance_verification_remark
        ),
        "approved_by": approved_by,
        "approved_on": getattr(
            obj,
            "updated_at",
            None,
        ),
        "email_stage": email_stage,
        "is_finance_verification_email": (
            email_stage == "finance_verification"
        ),
        "is_final_approval_email": (
            email_stage == "final_approval"
        ),
        "is_payment_processing_email": (
            email_stage == "payment_processing"
        ),
    }


def _attach_vendor_payment_files(
    email,
    context: dict,
) -> None:
    """
    Attach invoice files and the request-level bank attachment.
    """
    for invoice in context.get("invoices", []):
        invoice_number = (
            invoice.get("invoice_number") or ""
        )

        _attach_file_if_possible(
            email=email,
            file_field=invoice.get(
                "invoice_attachment"
            ),
            label=(
                f"invoice attachment {invoice_number}"
            ).strip(),
        )

    _attach_file_if_possible(
        email=email,
        file_field=context.get("bank_attachment"),
        label="bank details attachment",
    )


def send_vendor_payment_submission_email(
    request,
    obj,
) -> None:
    """
    Send the Finance Verification email.

    TO recipients are fully controlled from Vendor Admin Setup.
    """
    config = VendorApprovalConfig.get_config()

    to_list = _get_finance_email_list(config)

    if not to_list:
        logger.warning(
            "Vendor Finance Verification email skipped for request pk=%s "
            "because no Finance recipients are configured",
            getattr(obj, "pk", None),
        )
        return

    subject = (
        "Vendor Payment Verification Required - "
        f"{obj.request_id}"
    )

    review_url = request.build_absolute_uri(
        reverse(
            "vendor:detail",
            kwargs={"pk": obj.pk},
        )
    )

    context = _build_common_email_context(
        request,
        obj,
        review_url=review_url,
        email_stage="finance_verification",
    )

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

        email.attach_alternative(
            html_body,
            "text/html",
        )

        _attach_vendor_payment_files(
            email,
            context,
        )

        email.send(fail_silently=False)

        logger.info(
            "Vendor Finance Verification email sent for request pk=%s "
            "request_id=%s to=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
            to_list,
        )

    except Exception:
        logger.exception(
            "Vendor Finance Verification email failed for request pk=%s "
            "request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )


def send_vendor_payment_manager_approval_email(
    request,
    obj,
) -> None:
    """
    Send Final Approval email.

    TO:
    - Assigned senior authority

    CC:
    - Administrator-configured Finance Team
    """
    config = VendorApprovalConfig.get_config()

    senior_authority = config.senior_authority

    if not senior_authority:
        return

    approver_email = str(
        getattr(senior_authority, "email", "") or ""
    ).strip()

    if not approver_email:
        return

    to_list = _unique_email_list(
        [approver_email]
    )

    finance_cc_list = _get_finance_email_list(
        config
    )

    cc_list = _unique_email_list(
        finance_cc_list,
        exclude=to_list,
    )

    subject = (
        "Vendor Payment Ready for Final Approval - "
        f"{obj.request_id}"
    )

    approval_url = request.build_absolute_uri(
        reverse(
            "vendor:detail",
            kwargs={"pk": obj.pk},
        )
    )

    context = _build_common_email_context(
        request,
        obj,
        review_url=approval_url,
        email_stage="final_approval",
    )

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
            cc=cc_list,
        )

        email.attach_alternative(
            html_body,
            "text/html",
        )

        _attach_vendor_payment_files(
            email,
            context,
        )

        email.send(fail_silently=False)

        logger.info(
            "Vendor Final Approval email sent for request pk=%s "
            "request_id=%s to=%s cc=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
            to_list,
            cc_list,
        )

    except Exception:
        logger.exception(
            "Vendor Final Approval email failed for request pk=%s "
            "request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )


def send_vendor_payment_final_approval_email(
    request,
    obj,
) -> None:
    """
    Send Mumbai Office payment-processing email.

    TO:
    - Administrator-configured Mumbai Accounts recipients

    CC:
    - Existing configured CC emails
    - Administrator-configured Finance Team
    """
    config = VendorApprovalConfig.get_config()

    to_list = _get_mumbai_email_list(config)

    if not to_list:
        logger.warning(
            "Vendor Mumbai payment-processing email skipped for request "
            "pk=%s because no Mumbai recipients are configured",
            getattr(obj, "pk", None),
        )
        return

    existing_cc_list = config.get_cc_email_list()
    finance_cc_list = _get_finance_email_list(config)

    cc_list = _unique_email_list(
        list(existing_cc_list or [])
        + list(finance_cc_list or []),
        exclude=to_list,
    )

    subject = (
        f"Vendor Payment Approved - {obj.request_id}"
    )

    review_url = request.build_absolute_uri(
        reverse(
            "vendor:detail",
            kwargs={"pk": obj.pk},
        )
    )

    context = _build_common_email_context(
        request,
        obj,
        review_url=review_url,
        email_stage="payment_processing",
    )

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

        email.attach_alternative(
            html_body,
            "text/html",
        )

        _attach_vendor_payment_files(
            email,
            context,
        )

        email.send(fail_silently=False)

        logger.info(
            "Vendor Mumbai payment-processing email sent for request pk=%s "
            "request_id=%s to=%s cc=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
            to_list,
            cc_list,
        )

    except Exception:
        logger.exception(
            "Vendor Mumbai payment-processing email failed for request pk=%s "
            "request_id=%s",
            getattr(obj, "pk", None),
            getattr(obj, "request_id", ""),
        )