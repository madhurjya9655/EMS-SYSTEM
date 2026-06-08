# apps/vendor/forms.py
import os

from django import forms
from django.contrib.auth import get_user_model
from django.forms import inlineformset_factory

from .models import (
    Vendor,
    VendorPaymentRequest,
    VendorPaymentInvoice,
    VendorApprovalConfig,
)
from .utils import compress_file

User = get_user_model()


ALLOWED_UPLOAD_EXTENSIONS = {
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
}

ALLOWED_UPLOAD_CONTENT_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/webp",
}


def _validate_payment_upload(uploaded_file, field_label):
    if not uploaded_file:
        return uploaded_file

    name = uploaded_file.name or ""
    ext = os.path.splitext(name)[1].lower()

    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise forms.ValidationError(
            f"{field_label} must be a PDF or image file."
        )

    content_type = getattr(uploaded_file, "content_type", "")

    if content_type and content_type not in ALLOWED_UPLOAD_CONTENT_TYPES:
        raise forms.ValidationError(
            f"{field_label} must be a valid PDF or image file."
        )

    return compress_file(uploaded_file)


class VendorPaymentRequestForm(forms.ModelForm):
    """
    Parent request form.

    This form stores request-level data only:
    - vendor
    - vendor type display
    - bank attachment
    - bank details text

    Invoice rows are handled separately by VendorPaymentInvoiceFormSet.
    """

    vendor_type_display = forms.CharField(
        required=False,
        label="Vendor Type",
        widget=forms.TextInput(
            attrs={
                "class": "form-control",
                "readonly": "readonly",
                "placeholder": "Auto-filled after selecting vendor",
            }
        ),
    )

    class Meta:
        model = VendorPaymentRequest

        fields = [
            "vendor",
            "bank_attachment",
            "bank_details_text",
        ]

        widgets = {
            "bank_details_text": forms.Textarea(
                attrs={
                    "rows": 4,
                    "class": "form-control",
                    "placeholder": (
                        "Account Holder Name:\n"
                        "Bank Name:\n"
                        "Account Number:\n"
                        "IFSC:"
                    ),
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["vendor"].queryset = Vendor.objects.filter(
            is_active=True
        ).order_by("name")

        self.fields["vendor"].required = True
        self.fields["vendor"].empty_label = "— Select Vendor —"
        self.fields["vendor"].widget.attrs["class"] = "form-select"

        self.fields["bank_attachment"].required = False
        self.fields["bank_attachment"].widget.attrs.update(
            {
                "class": "form-control",
                "accept": ".pdf,.jpg,.jpeg,.png,.webp",
            }
        )

        self.fields["bank_details_text"].required = False

        if self.instance and self.instance.pk and self.instance.vendor_id:
            self.fields["vendor_type_display"].initial = (
                self.instance.vendor.get_type_display()
            )

    def clean(self):
        cleaned = super().clean()
        vendor = cleaned.get("vendor")

        if not vendor:
            raise forms.ValidationError(
                "Please select a vendor from Vendor Master."
            )

        # Backend protection:
        # Do not trust browser/user for vendor_type.
        # Always copy vendor type from Vendor Master.
        self.instance.vendor_type = vendor.type
        self.instance.vendor_name_manual = ""

        return cleaned

    def clean_bank_attachment(self):
        uploaded_file = self.cleaned_data.get("bank_attachment")
        return _validate_payment_upload(uploaded_file, "Bank details attachment")


class VendorPaymentInvoiceForm(forms.ModelForm):
    """
    Child invoice row form.

    One VendorPaymentRequest can have many of these forms.
    """

    class Meta:
        model = VendorPaymentInvoice

        fields = [
            "invoice_date",
            "invoice_number",
            "bill_type",
            "base_amount",
            "gst_amount",
            "description",
            "invoice_attachment",
        ]

        widgets = {
            "invoice_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "form-control invoice-date",
                }
            ),
            "invoice_number": forms.TextInput(
                attrs={
                    "class": "form-control invoice-number",
                    "placeholder": "INV001",
                }
            ),
            "bill_type": forms.Select(
                attrs={
                    "class": "form-select bill-type",
                }
            ),
            "base_amount": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "min": "0",
                    "class": "form-control base-amount",
                    "placeholder": "0.00",
                }
            ),
            "gst_amount": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "min": "0",
                    "class": "form-control gst-amount",
                    "placeholder": "0.00",
                }
            ),
            "description": forms.Textarea(
                attrs={
                    "rows": 2,
                    "class": "form-control",
                    "placeholder": "Invoice/payment description",
                }
            ),
            "invoice_attachment": forms.ClearableFileInput(
                attrs={
                    "class": "form-control invoice-attachment",
                    "accept": ".pdf,.jpg,.jpeg,.png,.webp",
                }
            ),
        }

    def clean_invoice_number(self):
        invoice_number = (self.cleaned_data.get("invoice_number") or "").strip()

        if not invoice_number:
            raise forms.ValidationError("Invoice number is required.")

        return invoice_number

    def clean_invoice_attachment(self):
        uploaded_file = self.cleaned_data.get("invoice_attachment")
        return _validate_payment_upload(uploaded_file, "Invoice attachment")

    def clean(self):
        cleaned = super().clean()

        bill_type = cleaned.get("bill_type")
        base_amount = cleaned.get("base_amount")
        gst_amount = cleaned.get("gst_amount")

        if base_amount is None:
            raise forms.ValidationError("Base amount is required.")

        if base_amount < 0:
            raise forms.ValidationError("Base amount cannot be negative.")

        if gst_amount is None:
            cleaned["gst_amount"] = 0
            gst_amount = 0

        if gst_amount < 0:
            raise forms.ValidationError("GST amount cannot be negative.")

        if bill_type == VendorPaymentInvoice.BillType.NON_GST and gst_amount:
            raise forms.ValidationError(
                "GST amount must be zero for Non-GST bill type."
            )

        return cleaned


class BaseVendorPaymentInvoiceFormSet(forms.BaseInlineFormSet):
    """
    Custom invoice formset validation.

    Ensures:
    - at least one non-deleted invoice exists
    - duplicate invoice numbers are not entered in same request
    """

    def clean(self):
        super().clean()

        if any(self.errors):
            return

        active_invoice_count = 0
        invoice_numbers = set()

        for form in self.forms:
            if not hasattr(form, "cleaned_data"):
                continue

            cleaned = form.cleaned_data

            if not cleaned:
                continue

            if cleaned.get("DELETE"):
                continue

            invoice_number = (cleaned.get("invoice_number") or "").strip()

            if not invoice_number:
                continue

            active_invoice_count += 1

            normalized_invoice_number = invoice_number.lower()

            if normalized_invoice_number in invoice_numbers:
                raise forms.ValidationError(
                    f"Duplicate invoice number found: {invoice_number}"
                )

            invoice_numbers.add(normalized_invoice_number)

        if active_invoice_count < 1:
            raise forms.ValidationError(
                "Please add at least one invoice row."
            )


VendorPaymentInvoiceFormSet = inlineformset_factory(
    parent_model=VendorPaymentRequest,
    model=VendorPaymentInvoice,
    form=VendorPaymentInvoiceForm,
    formset=BaseVendorPaymentInvoiceFormSet,
    extra=0,
    can_delete=True,
    min_num=1,
    validate_min=True,
)

class VendorApprovalConfigForm(forms.ModelForm):
    class Meta:
        model = VendorApprovalConfig

        fields = [
            "finance_users",
            "finance_manual_emails",
            "senior_authority",
            "mumbai_accounts",
            "mumbai_manual_emails",
            "cc_emails",
        ]

        widgets = {
            "finance_manual_emails": forms.HiddenInput(
                attrs={
                    "id": "id_finance_manual_emails",
                }
            ),
            "mumbai_manual_emails": forms.HiddenInput(
                attrs={
                    "id": "id_mumbai_manual_emails",
                }
            ),
            "cc_emails": forms.Textarea(
                attrs={
                    "rows": 3,
                    "class": "form-control",
                    "placeholder": "email1@example.com, email2@example.com",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        active_users = User.objects.filter(
            is_active=True
        ).order_by("first_name", "username")

        self.fields["senior_authority"].queryset = active_users
        self.fields["senior_authority"].widget.attrs["class"] = "form-select"
        self.fields["senior_authority"].required = False

        self.fields["finance_users"].widget = forms.CheckboxSelectMultiple()
        self.fields["finance_users"].queryset = active_users
        self.fields["finance_users"].required = False

        self.fields["mumbai_accounts"].widget = forms.CheckboxSelectMultiple()
        self.fields["mumbai_accounts"].queryset = active_users
        self.fields["mumbai_accounts"].required = False

        self.fields["finance_manual_emails"].required = False
        self.fields["mumbai_manual_emails"].required = False
        self.fields["cc_emails"].required = False

    def clean_finance_manual_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get("finance_manual_emails", "")
        )

    def clean_mumbai_manual_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get("mumbai_manual_emails", "")
        )

    def clean_cc_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get("cc_emails", "")
        )


def _clean_email_csv(raw):
    if not raw:
        return ""

    validator = forms.EmailField()
    valid = []

    for part in raw.split(","):
        part = part.strip().lower()

        if not part:
            continue

        try:
            valid.append(validator.clean(part))
        except forms.ValidationError:
            pass

    return ", ".join(valid)