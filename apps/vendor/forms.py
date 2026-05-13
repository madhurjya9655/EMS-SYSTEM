#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\vendor\forms.py
import os

from django import forms
from django.contrib.auth import get_user_model

from .models import Vendor, VendorPaymentRequest, VendorApprovalConfig
from .utils import compress_file

User = get_user_model()


ALLOWED_UPLOAD_EXTENSIONS = {
    '.pdf',
    '.jpg',
    '.jpeg',
    '.png',
    '.webp',
}

ALLOWED_UPLOAD_CONTENT_TYPES = {
    'application/pdf',
    'image/jpeg',
    'image/png',
    'image/webp',
}


def _validate_payment_upload(uploaded_file, field_label):
    if not uploaded_file:
        return uploaded_file

    name = uploaded_file.name or ''
    ext = os.path.splitext(name)[1].lower()

    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise forms.ValidationError(
            f'{field_label} must be a PDF or image file.'
        )

    content_type = getattr(uploaded_file, 'content_type', '')

    if content_type and content_type not in ALLOWED_UPLOAD_CONTENT_TYPES:
        raise forms.ValidationError(
            f'{field_label} must be a valid PDF or image file.'
        )

    return compress_file(uploaded_file)


class VendorPaymentRequestForm(forms.ModelForm):
    # Display-only field.
    # This is not saved directly to database.
    # Real vendor_type is set from selected Vendor in clean()/model save().
    vendor_type_display = forms.CharField(
        required=False,
        label='Vendor Type',
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'readonly': 'readonly',
            'placeholder': 'Auto-filled after selecting vendor',
        }),
    )

    class Meta:
        model = VendorPaymentRequest

        fields = [
            'vendor',
            'invoice_date',
            'invoice_number',
            'base_amount',
            'gst_amount',
            'bill_type',
            'description',
            'attachment',
            'bank_attachment',
            'bank_details_text',
        ]

        widgets = {
            'invoice_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'form-control',
            }),
            'invoice_number': forms.TextInput(attrs={
                'class': 'form-control',
            }),
            'base_amount': forms.NumberInput(attrs={
                'step': '0.01',
                'class': 'form-control',
                'id': 'id_base_amount',
            }),
            'gst_amount': forms.NumberInput(attrs={
                'step': '0.01',
                'class': 'form-control',
                'id': 'id_gst_amount',
            }),
            'bill_type': forms.Select(attrs={
                'class': 'form-select',
            }),
            'description': forms.Textarea(attrs={
                'rows': 3,
                'class': 'form-control',
            }),
            'bank_details_text': forms.Textarea(attrs={
                'rows': 4,
                'class': 'form-control',
                'placeholder': (
                    'Account Holder Name:\n'
                    'Bank Name:\n'
                    'Account Number:\n'
                    'IFSC:'
                ),
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields['vendor'].queryset = Vendor.objects.filter(
            is_active=True
        ).order_by('name')

        self.fields['vendor'].required = True
        self.fields['vendor'].empty_label = '— Select Vendor —'
        self.fields['vendor'].widget.attrs['class'] = 'form-select'

        self.fields['attachment'].required = False
        self.fields['bank_attachment'].required = False
        self.fields['bank_details_text'].required = False

        if self.instance and self.instance.pk and self.instance.vendor_id:
            self.fields['vendor_type_display'].initial = (
                self.instance.vendor.get_type_display()
            )

    def clean(self):
        cleaned = super().clean()
        vendor = cleaned.get('vendor')

        if not vendor:
            raise forms.ValidationError(
                'Please select a vendor from Vendor Master.'
            )

        # Backend protection:
        # Do not trust browser/user for vendor_type.
        # Always copy vendor type from Vendor Master.
        self.instance.vendor_type = vendor.type
        self.instance.vendor_name_manual = ''

        return cleaned

    def clean_attachment(self):
        uploaded_file = self.cleaned_data.get('attachment')
        return _validate_payment_upload(uploaded_file, 'Invoice attachment')

    def clean_bank_attachment(self):
        uploaded_file = self.cleaned_data.get('bank_attachment')
        return _validate_payment_upload(uploaded_file, 'Bank details attachment')


class VendorApprovalConfigForm(forms.ModelForm):
    class Meta:
        model = VendorApprovalConfig

        fields = [
            'finance_users',
            'finance_manual_emails',
            'senior_authority',
            'mumbai_accounts',
            'mumbai_manual_emails',
            'cc_emails',
        ]

        widgets = {
            'finance_manual_emails': forms.HiddenInput(attrs={
                'id': 'id_finance_manual_emails',
            }),
            'mumbai_manual_emails': forms.HiddenInput(attrs={
                'id': 'id_mumbai_manual_emails',
            }),
            'cc_emails': forms.Textarea(attrs={
                'rows': 3,
                'class': 'form-control',
                'placeholder': 'email1@example.com, email2@example.com',
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        active_users = User.objects.filter(
            is_active=True
        ).order_by('first_name', 'username')

        self.fields['senior_authority'].queryset = active_users
        self.fields['senior_authority'].widget.attrs['class'] = 'form-select'
        self.fields['senior_authority'].required = False

        self.fields['finance_users'].widget = forms.CheckboxSelectMultiple()
        self.fields['finance_users'].queryset = active_users
        self.fields['finance_users'].required = False

        self.fields['mumbai_accounts'].widget = forms.CheckboxSelectMultiple()
        self.fields['mumbai_accounts'].queryset = active_users
        self.fields['mumbai_accounts'].required = False

        self.fields['finance_manual_emails'].required = False
        self.fields['mumbai_manual_emails'].required = False
        self.fields['cc_emails'].required = False

    def clean_finance_manual_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get('finance_manual_emails', '')
        )

    def clean_mumbai_manual_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get('mumbai_manual_emails', '')
        )

    def clean_cc_emails(self):
        return _clean_email_csv(
            self.cleaned_data.get('cc_emails', '')
        )


def _clean_email_csv(raw):
    if not raw:
        return ''

    validator = forms.EmailField()
    valid = []

    for part in raw.split(','):
        part = part.strip().lower()

        if not part:
            continue

        try:
            valid.append(validator.clean(part))
        except forms.ValidationError:
            pass

    return ', '.join(valid)