from django import forms
from django.contrib.auth import get_user_model
from .models import Vendor, VendorPaymentRequest, VendorApprovalConfig
from .utils import compress_file

User = get_user_model()

VENDOR_TYPE_CHOICES = [
    ('', '— Select Type —'),
    ('supplier', 'Supplier'),
    ('contractor', 'Contractor'),
    ('service', 'Service Provider'),
    ('logistics', 'Logistics'),
    ('other', 'Other'),
]


class VendorPaymentRequestForm(forms.ModelForm):
    vendor_type = forms.ChoiceField(choices=VENDOR_TYPE_CHOICES)

    class Meta:
        model  = VendorPaymentRequest
        fields = [
            'vendor', 'vendor_name_manual', 'vendor_type',
            'invoice_date', 'invoice_number',
            'base_amount', 'gst_amount',
            'bill_type', 'description', 'attachment',
        ]
        widgets = {
            'invoice_date':       forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'description':        forms.Textarea(attrs={'rows': 3, 'class': 'form-control'}),
            'base_amount':        forms.NumberInput(attrs={'step': '0.01', 'class': 'form-control', 'id': 'id_base_amount'}),
            'gst_amount':         forms.NumberInput(attrs={'step': '0.01', 'class': 'form-control', 'id': 'id_gst_amount'}),
            'invoice_number':     forms.TextInput(attrs={'class': 'form-control'}),
            'vendor_name_manual': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter vendor name'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['vendor'].queryset   = Vendor.objects.filter(is_active=True)
        self.fields['vendor'].required   = False
        self.fields['vendor'].empty_label = '— Select Vendor —'
        self.fields['vendor'].widget.attrs['class'] = 'form-select'
        self.fields['vendor_type'].widget.attrs['class'] = 'form-select'
        self.fields['bill_type'].widget.attrs['class']   = 'form-select'
        self.fields['vendor_name_manual'].required = False

    def clean(self):
        cleaned = super().clean()
        if not cleaned.get('vendor') and not (cleaned.get('vendor_name_manual') or '').strip():
            raise forms.ValidationError('Select a vendor or enter a vendor name manually.')
        return cleaned

    def clean_attachment(self):
        f = self.cleaned_data.get('attachment')
        if f and hasattr(f, 'name') and hasattr(f, 'read'):
            f = compress_file(f)
        return f


class VendorApprovalConfigForm(forms.ModelForm):
    class Meta:
        model  = VendorApprovalConfig
        fields = [
            'finance_users',
            'finance_manual_emails',
            'senior_authority',
            'mumbai_accounts',
            'mumbai_manual_emails',
            'cc_emails',
        ]
        widgets = {
            # Hidden — values driven by JS tag-input chips
            'finance_manual_emails': forms.HiddenInput(attrs={'id': 'id_finance_manual_emails'}),
            'mumbai_manual_emails':  forms.HiddenInput(attrs={'id': 'id_mumbai_manual_emails'}),
            'cc_emails': forms.Textarea(attrs={
                'rows': 3, 'class': 'form-control',
                'placeholder': 'email1@example.com, email2@example.com',
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        active_users = User.objects.filter(is_active=True).order_by('first_name', 'username')

        self.fields['senior_authority'].queryset = active_users
        self.fields['senior_authority'].widget.attrs['class'] = 'form-select'
        self.fields['senior_authority'].required = False

        self.fields['finance_users'].widget   = forms.CheckboxSelectMultiple()
        self.fields['finance_users'].queryset = active_users
        self.fields['finance_users'].required = False

        self.fields['mumbai_accounts'].widget   = forms.CheckboxSelectMultiple()
        self.fields['mumbai_accounts'].queryset = active_users
        self.fields['mumbai_accounts'].required = False

        self.fields['finance_manual_emails'].required = False
        self.fields['mumbai_manual_emails'].required  = False
        self.fields['cc_emails'].required             = False

    def clean_finance_manual_emails(self):
        return _clean_email_csv(self.cleaned_data.get('finance_manual_emails', ''))

    def clean_mumbai_manual_emails(self):
        return _clean_email_csv(self.cleaned_data.get('mumbai_manual_emails', ''))

    def clean_cc_emails(self):
        return _clean_email_csv(self.cleaned_data.get('cc_emails', ''))


def _clean_email_csv(raw):
    """Validate and normalise a comma-separated email string."""
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
            pass  # silently skip malformed addresses
    return ', '.join(valid)