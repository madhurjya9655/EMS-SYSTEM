from django import forms
from django.core.exceptions import ValidationError
from .models import AuthorizedNumber, Holiday, SystemSetting

class AuthorizedNumberForm(forms.ModelForm):
    class Meta:
        model = AuthorizedNumber
        fields = ['label', 'number']
        widgets = {
            'label':  forms.TextInput(attrs={'class': 'form-control'}),
            'number': forms.TextInput(attrs={'class': 'form-control'}),
        }

class HolidayForm(forms.ModelForm):
    class Meta:
        model = Holiday
        fields = ['date', 'name']
        widgets = {
            'date': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'name': forms.TextInput(attrs={'class': 'form-control'}),
        }

    def clean_date(self):
        value = self.cleaned_data['date']
        if value.weekday() == 6:
            raise ValidationError("Cannot add holiday on Sunday (already non-working day).")
        return value

class HolidayUploadForm(forms.Form):
    file = forms.FileField(
        label="Upload Excel/CSV",
        help_text="Cols: date (YYYY-MM-DD), name",
        widget=forms.ClearableFileInput(attrs={'class': 'form-control'})
    )

class SystemSettingsForm(forms.ModelForm):
    class Meta:
        model = SystemSetting
        exclude = []
        widgets = {
            'whatsapp_vendor':      forms.TextInput(attrs={'class': 'form-control'}),
            'whatsapp_api_key':     forms.TextInput(attrs={'class': 'form-control'}),
            'whatsapp_sender_id':   forms.TextInput(attrs={'class': 'form-control'}),
            'whatsapp_webhook_url': forms.URLInput(attrs={'class': 'form-control'}),
            'authorized_phones':    forms.Textarea(attrs={'class': 'form-control', 'rows': 2}),
            'authorized_emails':    forms.Textarea(attrs={'class': 'form-control', 'rows': 2}),
            'send_daily_doer':    forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'send_daily_admin':   forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'send_weekly_doer':   forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'send_weekly_admin':  forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'send_monthly_doer':  forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'send_monthly_admin': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_wapp_pending_checklist':   forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_wapp_pending_delegation':  forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_pending_checklist':  forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_pending_delegation': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_wapp_checklist':            forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_wapp_fms':                  forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_checklist':           forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_delegation':          forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_helpticket':          forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'notify_email_helpticket_reminder': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'all_doer_report_generate':         forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'mis_performance_mode':  forms.Select(attrs={'class': 'form-select'}),
            'checklist_weightage':   forms.NumberInput(attrs={'class': 'form-control'}),
            'delegation_weightage':  forms.NumberInput(attrs={'class': 'form-control'}),
            'fms_weightage':         forms.NumberInput(attrs={'class': 'form-control'}),
            'weight_low':            forms.NumberInput(attrs={'class': 'form-control'}),
            'weight_medium':         forms.NumberInput(attrs={'class': 'form-control'}),
            'weight_high':           forms.NumberInput(attrs={'class': 'form-control'}),
            'smtp_from_name': forms.TextInput(attrs={'class': 'form-control'}),
            'smtp_username':  forms.EmailInput(attrs={'class': 'form-control'}),
            'smtp_password':  forms.PasswordInput(render_value=True, attrs={'class': 'form-control'}),
            'high_stock_notification_freq': forms.NumberInput(attrs={'class': 'form-control'}),
            'low_stock_notification_freq':  forms.NumberInput(attrs={'class': 'form-control'}),
            'stockout_notification_freq':   forms.NumberInput(attrs={'class': 'form-control'}),
            'max_fast_flowing_product':     forms.NumberInput(attrs={'class': 'form-control'}),
            'max_slow_flowing_product':     forms.NumberInput(attrs={'class': 'form-control'}),
            'marketing_mode':         forms.Select(attrs={'class': 'form-select'}),
            'marketing_freeze_min':   forms.NumberInput(attrs={'class': 'form-control'}),
            'marketing_freeze_max':   forms.NumberInput(attrs={'class': 'form-control'}),
            'marketing_after_sending':forms.NumberInput(attrs={'class': 'form-control'}),
            'marketing_sleep_min':    forms.NumberInput(attrs={'class': 'form-control'}),
            'marketing_sleep_max':    forms.NumberInput(attrs={'class': 'form-control'}),
            'logo': forms.ClearableFileInput(attrs={'class': 'form-control'}),
        }
