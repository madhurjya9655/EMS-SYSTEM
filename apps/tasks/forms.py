from django import forms
from django.core.exceptions import ValidationError
from .models import Checklist, Delegation, BulkUpload, HelpTicket
from apps.settings.models import Holiday
import datetime


def is_holiday_or_sunday(date_val):
    """
    Treat Sundays and dates present in Holiday table as non-working.
    Accepts a date or datetime; if datetime, compare by its date component.
    """
    if isinstance(date_val, datetime.datetime):
        date_val = date_val.date()
    return date_val.weekday() == 6 or Holiday.objects.filter(date=date_val).exists()


class ChecklistForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local', 'class': 'form-control'})
    )
    reminder_starting_time = forms.TimeField(
        widget=forms.TimeInput(attrs={'type': 'time', 'class': 'form-control'}),
        required=False
    )

    class Meta:
        model = Checklist
        fields = [
            'assign_by', 'task_name', 'assign_to', 'planned_date',
            'priority', 'attachment_mandatory', 'mode', 'frequency',
            'time_per_task_minutes', 'remind_before_days', 'message',
            'media_upload', 'assign_pc', 'group_name', 'notify_to', 'auditor',
            'set_reminder', 'reminder_mode', 'reminder_frequency',
            'reminder_before_days', 'reminder_starting_time',
            'checklist_auto_close', 'checklist_auto_close_days',
        ]
        widgets = {
            'assign_by': forms.Select(attrs={'class': 'form-select'}),
            'task_name': forms.TextInput(attrs={'class': 'form-control'}),
            'assign_to': forms.Select(attrs={'class': 'form-select'}),
            'priority': forms.Select(attrs={'class': 'form-select'}),
            'attachment_mandatory': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'mode': forms.Select(attrs={'class': 'form-select'}),
            'frequency': forms.NumberInput(attrs={'class': 'form-control', 'min': '1'}),
            'time_per_task_minutes': forms.NumberInput(attrs={'class': 'form-control', 'min': '0'}),
            'remind_before_days': forms.NumberInput(attrs={'class': 'form-control', 'min': '0'}),
            'message': forms.Textarea(attrs={'class': 'form-control', 'rows': 3}),
            'media_upload': forms.ClearableFileInput(attrs={'class': 'form-control'}),
            'assign_pc': forms.Select(attrs={'class': 'form-select'}),
            'group_name': forms.TextInput(attrs={'class': 'form-control'}),
            'notify_to': forms.Select(attrs={'class': 'form-select'}),
            'auditor': forms.Select(attrs={'class': 'form-select'}),
            'set_reminder': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'reminder_mode': forms.Select(attrs={'class': 'form-select'}),
            'reminder_frequency': forms.NumberInput(attrs={'class': 'form-control', 'min': '1'}),
            'reminder_before_days': forms.NumberInput(attrs={'class': 'form-control', 'min': '0'}),
            'checklist_auto_close': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'checklist_auto_close_days': forms.NumberInput(attrs={'class': 'form-control', 'min': '0'}),
        }

    def clean_planned_date(self):
        planned_date = self.cleaned_data['planned_date']
        if is_holiday_or_sunday(planned_date):
            raise ValidationError("This is a holiday date or Sunday, you cannot add a task on this day.")
        return planned_date

    def clean(self):
        cleaned = super().clean()
        mode = cleaned.get('mode')
        freq = cleaned.get('frequency')
        if mode and (not freq or int(freq) < 1):
            self.add_error('frequency', "Frequency must be at least 1 when a recurrence mode is selected.")
        for fld in ('time_per_task_minutes', 'remind_before_days', 'reminder_frequency', 'reminder_before_days', 'checklist_auto_close_days'):
            val = cleaned.get(fld)
            if val is not None and int(val) < 0:
                self.add_error(fld, "Must be a non-negative number.")
        return cleaned


class CompleteChecklistForm(forms.ModelForm):
    class Meta:
        model = Checklist
        fields = ['doer_file', 'doer_notes']
        widgets = {
            'doer_file':  forms.ClearableFileInput(attrs={'class': 'form-control'}),
            'doer_notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 4}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['doer_file'].required = self.instance.attachment_mandatory
        self.fields['doer_notes'].required = False


class DelegationForm(forms.ModelForm):
    # ✔ CHANGED: DateField -> DateTimeField so time is mandatory
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local', 'class': 'form-control'})
    )
    audio_recording = forms.FileField(
        widget=forms.ClearableFileInput(attrs={'accept': 'audio/*', 'class': 'form-control'}),
        required=False
    )
    time_per_task_minutes = forms.IntegerField(
        label="Time per Task (minutes)",
        min_value=0,
        help_text="How many minutes should this delegation take?",
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )

    class Meta:
        model = Delegation
        fields = [
            'assign_by', 'task_name', 'assign_to',
            'planned_date', 'priority',
            'attachment_mandatory', 'audio_recording',
            'time_per_task_minutes',
        ]
        widgets = {
            'assign_by': forms.Select(attrs={'class': 'form-select'}),
            'task_name': forms.TextInput(attrs={'class': 'form-control'}),
            'assign_to': forms.Select(attrs={'class': 'form-select'}),
            'priority': forms.Select(attrs={'class': 'form-select'}),
            'attachment_mandatory': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

    def clean_planned_date(self):
        planned_date = self.cleaned_data['planned_date']
        if is_holiday_or_sunday(planned_date):
            raise ValidationError("This is a holiday date or Sunday, you cannot add a task on this day.")
        return planned_date


class CompleteDelegationForm(forms.ModelForm):
    class Meta:
        model = Delegation
        fields = ['doer_file', 'doer_notes']
        widgets = {
            'doer_file':  forms.ClearableFileInput(attrs={'class': 'form-control'}),
            'doer_notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 4}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['doer_file'].required = self.instance.attachment_mandatory
        self.fields['doer_notes'].required = False


class BulkUploadForm(forms.ModelForm):
    class Meta:
        model = BulkUpload
        fields = ['form_type', 'csv_file']
        widgets = {
            'form_type': forms.Select(attrs={'class': 'form-select'}),
            'csv_file': forms.ClearableFileInput(attrs={'class': 'form-control'}),
        }


class HelpTicketForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local', 'class': 'form-control'})
    )
    media_upload = forms.FileField(
        widget=forms.ClearableFileInput(attrs={'class': 'form-control'}),
        required=False
    )

    class Meta:
        model = HelpTicket
        fields = [
            'title',
            'assign_to',
            'media_upload',
            'description',
            'priority',
            'status',
            'estimated_minutes',
            'planned_date',
        ]
    widgets = {
            'title': forms.TextInput(attrs={'class': 'form-control'}),
            'assign_to': forms.Select(attrs={'class': 'form-select'}),
            'description': forms.Textarea(attrs={'class': 'form-control', 'rows': 4}),
            'priority': forms.Select(attrs={'class': 'form-select'}),
            'status': forms.Select(attrs={'class': 'form-select'}),
            'estimated_minutes': forms.NumberInput(attrs={'class': 'form-control', 'min': '0'}),
        }

    def clean_planned_date(self):
        planned_date = self.cleaned_data['planned_date']
        if is_holiday_or_sunday(planned_date):
            raise ValidationError("This is a holiday date or Sunday, you cannot add a task on this day.")
        return planned_date
