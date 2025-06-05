from django import forms
from .models import Checklist, Delegation, BulkUpload

class ChecklistForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    reminder_starting_time = forms.TimeField(
        widget=forms.TimeInput(attrs={'type': 'time'}), required=False
    )

    class Meta:
        model = Checklist
        fields = [
            'assign_by', 'task_name', 'assign_to', 'planned_date',
            'priority', 'attachment_mandatory', 'mode', 'frequency',
            'remind_before_days', 'message', 'media_upload', 'assign_pc',
            'group_name', 'notify_to', 'auditor', 'set_reminder',
            'reminder_mode', 'reminder_frequency', 'reminder_before_days',
            'reminder_starting_time', 'checklist_auto_close',
            'checklist_auto_close_days'
        ]

class DelegationForm(forms.ModelForm):
    planned_date = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))
    audio_recording = forms.FileField(
        widget=forms.ClearableFileInput(attrs={'accept': 'audio/*'}),
        required=False
    )

    class Meta:
        model = Delegation
        fields = [
            'assign_by', 'task_name', 'assign_to', 'planned_date',
            'priority', 'attachment_mandatory', 'audio_recording'
        ]

class BulkUploadForm(forms.ModelForm):
    class Meta:
        model = BulkUpload
        fields = ['form_type', 'csv_file']
