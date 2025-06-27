from django import forms
from .models import Checklist, Delegation, BulkUpload

class ChecklistForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    reminder_starting_time = forms.TimeField(
        widget=forms.TimeInput(attrs={'type': 'time'}),
        required=False
    )

    class Meta:
        model = Checklist
        fields = [
            'assign_by', 'task_name', 'assign_to', 'planned_date',
            'priority', 'attachment_mandatory', 'mode', 'frequency',
            'time_per_task_minutes',    # <— new
            'remind_before_days', 'message',
            'media_upload', 'assign_pc', 'group_name',
            'notify_to', 'auditor', 'set_reminder',
            'reminder_mode', 'reminder_frequency',
            'reminder_before_days', 'reminder_starting_time',
            'checklist_auto_close', 'checklist_auto_close_days',
        ]
        widgets = {
            'assign_by': forms.Select(attrs={'class':'form-select'}),
            'task_name': forms.TextInput(attrs={'class':'form-control'}),
            'assign_to': forms.Select(attrs={'class':'form-select'}),
            'priority': forms.Select(attrs={'class':'form-select'}),
            'attachment_mandatory': forms.CheckboxInput(attrs={'class':'form-check-input'}),
            'mode': forms.Select(attrs={'class':'form-select'}),
            'frequency': forms.NumberInput(attrs={'class':'form-control'}),
            'time_per_task_minutes': forms.NumberInput(attrs={'class':'form-control','min':'0'}),
            'remind_before_days': forms.NumberInput(attrs={'class':'form-control'}),
            'message': forms.Textarea(attrs={'class':'form-control','rows':3}),
            'media_upload': forms.ClearableFileInput(attrs={'class':'form-control'}),
            'assign_pc': forms.Select(attrs={'class':'form-select'}),
            'group_name': forms.TextInput(attrs={'class':'form-control'}),
            'notify_to': forms.Select(attrs={'class':'form-select'}),
            'auditor': forms.Select(attrs={'class':'form-select'}),
            'set_reminder': forms.CheckboxInput(attrs={'class':'form-check-input'}),
            'reminder_mode': forms.Select(attrs={'class':'form-select'}),
            'reminder_frequency': forms.NumberInput(attrs={'class':'form-control'}),
            'reminder_before_days': forms.NumberInput(attrs={'class':'form-control'}),
            'checklist_auto_close': forms.CheckboxInput(attrs={'class':'form-check-input'}),
            'checklist_auto_close_days': forms.NumberInput(attrs={'class':'form-control'}),
        }

class DelegationForm(forms.ModelForm):
    planned_date = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date'})
    )
    audio_recording = forms.FileField(
        widget=forms.ClearableFileInput(attrs={'accept': 'audio/*'}),
        required=False
    )
    time_per_task_minutes = forms.IntegerField(
        label="Time per Task (minutes)",
        min_value=0,
        help_text="How many minutes should this delegation take?"
    )

    class Meta:
        model = Delegation
        fields = [
            'assign_by', 'task_name', 'assign_to',
            'planned_date', 'priority',
            'attachment_mandatory', 'audio_recording',
            'time_per_task_minutes',   # <— new
        ]

        widgets = {
            'assign_by': forms.Select(attrs={'class':'form-select'}),
            'task_name': forms.TextInput(attrs={'class':'form-control'}),
            'assign_to': forms.Select(attrs={'class':'form-select'}),
            'priority': forms.Select(attrs={'class':'form-select'}),
            'attachment_mandatory': forms.CheckboxInput(attrs={'class':'form-check-input'}),
        }

class BulkUploadForm(forms.ModelForm):
    class Meta:
        model = BulkUpload
        fields = ['form_type', 'csv_file']
        widgets = {
            'form_type': forms.Select(attrs={'class':'form-select'}),
            'csv_file': forms.ClearableFileInput(attrs={'class':'form-control'}),
        }
