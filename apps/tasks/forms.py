from __future__ import annotations

import datetime
from django import forms
from django.core.exceptions import ValidationError
from django.contrib.auth import get_user_model

from .models import Checklist, Delegation, BulkUpload, HelpTicket
from apps.settings.models import Holiday

User = get_user_model()


# -----------------------------
# Helpers
# -----------------------------
def is_holiday_or_sunday(date_val: datetime.date | datetime.datetime) -> bool:
    """
    Returns True if the given date is Sunday or a configured holiday.
    Accepts date or datetime.
    """
    if isinstance(date_val, datetime.datetime):
        date_val = date_val.date()
    return date_val.weekday() == 6 or Holiday.objects.filter(date=date_val).exists()


# -----------------------------
# Checklist
# -----------------------------
class ChecklistForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        help_text="Select date and time for this task",
    )
    reminder_starting_time = forms.TimeField(
        widget=forms.TimeInput(attrs={"type": "time", "class": "form-control"}),
        required=False,
        help_text="Time to start sending reminders",
    )

    class Meta:
        model = Checklist
        fields = [
            "assign_by", "task_name", "assign_to", "planned_date",
            "priority", "attachment_mandatory", "mode", "frequency",
            "time_per_task_minutes", "remind_before_days", "message",
            "media_upload", "assign_pc", "group_name", "notify_to", "auditor",
            "set_reminder", "reminder_mode", "reminder_frequency",
            "reminder_starting_time",
            "checklist_auto_close", "checklist_auto_close_days",
        ]
        widgets = {
            "assign_by": forms.Select(attrs={"class": "form-select"}),
            "task_name": forms.TextInput(attrs={"class": "form-control", "placeholder": "Enter task name"}),
            "assign_to": forms.Select(attrs={"class": "form-select"}),
            "priority": forms.Select(attrs={"class": "form-select"}),
            "attachment_mandatory": forms.CheckboxInput(attrs={"class": "form-check-input"}),
            "mode": forms.Select(attrs={"class": "form-select"}),
            "frequency": forms.NumberInput(attrs={"class": "form-control", "min": "1", "placeholder": "e.g., 1"}),
            "time_per_task_minutes": forms.NumberInput(attrs={"class": "form-control", "min": "0", "placeholder": "Minutes"}),
            "remind_before_days": forms.NumberInput(attrs={"class": "form-control", "min": "0", "placeholder": "Days"}),
            "message": forms.Textarea(attrs={"class": "form-control", "rows": 3, "placeholder": "Task description or instructions"}),
            "media_upload": forms.ClearableFileInput(attrs={"class": "form-control"}),
            "assign_pc": forms.Select(attrs={"class": "form-select"}),
            "group_name": forms.TextInput(attrs={"class": "form-control", "placeholder": "Group or category name"}),
            "notify_to": forms.Select(attrs={"class": "form-select"}),
            "auditor": forms.Select(attrs={"class": "form-select"}),
            "set_reminder": forms.CheckboxInput(attrs={"class": "form-check-input"}),
            "reminder_mode": forms.Select(attrs={"class": "form-select"}),
            "reminder_frequency": forms.NumberInput(attrs={"class": "form-control", "min": "1"}),
            "checklist_auto_close": forms.CheckboxInput(attrs={"class": "form-check-input"}),
            "checklist_auto_close_days": forms.NumberInput(attrs={"class": "form-control", "min": "0"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        active_users = User.objects.filter(is_active=True).order_by("username")
        for fld in ("assign_by", "assign_to", "assign_pc", "notify_to", "auditor"):
            if fld in self.fields:
                self.fields[fld].queryset = active_users

        # Mark a few fields as optional in the UI
        for fld in ("assign_pc", "notify_to", "auditor", "media_upload", "group_name", "message"):
            if fld in self.fields:
                self.fields[fld].required = False

    def clean_planned_date(self):
        # Views handle time normalization; just return the value
        return self.cleaned_data["planned_date"]

    def clean(self):
        cleaned = super().clean()
        mode = cleaned.get("mode")
        freq = cleaned.get("frequency")

        if mode and mode != "" and (not freq or int(freq) < 1):
            self.add_error("frequency", "Frequency must be at least 1 when a recurrence mode is selected.")

        for field_name in ("time_per_task_minutes", "remind_before_days", "reminder_frequency", "checklist_auto_close_days"):
            val = cleaned.get(field_name)
            if val is not None and int(val) < 0:
                self.add_error(field_name, "Must be a non-negative number.")

        if cleaned.get("set_reminder"):
            if not cleaned.get("reminder_mode"):
                self.add_error("reminder_mode", "Reminder mode is required when reminders are enabled.")
            rf = cleaned.get("reminder_frequency")
            if not rf or int(rf) < 1:
                self.add_error("reminder_frequency", "Reminder frequency must be at least 1 when reminders are enabled.")

        return cleaned


class CompleteChecklistForm(forms.ModelForm):
    class Meta:
        model = Checklist
        fields = ["doer_file", "doer_notes"]
        widgets = {
            "doer_file": forms.ClearableFileInput(attrs={"class": "form-control"}),
            "doer_notes": forms.Textarea(
                attrs={"class": "form-control", "rows": 4, "placeholder": "Add any notes about completing this task..."}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["doer_file"].required = bool(getattr(self.instance, "attachment_mandatory", False))
        self.fields["doer_notes"].required = False


# -----------------------------
# Delegation
# -----------------------------
class DelegationForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        help_text="Select date and time for this delegation",
    )
    audio_recording = forms.FileField(
        widget=forms.ClearableFileInput(attrs={"accept": "audio/*", "class": "form-control"}),
        required=False,
        help_text="Optional audio recording with instructions",
    )
    time_per_task_minutes = forms.IntegerField(
        label="Time per Task (minutes)",
        min_value=0,
        help_text="How many minutes should this delegation take?",
        widget=forms.NumberInput(attrs={"class": "form-control", "placeholder": "Minutes"}),
    )

    class Meta:
        model = Delegation
        fields = [
            "assign_by", "task_name", "assign_to",
            "planned_date", "priority",
            "attachment_mandatory", "audio_recording",
            "time_per_task_minutes", "mode", "frequency",
        ]
        widgets = {
            "assign_by": forms.Select(attrs={"class": "form-select"}),
            "task_name": forms.TextInput(attrs={"class": "form-control", "placeholder": "Enter delegation task name"}),
            "assign_to": forms.Select(attrs={"class": "form-select"}),
            "priority": forms.Select(attrs={"class": "form-select"}),
            "attachment_mandatory": forms.CheckboxInput(attrs={"class": "form-check-input"}),
            "mode": forms.Select(attrs={"class": "form-select"}),
            "frequency": forms.NumberInput(attrs={"class": "form-control", "min": "1"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        active_users = User.objects.filter(is_active=True).order_by("username")
        self.fields["assign_by"].queryset = active_users
        self.fields["assign_to"].queryset = active_users

        for fld in ("audio_recording", "mode", "frequency"):
            if fld in self.fields:
                self.fields[fld].required = False

    def clean_planned_date(self):
        return self.cleaned_data["planned_date"]

    def clean(self):
        cleaned = super().clean()
        mode = cleaned.get("mode")
        freq = cleaned.get("frequency")

        if mode and mode != "" and (not freq or int(freq) < 1):
            self.add_error("frequency", "Frequency must be at least 1 when a recurrence mode is selected.")

        tpt = cleaned.get("time_per_task_minutes")
        if tpt is not None and int(tpt) < 0:
            self.add_error("time_per_task_minutes", "Time per task must be non-negative.")

        return cleaned


class CompleteDelegationForm(forms.ModelForm):
    class Meta:
        model = Delegation
        fields = ["doer_file", "doer_notes"]
        widgets = {
            "doer_file": forms.ClearableFileInput(attrs={"class": "form-control"}),
            "doer_notes": forms.Textarea(
                attrs={"class": "form-control", "rows": 4, "placeholder": "Add any notes about completing this delegation..."}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["doer_file"].required = bool(getattr(self.instance, "attachment_mandatory", False))
        self.fields["doer_notes"].required = False


# -----------------------------
# Bulk Upload
# -----------------------------
class BulkUploadForm(forms.ModelForm):
    """
    Minimal model-backed form so your upload can be audited (if BulkUpload model persists entries).
    If you don’t want DB persistence, switch this to a simple forms.Form with the same fields/validation.
    """
    class Meta:
        model = BulkUpload
        fields = ["form_type", "csv_file"]
        widgets = {
            "form_type": forms.Select(attrs={"class": "form-select"}),
            "csv_file": forms.ClearableFileInput(attrs={"class": "form-control", "accept": ".csv,.xlsx,.xls"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["csv_file"].help_text = "Upload a CSV or Excel file (.csv, .xlsx, .xls). Max 10MB."

    def clean_csv_file(self):
        csv_file = self.cleaned_data.get("csv_file")
        if not csv_file:
            raise ValidationError("Please upload a file.")

        allowed_extensions = {".csv", ".xlsx", ".xls"}
        ext = "." + csv_file.name.split(".")[-1].lower()
        if ext not in allowed_extensions:
            raise ValidationError(f"Invalid file type. Allowed: {', '.join(sorted(allowed_extensions))}")

        # 10 MB guard
        size = getattr(csv_file, "size", None)
        if size and size > 10 * 1024 * 1024:
            raise ValidationError("File too large (max 10MB).")

        return csv_file


# -----------------------------
# Help Ticket
# -----------------------------
class HelpTicketForm(forms.ModelForm):
    planned_date = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        help_text="Select date and time for this help ticket",
    )
    media_upload = forms.FileField(
        widget=forms.ClearableFileInput(attrs={"class": "form-control"}),
        required=False,
        help_text="Upload any relevant files or screenshots",
    )

    class Meta:
        model = HelpTicket
        fields = [
            "title",
            "assign_to",
            "media_upload",
            "description",
            "priority",
            "status",
            "estimated_minutes",
            "planned_date",
        ]
        widgets = {
            "title": forms.TextInput(attrs={"class": "form-control", "placeholder": "Enter ticket title"}),
            "assign_to": forms.Select(attrs={"class": "form-select"}),
            "description": forms.Textarea(
                attrs={"class": "form-control", "rows": 4, "placeholder": "Describe the issue or request in detail..."}
            ),
            "priority": forms.Select(attrs={"class": "form-select"}),
            "status": forms.Select(attrs={"class": "form-select"}),
            "estimated_minutes": forms.NumberInput(
                attrs={"class": "form-control", "min": "0", "placeholder": "Estimated time in minutes"}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        active_users = User.objects.filter(is_active=True).order_by("username")
        self.fields["assign_to"].queryset = active_users

        for fld in ("media_upload", "estimated_minutes"):
            if fld in self.fields:
                self.fields[fld].required = False

    def clean_planned_date(self):
        planned_date = self.cleaned_data["planned_date"]
        if is_holiday_or_sunday(planned_date):
            raise ValidationError("This is a holiday or Sunday — you cannot add a ticket on this day.")
        return planned_date

    def clean_estimated_minutes(self):
        v = self.cleaned_data.get("estimated_minutes")
        if v is not None and v < 0:
            raise ValidationError("Estimated minutes must be non-negative.")
        return v

    def clean(self):
        cleaned = super().clean()
        title = cleaned.get("title")
        description = cleaned.get("description")

        if title and len(title.strip()) < 3:
            self.add_error("title", "Title must be at least 3 characters long.")

        if description and len(description.strip()) < 10:
            self.add_error("description", "Description must be at least 10 characters long.")

        return cleaned


# -----------------------------
# Filters
# -----------------------------
class ChecklistFilterForm(forms.Form):
    keyword = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Search task name or message..."}),
    )
    assign_to = forms.ModelChoiceField(
        queryset=User.objects.filter(is_active=True).order_by("username"),
        required=False,
        empty_label="All Users",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    priority = forms.ChoiceField(
        choices=[("", "All Priorities")] + Checklist._meta.get_field("priority").choices,
        required=False,
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    status = forms.ChoiceField(
        choices=[("", "All Statuses")] + Checklist.STATUS_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    start_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
    )
    end_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
    )
    today_only = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
    )
