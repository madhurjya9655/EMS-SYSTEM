from __future__ import annotations

from django import forms
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
from zoneinfo import ZoneInfo

from .models import LeaveRequest


class LeaveRequestForm(forms.ModelForm):
    # Use HTML5 datetime-local; accept common formats from browsers
    start_at = forms.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"],
        required=True,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        label="Start Date & Time",
    )
    end_at = forms.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"],
        required=True,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        label="End Date & Time",
    )

    class Meta:
        model = LeaveRequest
        fields = ["leave_type", "start_at", "end_at", "is_half_day", "reason", "attachment"]
        widgets = {
            "leave_type": forms.Select(attrs={"class": "form-select"}),
            "is_half_day": forms.CheckboxInput(attrs={"class": "form-check-input"}),
            "reason": forms.Textarea(attrs={"class": "form-control", "rows": 4, "placeholder": "Reason for leave..."}),
            "attachment": forms.ClearableFileInput(
                attrs={"class": "form-control", "accept": ".pdf,.jpg,.jpeg,.png,.doc,.docx,.txt"}
            ),
        }
        labels = {
            "leave_type": "Type",
            "is_half_day": "Half day",
            "reason": "Reason",
            "attachment": "Attachment (optional)",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Helpful placeholders for browsers that don't render datetime-local nicely
        self.fields["start_at"].widget.attrs.setdefault("placeholder", "YYYY-MM-DDTHH:MM")
        self.fields["end_at"].widget.attrs.setdefault("placeholder", "YYYY-MM-DDTHH:MM")

    # ---- Validation ----

    def clean(self):
        cleaned = super().clean()
        start = cleaned.get("start_at")
        end = cleaned.get("end_at")
        is_half_day = cleaned.get("is_half_day")

        # Convert naive inputs from <input type="datetime-local"> to aware IST
        tz = ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))
        if start and timezone.is_naive(start):
            start = timezone.make_aware(start, tz)
        if end and timezone.is_naive(end):
            end = timezone.make_aware(end, tz)
        cleaned["start_at"] = start
        cleaned["end_at"] = end

        errors = {}

        if start and end:
            if end <= start:
                errors["end_at"] = "End must be after Start."

            if is_half_day:
                # Half-day implies same calendar date and short duration unless explicit short times provided
                if start.date() != end.date():
                    errors["is_half_day"] = "Half-day leave must start and end on the same date."
                elif (end - start) > timezone.timedelta(hours=6):
                    errors["is_half_day"] = "Half-day duration should be 6 hours or less."

        if errors:
            raise ValidationError(errors)
        return cleaned

    def clean_attachment(self):
        f = self.cleaned_data.get("attachment")
        if not f:
            return f
        # Basic size/type validation only (storage/backends handle the rest)
        max_mb = getattr(settings, "MAX_LEAVE_ATTACHMENT_MB", 10)
        if getattr(f, "size", 0) > max_mb * 1024 * 1024:
            raise ValidationError(f"Attachment too large (>{max_mb} MB).")
        allowed = {".pdf", ".jpg", ".jpeg", ".png", ".doc", ".docx", ".txt"}
        name = (getattr(f, "name", "") or "").lower()
        ext = "." + name.rsplit(".", 1)[-1] if "." in name else ""
        if ext and ext not in allowed:
            raise ValidationError("Unsupported file type.")
        return f
