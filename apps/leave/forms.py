from __future__ import annotations

from typing import Optional, Iterable

from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.utils import timezone

from .models import LeaveRequest, LeaveType, LeaveStatus

User = get_user_model()


# HTML5 datetime-local widget (renders and parses "YYYY-MM-DDTHH:MM")
# We explicitly set format and accepted input formats for reliability across browsers.
_DATETIME_LOCAL_FORMAT = "%Y-%m-%dT%H:%M"
_DATETIME_LOCAL_INPUTS: Iterable[str] = (
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
)

class DateTimeLocalInput(forms.DateTimeInput):
    input_type = "datetime-local"
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("format", _DATETIME_LOCAL_FORMAT)
        super().__init__(*args, **kwargs)


class LeaveRequestForm(forms.ModelForm):
    """
    Employee leave application form.
    - Uses HTML5 datetime-local inputs.
    - Converts naive datetimes to timezone-aware (Asia/Kolkata via settings.TIME_ZONE).
    - Validates end > start and half-day constraints.
    - Optional 'manager' select (can override auto-assignment).
    """

    # Override model fields to wire in custom widgets / input formats
    start_at = forms.DateTimeField(
        widget=DateTimeLocalInput(attrs={"class": "form-control"}),
        input_formats=_DATETIME_LOCAL_INPUTS,
        help_text="Start date & time (IST).",
    )
    end_at = forms.DateTimeField(
        widget=DateTimeLocalInput(attrs={"class": "form-control"}),
        input_formats=_DATETIME_LOCAL_INPUTS,
        help_text="End date & time (IST).",
    )
    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.all().order_by("name"),
        widget=forms.Select(attrs={"class": "form-select"}),
        empty_label="-- Select Leave Type --",
    )
    is_half_day = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        help_text="Half-day must start and end on the same calendar date.",
    )
    reason = forms.CharField(
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3, "placeholder": "Reason for leave..."}),
        help_text="Tell your manager why you need this leave.",
    )
    attachment = forms.FileField(
        required=False,
        widget=forms.ClearableFileInput(attrs={"class": "form-control"}),
        help_text="Optional supporting file (PDF/Image, etc.).",
    )

    # Optional: allow employee to suggest/override manager
    manager = forms.ModelChoiceField(
        queryset=User.objects.none(),  # set in __init__
        required=False,
        widget=forms.Select(attrs={"class": "form-select"}),
        help_text="Optional: choose a specific manager. If left blank, your profile’s manager (or a default) will be used.",
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type",
            "start_at",
            "end_at",
            "is_half_day",
            "reason",
            "attachment",
            "manager",   # optional; model will still auto-assign if empty
        ]

    def __init__(self, *args, **kwargs):
        request_user: Optional[User] = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)

        # Build manager queryset: Manager group users OR superusers, active, not the applicant
        mgr_qs = User.objects.filter(is_active=True)
        try:
            mgr_grp = Group.objects.get(name__iexact="Manager")
            mgr_qs = mgr_qs.filter(forms.models.Q(groups=mgr_grp) | forms.models.Q(is_superuser=True)).distinct()
        except Group.DoesNotExist:
            mgr_qs = mgr_qs.filter(is_superuser=True)

        if request_user and getattr(request_user, "pk", None):
            mgr_qs = mgr_qs.exclude(pk=request_user.pk)

        self.fields["manager"].queryset = mgr_qs.order_by("date_joined", "id")

        # If instance already decided, lock all fields
        if getattr(self.instance, "pk", None) and self.instance.is_decided:
            for f in self.fields.values():
                f.disabled = True

    # --- helpers ---
    @staticmethod
    def _to_aware(dt):
        """Make date-time timezone-aware in the current TZ if it is naive."""
        if not dt:
            return dt
        tz = timezone.get_current_timezone()  # should be Asia/Kolkata per settings
        if timezone.is_naive(dt):
            return timezone.make_aware(dt, tz)
        # normalize to current tz for consistency
        return timezone.localtime(dt, tz)

    # --- cross-field validation ---
    def clean(self):
        cleaned = super().clean()

        start_at = self._to_aware(cleaned.get("start_at"))
        end_at = self._to_aware(cleaned.get("end_at"))
        is_half_day = cleaned.get("is_half_day") is True

        # Push aware values back to cleaned_data so the model's clean() sees aware datetimes
        cleaned["start_at"] = start_at
        cleaned["end_at"] = end_at

        # Basic presence
        if not start_at:
            self.add_error("start_at", "Please provide a start datetime.")
        if not end_at:
            self.add_error("end_at", "Please provide an end datetime.")
        if self.errors:
            return cleaned

        # end > start
        if end_at <= start_at:
            self.add_error("end_at", "End must be after Start.")

        # Half-day rules
        if is_half_day:
            if start_at.date() != end_at.date():
                self.add_error("is_half_day", "Half-day must begin and end on the same calendar date.")
            # Guard unusual long half-day (> 6 hours)
            duration = end_at - start_at
            if duration.total_seconds() > 6 * 3600:
                self.add_error("is_half_day", "Half-day duration should be 6 hours or less.")

        return cleaned
