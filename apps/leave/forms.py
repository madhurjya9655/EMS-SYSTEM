# FILE: apps/leave/forms.py
# PURPOSE: Date-only leave (remove time-based leave restrictions)
# UPDATED: 2026-02-25

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import List, Tuple

import pytz
from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.utils import timezone

from .models import LeaveRequest, LeaveType  # noqa

IST = pytz.timezone("Asia/Kolkata")

# Backward compatibility only (ignored)
DURATION_FULL = "FULL"
DURATION_HALF = "HALF"
DURATION_CHOICES = (
    (DURATION_FULL, "Full Day"),
    (DURATION_HALF, "Half Day"),
)

ALLOWED_LEAVE_TYPE_NAMES = {
    "Compensatory Off",
    "Casual Leave",
    "Maternity Leave",
}

# Date-only normalization boundaries (IST)
FULL_DAY_START = dtime(0, 0, 0)
FULL_DAY_END = dtime(23, 59, 59)


def _combine_ist(date_value, t: dtime) -> datetime:
    naive = datetime.combine(date_value, t)
    return IST.localize(naive)


def _ist_local(dt: datetime) -> datetime:
    return timezone.localtime(dt, IST)


class LeaveRequestForm(forms.ModelForm):
    """
    Employee-facing form used by /leave/apply/.

    RULES:
      - Date-only leave (no working-hours time restrictions).
      - Validate only: end_date >= start_date.
      - If end_date missing => treat as same as start_date.
      - Convert to IST-aware datetimes spanning the full day.
    """

    # Backward compatible (ignored)
    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial=DURATION_FULL,
        widget=forms.RadioSelect,
        required=False,
    )

    # Date-only inputs
    start_at = forms.DateField(label="Start Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)

    # Backward compatible (ignored)
    from_time = forms.TimeField(
        label="From Time (Deprecated)",
        required=False,
        widget=forms.TimeInput(format="%I:%M %p"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (Deprecated)",
        required=False,
        widget=forms.TimeInput(format="%I:%M %p"),
        input_formats=["%H:%M", "%I:%M %p"],
    )

    reason = forms.CharField(widget=forms.Textarea(attrs={"rows": 3}), required=False)

    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.filter(name__in=ALLOWED_LEAVE_TYPE_NAMES).order_by("name"),
        empty_label="-- Select Type --",
        required=True,
        label="Leave Type",
    )

    attachment = forms.FileField(required=False)

    # Optional handover
    delegate_to = forms.ModelChoiceField(
        queryset=get_user_model().objects.none(),
        required=False,
        label="Delegate to"
    )
    handover_checklist = forms.MultipleChoiceField(
        required=False,
        label="Checklist",
        widget=forms.CheckboxSelectMultiple
    )
    handover_delegation = forms.MultipleChoiceField(
        required=False,
        label="Delegation",
        widget=forms.CheckboxSelectMultiple
    )
    handover_help_ticket = forms.MultipleChoiceField(
        required=False,
        label="Help Tickets",
        widget=forms.CheckboxSelectMultiple
    )
    handover_message = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 2}),
        label="Message to assignee"
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type",
            "start_at",
            "end_at",
            "reason",
            "attachment",
            # Backward compatible (ignored):
            "duration_type",
            "from_time",
            "to_time",
        ]

    def __init__(self, *args, user=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user

        User = get_user_model()
        self.fields["delegate_to"].queryset = (
            User.objects.filter(is_active=True).exclude(id=getattr(user, "id", None))
        )

        self._populate_handover_choices()

    def _populate_handover_choices(self):
        """
        Note: these choices are NOT range-filtered here (range-filtering usually needs dates).
        Your view already does skip/handovers with the computed window after save.
        """
        try:
            from apps.tasks.models import Checklist, Delegation, HelpTicket  # type: ignore

            def opt(qs) -> List[Tuple[str, str]]:
                out: List[Tuple[str, str]] = []
                for obj in qs[:200]:
                    label = (
                        getattr(obj, "task_name", None)
                        or getattr(obj, "title", None)
                        or getattr(obj, "name", None)
                        or f"#{obj.pk}"
                    )
                    out.append((str(obj.pk), str(label)))
                return out

            base = {"assign_to": self.user} if self.user else {}
            self.fields["handover_checklist"].choices = opt(
                Checklist.objects.filter(**base).order_by("-id")
            )
            self.fields["handover_delegation"].choices = opt(
                Delegation.objects.filter(**base).order_by("-id")
            )
            self.fields["handover_help_ticket"].choices = opt(
                HelpTicket.objects.filter(**base).order_by("-id")
            )
        except Exception:
            self.fields["handover_checklist"].choices = []
            self.fields["handover_delegation"].choices = []
            self.fields["handover_help_ticket"].choices = []

    def clean(self):
        cd = super().clean()

        if not cd.get("leave_type"):
            self.add_error("leave_type", "Please select a leave type.")

        start_date = cd.get("start_at")
        end_date = cd.get("end_at")

        if not start_date:
            self.add_error("start_at", "Please select a start date.")
            return cd

        if not end_date:
            end_date = start_date

        if end_date < start_date:
            self.add_error("end_at", "End date must be on or after start date.")
            return cd

        cd["_computed_start_at"] = _combine_ist(start_date, FULL_DAY_START)
        cd["_computed_end_at"] = _combine_ist(end_date, FULL_DAY_END)
        cd["_computed_is_half_day"] = False  # date-only system

        return cd

    def _post_clean(self):
        """
        Inject computed IST-aware datetimes into the instance before model.clean() runs.
        """
        cd = getattr(self, "cleaned_data", {}) or {}

        if "_computed_start_at" in cd and "_computed_end_at" in cd:
            aware_start = cd["_computed_start_at"]
            aware_end = cd["_computed_end_at"]

            # Replace form values (DateField -> DateTime) so model validation sees datetimes.
            self.cleaned_data["start_at"] = aware_start
            self.cleaned_data["end_at"] = aware_end

            self.instance.start_at = aware_start
            self.instance.end_at = aware_end
            self.instance.is_half_day = bool(cd.get("_computed_is_half_day", False))

        if self.user and not getattr(self.instance, "employee_id", None):
            self.instance.employee = self.user

        super()._post_clean()

    def save(self, commit: bool = True) -> LeaveRequest:
        """
        Persist using the computed date span.
        (M2M handover selections are handled by your view logic.)
        """
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd = self.cleaned_data

        instance = LeaveRequest(
            employee=self.user,
            leave_type=cd.get("leave_type"),
            start_at=cd["_computed_start_at"],
            end_at=cd["_computed_end_at"],
            is_half_day=cd.get("_computed_is_half_day", False),
            reason=cd.get("reason") or "",
            attachment=cd.get("attachment"),
        )

        if commit:
            instance.save()
        return instance


class AdminLeaveEditForm(forms.ModelForm):
    """
    Admin edit form (date-only).
    """

    # Backward compatible (ignored)
    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES, initial=DURATION_FULL, widget=forms.RadioSelect, required=False
    )

    start_at = forms.DateField(label="Start Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)

    # Backward compatible (ignored)
    from_time = forms.TimeField(
        label="From Time (Deprecated)", required=False, input_formats=["%H:%M", "%I:%M %p"]
    )
    to_time = forms.TimeField(
        label="To Time (Deprecated)", required=False, input_formats=["%H:%M", "%I:%M %p"]
    )

    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.filter(name__in=ALLOWED_LEAVE_TYPE_NAMES).order_by("name"),
        empty_label="-- Select Type --",
        required=True,
        label="Leave Type",
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type",
            "start_at",
            "end_at",
            "status",
            "reason",
            "attachment",
            # Backward compatible (ignored):
            "duration_type",
            "from_time",
            "to_time",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.instance and self.instance.pk:
            ist_start = _ist_local(self.instance.start_at)
            ist_end = _ist_local(self.instance.end_at)
            self.fields["start_at"].initial = ist_start.date()
            self.fields["end_at"].initial = ist_end.date()
            self.fields["duration_type"].initial = DURATION_FULL

    def clean(self):
        cd = super().clean()

        if not cd.get("leave_type"):
            self.add_error("leave_type", "Please select a leave type.")

        start_date = cd.get("start_at")
        end_date = cd.get("end_at")

        if not start_date:
            self.add_error("start_at", "Please select a start date.")
            return cd

        if not end_date:
            end_date = start_date

        if end_date < start_date:
            self.add_error("end_at", "End date must be on or after start date.")
            return cd

        cd["_computed_start_at"] = _combine_ist(start_date, FULL_DAY_START)
        cd["_computed_end_at"] = _combine_ist(end_date, FULL_DAY_END)
        cd["_computed_is_half_day"] = False

        return cd

    def _post_clean(self):
        cd = getattr(self, "cleaned_data", {}) or {}
        if "_computed_start_at" in cd and "_computed_end_at" in cd:
            aware_start = cd["_computed_start_at"]
            aware_end = cd["_computed_end_at"]
            self.cleaned_data["start_at"] = aware_start
            self.cleaned_data["end_at"] = aware_end
            self.instance.start_at = aware_start
            self.instance.end_at = aware_end
            self.instance.is_half_day = bool(cd.get("_computed_is_half_day", False))
        super()._post_clean()

    def save(self, commit: bool = True) -> LeaveRequest:
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd = self.cleaned_data
        inst: LeaveRequest = super().save(commit=False)
        inst.leave_type = cd.get("leave_type")
        inst.start_at = cd["_computed_start_at"]
        inst.end_at = cd["_computed_end_at"]
        inst.is_half_day = cd.get("_computed_is_half_day", False)
        if commit:
            inst.save()
        return inst