from __future__ import annotations

from datetime import datetime, time as dtime
from typing import List, Tuple

import pytz
from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.utils import timezone

from .models import LeaveRequest, LeaveType, HandoverTaskType  # noqa

IST = pytz.timezone("Asia/Kolkata")

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

WORK_START = dtime(9, 30)
WORK_END = dtime(18, 0)


def _ist_date(dt: datetime) -> datetime:
    return timezone.localtime(dt, IST)


def _combine_ist(date_value, t: dtime) -> datetime:
    naive = datetime.combine(date_value, t)
    return IST.localize(naive)


class LeaveRequestForm(forms.ModelForm):
    """
    Employee-facing form used by /leave/apply/.

    Requirement:
      • When Duration = HALF, `leave_type` is OPTIONAL.
      • When Duration = FULL,  `leave_type` is REQUIRED.
    """

    # Helper radio (not stored on the model)
    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial=DURATION_FULL,
        widget=forms.RadioSelect
    )

    # Date-only fields; converted to datetimes in clean()
    start_at = forms.DateField(label="Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)

    # Half-day window (same date)
    from_time = forms.TimeField(
        label="From Time (Half Day)",
        required=False,
        widget=forms.TimeInput(format="%I:%M %p"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (Half Day)",
        required=False,
        widget=forms.TimeInput(format="%I:%M %p"),
        input_formats=["%H:%M", "%I:%M %p"],
    )

    reason = forms.CharField(widget=forms.Textarea(attrs={"rows": 3}), required=False)

    # We keep this NOT required at field level; enforce conditionally in clean()
    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.filter(name__in=ALLOWED_LEAVE_TYPE_NAMES).order_by("name"),
        empty_label="-- Select Type --",
        required=False,
        label="Leave Type",
    )

    attachment = forms.FileField(required=False)

    # Optional handover
    delegate_to = forms.ModelChoiceField(
        queryset=get_user_model().objects.none(), required=False, label="Delegate to"
    )
    handover_checklist = forms.MultipleChoiceField(
        required=False, label="Checklist", widget=forms.CheckboxSelectMultiple
    )
    handover_delegation = forms.MultipleChoiceField(
        required=False, label="Delegation", widget=forms.CheckboxSelectMultiple
    )
    handover_help_ticket = forms.MultipleChoiceField(
        required=False, label="Help Tickets", widget=forms.CheckboxSelectMultiple
    )
    handover_message = forms.CharField(
        required=False, widget=forms.Textarea(attrs={"rows": 2}), label="Message to assignee"
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "duration_type",
            "leave_type",
            "start_at",
            "end_at",
            "from_time",
            "to_time",
            "reason",
            "attachment",
        ]

    def __init__(self, *args, user=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user

        # Delegation dropdown
        User = get_user_model()
        self.fields["delegate_to"].queryset = (
            User.objects.filter(is_active=True).exclude(id=getattr(user, "id", None))
        )

        # Conditional: keep leave_type not required; enforce in clean()
        self.fields["leave_type"].required = False

        self._populate_handover_choices()

    # ----- dynamic handover choices (safe to be empty) -----
    def _populate_handover_choices(self):
        try:
            from apps.tasks.models import Checklist, Delegation, HelpTicket  # type: ignore

            def opt(qs) -> List[Tuple[str, str]]:
                return [(str(obj.pk), getattr(obj, "title", f"#{obj.pk}")) for obj in qs[:200]]

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

    # ----- validation -----
    def clean(self):
        cd = super().clean()

        duration = (cd.get("duration_type") or DURATION_FULL).upper()
        is_half = duration == DURATION_HALF

        # Conditional: leave_type required for FULL day
        if not is_half and not cd.get("leave_type"):
            self.add_error("leave_type", "This field is required for Full Day leave.")

        # Dates & times
        start_date = cd.get("start_at")
        end_date = cd.get("end_at")
        from_time = cd.get("from_time")
        to_time = cd.get("to_time")

        if not start_date:
            self.add_error("start_at", "Please select a date.")
            return cd

        if is_half:
            # times required and within window
            if not (from_time and to_time):
                if not from_time:
                    self.add_error("from_time", "Required for Half Day.")
                if not to_time:
                    self.add_error("to_time", "Required for Half Day.")
                return cd

            if not (WORK_START <= from_time <= WORK_END and WORK_START <= to_time <= WORK_END):
                raise ValidationError("Half-day time must be within 09:30–18:00 IST.")
            if to_time <= from_time:
                raise ValidationError("End time must be after start time for Half Day.")

            dt_start = _combine_ist(start_date, from_time)
            dt_end = _combine_ist(start_date, to_time)
            if (dt_end - dt_start).total_seconds() / 3600.0 > 6.0:
                raise ValidationError("Half-day window cannot exceed 6 hours.")

            cd["_computed_start_at"] = dt_start
            cd["_computed_end_at"] = dt_end
            cd["_computed_is_half_day"] = True
        else:
            # Full day defaults and checks
            if not end_date:
                end_date = start_date
            if end_date < start_date:
                self.add_error("end_at", "End date cannot be before the start date.")
                return cd

            cd["_computed_start_at"] = _combine_ist(start_date, WORK_START)
            cd["_computed_end_at"] = _combine_ist(end_date, WORK_END)
            cd["_computed_is_half_day"] = False

        return cd

    # ----- persistence -----
    def save(self, commit: bool = True) -> LeaveRequest:
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd = self.cleaned_data
        instance = LeaveRequest(
            employee=self.user,
            leave_type=cd.get("leave_type"),  # may be None for half-day (model must allow null)
            start_at=cd["_computed_start_at"],
            end_at=cd["_computed_end_at"],
            is_half_day=cd["_computed_is_half_day"],
            reason=cd.get("reason") or "",
            attachment=cd.get("attachment"),
        )
        if commit:
            instance.save()
        return instance


# ---------------- Admin edit form ----------------

class AdminLeaveEditForm(forms.ModelForm):
    """
    Admin-facing edit with the same conditional rule for leave_type.
    """
    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES, initial=DURATION_FULL, widget=forms.RadioSelect
    )
    start_at = forms.DateField(label="Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)
    from_time = forms.TimeField(label="From Time (Half Day)", required=False, input_formats=["%H:%M", "%I:%M %p"])
    to_time = forms.TimeField(label="To Time (Half Day)", required=False, input_formats=["%H:%M", "%I:%M %p"])

    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.filter(name__in=ALLOWED_LEAVE_TYPE_NAMES).order_by("name"),
        empty_label="-- Select Type --",
        required=False,
        label="Leave Type",
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "duration_type",
            "leave_type",
            "start_at",
            "end_at",
            "from_time",
            "to_time",
            "status",
            "reason",
            "attachment",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initialize duration and date/time displays from instance
        if self.instance and self.instance.pk:
            is_half = bool(getattr(self.instance, "is_half_day", False))
            self.fields["duration_type"].initial = DURATION_HALF if is_half else DURATION_FULL

            ist_start = _ist_date(self.instance.start_at)
            ist_end = _ist_date(self.instance.end_at)
            self.fields["start_at"].initial = ist_start.date()
            self.fields["end_at"].initial = ist_end.date()
            if is_half:
                self.fields["from_time"].initial = ist_start.time().replace(second=0, microsecond=0)
                self.fields["to_time"].initial = ist_end.time().replace(second=0, microsecond=0)

        self.fields["leave_type"].required = False  # conditional in clean()

    def clean(self):
        cd = super().clean()
        duration = (cd.get("duration_type") or DURATION_FULL).upper()
        is_half = duration == DURATION_HALF

        if not is_half and not cd.get("leave_type"):
            self.add_error("leave_type", "This field is required for Full Day leave.")

        start_date = cd.get("start_at")
        end_date = cd.get("end_at")
        from_time = cd.get("from_time")
        to_time = cd.get("to_time")

        if not start_date:
            self.add_error("start_at", "Please select a date.")
            return cd

        if is_half:
            if not (from_time and to_time):
                if not from_time:
                    self.add_error("from_time", "Required for Half Day.")
                if not to_time:
                    self.add_error("to_time", "Required for Half Day.")
                return cd

            if not (WORK_START <= from_time <= WORK_END and WORK_START <= to_time <= WORK_END):
                raise ValidationError("Half-day time must be within 09:30–18:00 IST.")
            if to_time <= from_time:
                raise ValidationError("End time must be after start time for Half Day.")

            dt_start = _combine_ist(start_date, from_time)
            dt_end = _combine_ist(start_date, to_time)
            if (dt_end - dt_start).total_seconds() / 3600.0 > 6.0:
                raise ValidationError("Half-day window cannot exceed 6 hours.")

            cd["_computed_start_at"] = dt_start
            cd["_computed_end_at"] = dt_end
            cd["_computed_is_half_day"] = True
        else:
            if not end_date:
                end_date = start_date
            if end_date < start_date:
                self.add_error("end_at", "End date cannot be before the start date.")
                return cd

            cd["_computed_start_at"] = _combine_ist(start_date, WORK_START)
            cd["_computed_end_at"] = _combine_ist(end_date, WORK_END)
            cd["_computed_is_half_day"] = False

        return cd

    def save(self, commit: bool = True) -> LeaveRequest:
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd = self.cleaned_data
        inst: LeaveRequest = super().save(commit=False)
        inst.leave_type = cd.get("leave_type")  # may be None for half-day
        inst.start_at = cd["_computed_start_at"]
        inst.end_at = cd["_computed_end_at"]
        inst.is_half_day = cd["_computed_is_half_day"]
        if commit:
            inst.save()
        return inst
