# FILE: apps/leave/forms.py
# PURPOSE: Leave module forms — employee apply + admin edit
# UPDATED: 2026-03-14
# CHANGE:  Added attrs={"type": "time"} to TimeInput widgets for from_time and
#          to_time in BOTH LeaveRequestForm and AdminLeaveEditForm.
#          Without this, some browser/Django combinations render the widget as
#          type="text", allowing free-form strings (e.g. "2:00PM" without a
#          space) that fail all input_formats → "Enter a valid time".
#          With type="time" the browser always submits clean HH:MM (24-hour)
#          which is unambiguously parsed by the "%H:%M" input_format.
#          No business logic changed.

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import List, Tuple
from zoneinfo import ZoneInfo

from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.utils import timezone

from .models import LeaveRequest, LeaveType  # noqa

IST = ZoneInfo("Asia/Kolkata")

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

CASUAL_LEAVE_NAME = "Casual Leave"

# Full-day normalization boundaries (kept for real full-day leaves only)
FULL_DAY_START = dtime(0, 0, 0)
FULL_DAY_END = dtime(23, 59, 59)

# Official office hours for time-based leave
OFFICE_START = dtime(10, 0, 0)
OFFICE_END = dtime(18, 30, 0)


def _combine_ist(date_value, t: dtime) -> datetime:
    naive = datetime.combine(date_value, t)
    return naive.replace(tzinfo=IST)


def _ist_local(dt: datetime) -> datetime:
    return timezone.localtime(dt, IST)


def _is_casual_leave(leave_type) -> bool:
    return bool(leave_type and getattr(leave_type, "name", "") == CASUAL_LEAVE_NAME)


def _is_time_based_request(
    leave_type, duration_type: str, from_time: dtime | None, to_time: dtime | None
) -> bool:
    """
    Exact-time leave should be honored when:
    - duration_type is explicitly HALF, or
    - leave type is Casual Leave and any time input was provided.

    This makes the fix resilient even if the template/view fails to post
    duration_type correctly but does post the selected times.
    """
    raw_duration = (duration_type or DURATION_FULL).strip().upper()
    if raw_duration == DURATION_HALF:
        return True
    if _is_casual_leave(leave_type) and (from_time is not None or to_time is not None):
        return True
    return False


def _validate_time_window(from_time: dtime, to_time: dtime) -> None:
    if from_time < OFFICE_START:
        raise ValidationError({"from_time": "Start time must be 10:00 AM or later."})
    if to_time > OFFICE_END:
        raise ValidationError({"to_time": "End time must be 06:30 PM or earlier."})
    if to_time <= from_time:
        raise ValidationError({"to_time": "End time must be after start time."})


def _resolve_datetimes(
    *,
    leave_type,
    start_date,
    end_date,
    duration_type: str,
    from_time: dtime | None,
    to_time: dtime | None,
) -> Tuple[datetime, datetime, bool]:
    """
    Returns (computed_start_at, computed_end_at, is_half_day).

    Rules:
    - Full-day leave uses 00:00:00 -> 23:59:59
    - Time-based leave stores exact submitted times
    - Time-based leave must be single-day and within office hours
    """
    is_time_based = _is_time_based_request(leave_type, duration_type, from_time, to_time)

    if not is_time_based:
        return (
            _combine_ist(start_date, FULL_DAY_START),
            _combine_ist(end_date, FULL_DAY_END),
            False,
        )

    if from_time is None:
        raise ValidationError({"from_time": "Please select a start time."})
    if to_time is None:
        raise ValidationError({"to_time": "Please select an end time."})
    if end_date != start_date:
        raise ValidationError(
            {
                "end_at": (
                    "Time-based leave must start and end on the same day. "
                    "Select 'Full Day' for multi-day leave."
                )
            }
        )

    _validate_time_window(from_time, to_time)

    return (
        _combine_ist(start_date, from_time),
        _combine_ist(end_date, to_time),
        True,
    )


class LeaveRequestForm(forms.ModelForm):
    """
    Employee-facing form used by /leave/apply/.

    RULES:
      - Full-day leave uses full-day bounds.
      - Casual Leave with entered times stores those exact times.
      - Half-day/time-based leave must be single-day and within office hours.
      - If end_date missing → treat as same as start_date.
    """

    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial=DURATION_FULL,
        widget=forms.RadioSelect,
        required=False,
    )

    start_at = forms.DateField(label="Start Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)

    # ── FIX: attrs={"type": "time"} forces browser to submit clean HH:MM
    #         (24-hour) regardless of browser locale.  Without this, some
    #         builds render type="text", allowing free-form strings that fail
    #         all input_formats → "Enter a valid time" errors. ──────────────
    from_time = forms.TimeField(
        label="From Time (IST)",
        required=False,
        widget=forms.TimeInput(attrs={"type": "time"}, format="%H:%M"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (IST)",
        required=False,
        widget=forms.TimeInput(attrs={"type": "time"}, format="%H:%M"),
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

    delegate_to = forms.ModelChoiceField(
        queryset=get_user_model().objects.none(),
        required=False,
        label="Delegate to",
    )
    handover_checklist = forms.MultipleChoiceField(
        required=False,
        label="Checklist",
        widget=forms.CheckboxSelectMultiple,
    )
    handover_delegation = forms.MultipleChoiceField(
        required=False,
        label="Delegation",
        widget=forms.CheckboxSelectMultiple,
    )
    handover_help_ticket = forms.MultipleChoiceField(
        required=False,
        label="Help Tickets",
        widget=forms.CheckboxSelectMultiple,
    )
    handover_message = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 2}),
        label="Message to assignee",
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type",
            "start_at",
            "end_at",
            "reason",
            "attachment",
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

        leave_type = cd.get("leave_type")
        if not leave_type:
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

        raw_duration = (cd.get("duration_type") or DURATION_FULL).strip().upper()
        from_time: dtime | None = cd.get("from_time")
        to_time: dtime | None = cd.get("to_time")

        try:
            computed_start, computed_end, is_half_day = _resolve_datetimes(
                leave_type=leave_type,
                start_date=start_date,
                end_date=end_date,
                duration_type=raw_duration,
                from_time=from_time,
                to_time=to_time,
            )
        except ValidationError as exc:
            if hasattr(exc, "error_dict"):
                for field, errors in exc.error_dict.items():
                    for err in errors:
                        self.add_error(field, err)
            else:
                self.add_error(None, exc)
            return cd

        cd["_computed_start_at"] = computed_start
        cd["_computed_end_at"] = computed_end
        cd["_computed_is_half_day"] = is_half_day

        return cd

    def _post_clean(self):
        """
        Inject computed IST-aware datetimes into the instance before model.clean() runs.
        """
        cd = getattr(self, "cleaned_data", {}) or {}

        if "_computed_start_at" in cd and "_computed_end_at" in cd:
            aware_start = cd["_computed_start_at"]
            aware_end = cd["_computed_end_at"]

            self.cleaned_data["start_at"] = aware_start
            self.cleaned_data["end_at"] = aware_end

            self.instance.start_at = aware_start
            self.instance.end_at = aware_end
            self.instance.is_half_day = bool(cd.get("_computed_is_half_day", False))

        if self.user and not getattr(self.instance, "employee_id", None):
            self.instance.employee = self.user

        super()._post_clean()

    def save(self, commit: bool = True) -> LeaveRequest:
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
    Admin edit form with exact-time support for Casual Leave and time-based leave.
    """

    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial=DURATION_FULL,
        widget=forms.RadioSelect,
        required=False,
    )

    start_at = forms.DateField(label="Start Date (IST)")
    end_at = forms.DateField(label="End Date (IST)", required=False)

    # ── FIX: same attrs={"type": "time"} fix applied here ─────────────────
    from_time = forms.TimeField(
        label="From Time (IST)",
        required=False,
        widget=forms.TimeInput(attrs={"type": "time"}, format="%H:%M"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (IST)",
        required=False,
        widget=forms.TimeInput(attrs={"type": "time"}, format="%H:%M"),
        input_formats=["%H:%M", "%I:%M %p"],
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

            if self.instance.is_half_day:
                self.fields["duration_type"].initial = DURATION_HALF
                self.fields["from_time"].initial = ist_start.time().replace(second=0, microsecond=0)
                self.fields["to_time"].initial = ist_end.time().replace(second=0, microsecond=0)
            else:
                self.fields["duration_type"].initial = DURATION_FULL

    def clean(self):
        cd = super().clean()

        leave_type = cd.get("leave_type")
        if not leave_type:
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

        raw_duration = (cd.get("duration_type") or DURATION_FULL).strip().upper()
        from_time: dtime | None = cd.get("from_time")
        to_time: dtime | None = cd.get("to_time")

        try:
            computed_start, computed_end, is_half_day = _resolve_datetimes(
                leave_type=leave_type,
                start_date=start_date,
                end_date=end_date,
                duration_type=raw_duration,
                from_time=from_time,
                to_time=to_time,
            )
        except ValidationError as exc:
            if hasattr(exc, "error_dict"):
                for field, errors in exc.error_dict.items():
                    for err in errors:
                        self.add_error(field, err)
            else:
                self.add_error(None, exc)
            return cd

        cd["_computed_start_at"] = computed_start
        cd["_computed_end_at"] = computed_end
        cd["_computed_is_half_day"] = is_half_day

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