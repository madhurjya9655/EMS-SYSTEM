# File: apps/leave/forms.py
# PURPOSE: Date-only leave (remove time-based leave restrictions)
# UPDATED: 2026-03-10
# CHANGE:  Fixed half-day leave bug — from_time/to_time/duration_type were
#          accepted by the form but silently discarded in clean().  When the
#          employee selects "Half Day" and enters times the form now:
#            • reads duration_type, from_time, to_time
#            • sets _computed_start_at / _computed_end_at to the actual times
#            • sets _computed_is_half_day = True
#          Full-day leaves continue to use FULL_DAY_START / FULL_DAY_END.

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

# Date-only normalization boundaries (IST) — used for full-day leaves only
FULL_DAY_START = dtime(0, 0, 0)
FULL_DAY_END   = dtime(23, 59, 59)

# Sensible defaults when the user picks "Half Day" but forgets to enter times
HALF_DAY_MORNING_START = dtime(9, 30, 0)
HALF_DAY_MORNING_END   = dtime(13, 30, 0)


def _combine_ist(date_value, t: dtime) -> datetime:
    naive = datetime.combine(date_value, t)
    return IST.localize(naive)


def _ist_local(dt: datetime) -> datetime:
    return timezone.localtime(dt, IST)


# ---------------------------------------------------------------------------
# Internal helper: resolve the (start_time, end_time, is_half_day) triple
# from form data.  Lives outside clean() so it can be unit-tested.
# ---------------------------------------------------------------------------
def _resolve_times(
    duration_type: str,
    from_time: dtime | None,
    to_time:   dtime | None,
) -> Tuple[dtime, dtime, bool]:
    """
    Returns (start_time, end_time, is_half_day).

    Full-day → (00:00:00, 23:59:59, False)
    Half-day with times → (from_time, to_time, True)
    Half-day without times → (09:30:00, 13:30:00, True)  [safe default]
    """
    if duration_type == DURATION_HALF:
        # Use the user-supplied times when both are present and logically ordered
        if from_time and to_time and to_time > from_time:
            return from_time, to_time, True
        # Partial entry: only one time supplied — still mark as half-day with defaults
        return HALF_DAY_MORNING_START, HALF_DAY_MORNING_END, True
    # Full day (or unrecognised duration_type)
    return FULL_DAY_START, FULL_DAY_END, False


class LeaveRequestForm(forms.ModelForm):
    """
    Employee-facing form used by /leave/apply/.

    RULES:
      - Duration type: FULL (all-day) or HALF (employee enters from/to times).
      - Validate only: end_date >= start_date; for half-day end_date == start_date.
      - If end_date missing → treat as same as start_date.
      - Convert to IST-aware datetimes.
    """

    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial=DURATION_FULL,
        widget=forms.RadioSelect,
        required=False,
    )

    # Date-only inputs
    start_at = forms.DateField(label="Start Date (IST)")
    end_at   = forms.DateField(label="End Date (IST)", required=False)

    # Half-day time range (now actively used)
    from_time = forms.TimeField(
        label="From Time (IST)",
        required=False,
        widget=forms.TimeInput(format="%H:%M"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (IST)",
        required=False,
        widget=forms.TimeInput(format="%H:%M"),
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

        if not cd.get("leave_type"):
            self.add_error("leave_type", "Please select a leave type.")

        start_date = cd.get("start_at")
        end_date   = cd.get("end_at")

        if not start_date:
            self.add_error("start_at", "Please select a start date.")
            return cd

        if not end_date:
            end_date = start_date

        # ── FIX: read duration_type and times instead of hardcoding them ────────
        raw_duration = (cd.get("duration_type") or DURATION_FULL).strip().upper()
        from_time: dtime | None = cd.get("from_time")
        to_time:   dtime | None = cd.get("to_time")

        start_time, end_time, is_half_day = _resolve_times(raw_duration, from_time, to_time)

        # Half-day leaves must be a single calendar day
        if is_half_day and end_date != start_date:
            self.add_error(
                "end_at",
                "Half-day leaves must start and end on the same day. "
                "Select 'Full Day' for multi-day leaves.",
            )
            return cd

        if end_date < start_date:
            self.add_error("end_at", "End date must be on or after start date.")
            return cd

        # Additional sanity check: to_time must be after from_time on same day
        if is_half_day and from_time and to_time and to_time <= from_time:
            self.add_error(
                "to_time",
                "End time must be after start time for a half-day leave.",
            )
            return cd

        cd["_computed_start_at"]    = _combine_ist(start_date, start_time)
        cd["_computed_end_at"]      = _combine_ist(end_date,   end_time)
        cd["_computed_is_half_day"] = is_half_day
        # ────────────────────────────────────────────────────────────────────────

        return cd

    def _post_clean(self):
        """
        Inject computed IST-aware datetimes into the instance before model.clean() runs.
        """
        cd = getattr(self, "cleaned_data", {}) or {}

        if "_computed_start_at" in cd and "_computed_end_at" in cd:
            aware_start = cd["_computed_start_at"]
            aware_end   = cd["_computed_end_at"]

            self.cleaned_data["start_at"] = aware_start
            self.cleaned_data["end_at"]   = aware_end

            self.instance.start_at    = aware_start
            self.instance.end_at      = aware_end
            self.instance.is_half_day = bool(cd.get("_computed_is_half_day", False))

        if self.user and not getattr(self.instance, "employee_id", None):
            self.instance.employee = self.user

        super()._post_clean()

    def save(self, commit: bool = True) -> LeaveRequest:
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd = self.cleaned_data

        instance = LeaveRequest(
            employee      = self.user,
            leave_type    = cd.get("leave_type"),
            start_at      = cd["_computed_start_at"],
            end_at        = cd["_computed_end_at"],
            is_half_day   = cd.get("_computed_is_half_day", False),
            reason        = cd.get("reason") or "",
            attachment    = cd.get("attachment"),
        )

        if commit:
            instance.save()
        return instance


class AdminLeaveEditForm(forms.ModelForm):
    """
    Admin edit form (date-only, with half-day support).
    """

    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES, initial=DURATION_FULL, widget=forms.RadioSelect, required=False
    )

    start_at = forms.DateField(label="Start Date (IST)")
    end_at   = forms.DateField(label="End Date (IST)", required=False)

    # Half-day time range (now actively used)
    from_time = forms.TimeField(
        label="From Time (IST)",
        required=False,
        widget=forms.TimeInput(format="%H:%M"),
        input_formats=["%H:%M", "%I:%M %p"],
    )
    to_time = forms.TimeField(
        label="To Time (IST)",
        required=False,
        widget=forms.TimeInput(format="%H:%M"),
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
            ist_end   = _ist_local(self.instance.end_at)

            self.fields["start_at"].initial = ist_start.date()
            self.fields["end_at"].initial   = ist_end.date()

            # ── FIX: pre-populate duration_type, from_time, to_time from the
            #         stored values so editing an existing half-day leave shows
            #         the correct data.
            if self.instance.is_half_day:
                self.fields["duration_type"].initial = DURATION_HALF
                self.fields["from_time"].initial     = ist_start.time()
                self.fields["to_time"].initial       = ist_end.time()
            else:
                self.fields["duration_type"].initial = DURATION_FULL

    def clean(self):
        cd = super().clean()

        if not cd.get("leave_type"):
            self.add_error("leave_type", "Please select a leave type.")

        start_date = cd.get("start_at")
        end_date   = cd.get("end_at")

        if not start_date:
            self.add_error("start_at", "Please select a start date.")
            return cd

        if not end_date:
            end_date = start_date

        # ── FIX: read duration_type and times instead of hardcoding them ────────
        raw_duration = (cd.get("duration_type") or DURATION_FULL).strip().upper()
        from_time: dtime | None = cd.get("from_time")
        to_time:   dtime | None = cd.get("to_time")

        start_time, end_time, is_half_day = _resolve_times(raw_duration, from_time, to_time)

        if is_half_day and end_date != start_date:
            self.add_error(
                "end_at",
                "Half-day leaves must start and end on the same day.",
            )
            return cd

        if end_date < start_date:
            self.add_error("end_at", "End date must be on or after start date.")
            return cd

        if is_half_day and from_time and to_time and to_time <= from_time:
            self.add_error("to_time", "End time must be after start time.")
            return cd

        cd["_computed_start_at"]    = _combine_ist(start_date, start_time)
        cd["_computed_end_at"]      = _combine_ist(end_date,   end_time)
        cd["_computed_is_half_day"] = is_half_day
        # ────────────────────────────────────────────────────────────────────────

        return cd

    def _post_clean(self):
        cd = getattr(self, "cleaned_data", {}) or {}
        if "_computed_start_at" in cd and "_computed_end_at" in cd:
            aware_start = cd["_computed_start_at"]
            aware_end   = cd["_computed_end_at"]
            self.cleaned_data["start_at"] = aware_start
            self.cleaned_data["end_at"]   = aware_end
            self.instance.start_at    = aware_start
            self.instance.end_at      = aware_end
            self.instance.is_half_day = bool(cd.get("_computed_is_half_day", False))
        super()._post_clean()

    def save(self, commit: bool = True) -> LeaveRequest:
        if not self.is_valid():
            raise ValidationError("Invalid form; cannot save.")

        cd   = self.cleaned_data
        inst: LeaveRequest = super().save(commit=False)
        inst.leave_type  = cd.get("leave_type")
        inst.start_at    = cd["_computed_start_at"]
        inst.end_at      = cd["_computed_end_at"]
        inst.is_half_day = cd.get("_computed_is_half_day", False)
        if commit:
            inst.save()
        return inst