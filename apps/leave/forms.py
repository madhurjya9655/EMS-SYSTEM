# apps/leave/forms.py
from __future__ import annotations

from datetime import timedelta, time, datetime, date
from typing import Optional, List, Tuple

from django import forms
from django.contrib.auth import get_user_model
from django.utils import timezone
from zoneinfo import ZoneInfo

from .models import LeaveRequest, LeaveStatus, LeaveType

IST = ZoneInfo("Asia/Kolkata")
User = get_user_model()

ALLOWED_ATTACHMENT_EXTS = {
    ".pdf", ".png", ".jpg", ".jpeg", ".webp", ".heic", ".doc", ".docx", ".txt"
}

# These are the leave types shown in the dropdown.
SOP_ALLOWED_TYPE_NAMES = [
    "Compensatory Off",
    "Leave Without Pay",
    "Leave Without Pay (If No Leave Balance)",
    "Sick Leave",
    "Casual Leave",
    "Maternity Leave",
    "Paternity Leave",
    "Personal Leave",
]

# Working-day anchors (IST)
WORK_FROM  = time(9, 30)
WORK_TO    = time(18, 0)
FULL_DAY_DEFAULT_FROM = WORK_FROM
FULL_DAY_DEFAULT_TO   = WORK_TO


def _aware_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, IST)
    return timezone.localtime(dt, IST)


def _choices(items: List[Tuple[int, str]]) -> List[Tuple[str, str]]:
    return [(str(i), s) for i, s in items]


def _now_ist() -> datetime:
    return timezone.localtime(timezone.now(), IST)


class LeaveRequestForm(forms.ModelForm):
    """
    New UI contract:
      • duration_type = FULL / HALF (radio)
      • For HALF:   one Date + From/To time (any range inside 09:30–18:00)
      • For FULL:   Start Date required; End Date optional (defaults to Start)
    """

    # Duration
    DURATION_CHOICES = (("FULL", "Full Day"), ("HALF", "Half Day"))
    duration_type = forms.ChoiceField(
        choices=DURATION_CHOICES,
        initial="FULL",
        widget=forms.RadioSelect(attrs={"id": "id_duration_type"}),
        label="Duration",
        required=True,
    )

    # Leave type (no "Half Day" here)
    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.none(),
        required=True,
        empty_label="-- Select Type --",
        label="Leave Type",
        widget=forms.Select(attrs={"class": "form-select", "id": "id_leave_type"}),
    )

    # DATE INPUTS
    # Start date is always present. End date is optional (used for multi-day Full Day).
    start_at = forms.DateField(
        required=True,
        label="Start Date (IST) / Date (Half Day)",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control", "id": "id_start_at"}),
        help_text="For Full Day we’ll use 09:30 → 18:00. For Half Day, pick your time range below on the same date.",
    )
    end_at = forms.DateField(
        required=False,   # <-- optional now
        label="End Date (IST – Full Day only)",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control", "id": "id_end_at"}),
        help_text="Optional for Full Day. If empty, it becomes a single day.",
    )

    # Half-day specific time slot
    from_time = forms.TimeField(
        required=False,
        label="From Time (Half Day)",
        initial=WORK_FROM,
        widget=forms.TimeInput(attrs={"type": "time", "class": "form-control", "id": "id_from_time"}),
        help_text="Only when Duration = Half Day. Must be within 09:30–18:00.",
    )
    to_time = forms.TimeField(
        required=False,
        label="To Time (Half Day)",
        initial=WORK_TO,
        widget=forms.TimeInput(attrs={"type": "time", "class": "form-control", "id": "id_to_time"}),
        help_text="Only when Duration = Half Day. Must be within 09:30–18:00.",
    )

    reason = forms.CharField(
        required=True,
        label="Reason",
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "e.g., Medical checkup / personal work", "class": "form-control"}),
    )

    attachment = forms.FileField(
        required=False,
        label="Attachment (optional)",
        widget=forms.FileInput(attrs={"class": "form-control"}),
        help_text="Allowed: PDF, images, DOC/DOCX/TXT (max 10 MB).",
    )

    # Task handover fields
    delegate_to = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        label="Delegate To (handover)",
        help_text="Select a colleague to temporarily take over your selected tasks while you are on leave.",
        widget=forms.Select(attrs={"class": "form-select"}),
        empty_label="---------",
    )
    handover_checklist = forms.MultipleChoiceField(
        required=False,
        label="Checklist tasks to hand over",
        choices=[],
        widget=forms.CheckboxSelectMultiple(attrs={"class": "form-check-input"}),
        help_text="Only tasks due or in progress in the leave period are listed.",
    )
    handover_delegation = forms.MultipleChoiceField(
        required=False,
        label="Delegation tasks to hand over",
        choices=[],
        widget=forms.CheckboxSelectMultiple(attrs={"class": "form-check-input"}),
    )
    handover_help_ticket = forms.MultipleChoiceField(
        required=False,
        label="Help tickets to hand over",
        choices=[],
        widget=forms.CheckboxSelectMultiple(attrs={"class": "form-check-input"}),
    )
    handover_message = forms.CharField(
        required=False,
        label="Handover message (optional)",
        widget=forms.Textarea(attrs={"rows": 2, "placeholder": "Any instructions for the delegate", "class": "form-control"}),
    )

    class Meta:
        model = LeaveRequest
        fields = [
            "duration_type",
            "leave_type",
            "start_at", "end_at",
            "from_time", "to_time",
            "reason", "attachment",
            "delegate_to", "handover_checklist", "handover_delegation",
            "handover_help_ticket", "handover_message",
        ]

    # ------------------------------------------------------------------ init
    def __init__(self, *args, user: Optional[User] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user

        # Only SOP-allowed leave types (includes Personal Leave)
        self.fields["leave_type"].queryset = LeaveType.objects.filter(
            name__in=SOP_ALLOWED_TYPE_NAMES
        ).order_by("name")

        # Select employee
        if self.instance and getattr(self.instance, "employee_id", None):
            self.employee = self.instance.employee
        else:
            self.employee = user
        try:
            if self.instance and not getattr(self.instance, "employee_id", None) and self.employee:
                self.instance.employee = self.employee
        except Exception:
            pass

        # delegate choices
        delegate_qs = User.objects.filter(is_active=True) \
            .exclude(email__isnull=True).exclude(email__exact="") \
            .order_by("first_name", "last_name", "username")
        if self.employee:
            delegate_qs = delegate_qs.exclude(pk=getattr(self.employee, "pk", None))
        self.fields["delegate_to"].queryset = delegate_qs

        # handover choices (based on dates if present)
        start_d: Optional[date] = None
        end_d: Optional[date] = None
        if self.data:
            try:
                sd = self.data.get("start_at")
                ed = self.data.get("end_at")
                if sd:
                    start_d = datetime.fromisoformat(sd).date()
                if ed:
                    end_d = datetime.fromisoformat(ed).date()
            except Exception:
                pass
        if not start_d:
            start_d = timezone.localtime(timezone.now(), IST).date()
        if not end_d:
            end_d = start_d
        self._load_handover_choices(
            timezone.make_aware(datetime.combine(start_d, time.min), IST),
            timezone.make_aware(datetime.combine(end_d,   time.max), IST),
        )

    # ------------------------------------------------------------- choices load
    def _load_handover_choices(self, start_at, end_at):
        if not self.employee:
            self.fields["handover_checklist"].choices = []
            self.fields["handover_delegation"].choices = []
            self.fields["handover_help_ticket"].choices = []
            return

        def _fmt(obj, title_attr: str = "task_name"):
            title = getattr(obj, title_attr, None) or getattr(obj, "title", None) or str(obj)
            planned = getattr(obj, 'planned_date', None)
            date_str = planned.strftime('%m/%d') if planned else 'No date'
            return f"#{getattr(obj, 'id', '—')} • {title} ({date_str})"

        cl_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import Checklist
            q = Checklist.objects.filter(assign_to=self.employee, status='Pending')
            q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            cl_items = [(t.id, _fmt(t, "task_name")) for t in q.order_by("-planned_date", "-id")[:50]]
        except Exception:
            cl_items = []

        dg_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import Delegation
            q = Delegation.objects.filter(assign_to=self.employee, status='Pending')
            q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            dg_items = [(t.id, _fmt(t, "task_name")) for t in q.order_by("-planned_date", "-id")[:50]]
        except Exception:
            dg_items = []

        ht_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import HelpTicket
            q = HelpTicket.objects.filter(assign_to=self.employee).exclude(
                status__in=["Done", "CLOSED", "COMPLETED", "Completed", "Closed"]
            )
            q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            ht_items = [(t.id, _fmt(t, "title")) for t in q.order_by("-planned_date", "-id")[:50]]
        except Exception:
            ht_items = []

        self.fields["handover_checklist"].choices = _choices(cl_items)
        self.fields["handover_delegation"].choices = _choices(dg_items)
        self.fields["handover_help_ticket"].choices = _choices(ht_items)

    # --------------------------------------------------------------- validators
    def clean_attachment(self):
        f = self.cleaned_data.get("attachment")
        if not f:
            return f
        name = (getattr(f, "name", "") or "").lower()
        ext = name[name.rfind("."):] if "." in name else ""
        if ext not in ALLOWED_ATTACHMENT_EXTS:
            raise forms.ValidationError("Unsupported file type. Upload PDF, image, DOC/DOCX, or TXT.")
        if getattr(f, "size", 0) and f.size > 10 * 1024 * 1024:
            raise forms.ValidationError("File too large. Max 10 MB.")
        return f

    def _overlaps_existing(self, start_at, end_at) -> bool:
        if not hasattr(self, "employee") or not self.employee:
            return False
        qs = (
            LeaveRequest.objects.filter(employee=self.employee)
            .exclude(pk=self.instance.pk or 0)
            .filter(status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])
        )
        return qs.filter(start_at__lt=end_at, end_at__gt=start_at).exists()

    def clean(self):
        cleaned = super().clean()

        dur = (cleaned.get("duration_type") or "FULL").upper()
        leave_type = cleaned.get("leave_type")
        start_d: Optional[date] = cleaned.get("start_at")
        end_d:   Optional[date] = cleaned.get("end_at")

        if not leave_type or not start_d:
            return cleaned  # field-level errors will be shown

        now_ist = _now_ist()

        # HALF DAY: one date + free range inside 09:30–18:00
        if dur == "HALF":
            f = cleaned.get("from_time") or WORK_FROM
            t = cleaned.get("to_time")   or WORK_TO

            # Must be same calendar date (UI only sends one date anyway)
            end_d = start_d

            # Validate range within work hours and order
            if not (WORK_FROM <= f < WORK_TO) or not (WORK_FROM < t <= WORK_TO):
                self.add_error("from_time", "Half-day time must be within 09:30–18:00.")
                self.add_error("to_time", "Half-day time must be within 09:30–18:00.")
            if t <= f:
                self.add_error("to_time", "Half-day 'To Time' must be after 'From Time'.")

            start_dt = _aware_ist(datetime.combine(start_d, f))
            end_dt   = _aware_ist(datetime.combine(end_d,   t))

            cleaned["is_half_day"] = True
            cleaned["start_at"] = start_dt
            cleaned["end_at"]   = end_dt
            return cleaned

        # FULL DAY: start date required; end date optional
        if end_d and end_d < start_d:
            self.add_error("end_at", "End date must be on or after Start date.")
            return cleaned

        if not end_d:
            end_d = start_d

        start_dt = _aware_ist(datetime.combine(start_d, FULL_DAY_DEFAULT_FROM))
        end_dt   = _aware_ist(datetime.combine(end_d,   FULL_DAY_DEFAULT_TO))

        if end_dt <= start_dt:
            self.add_error("end_at", "End must be after Start.")

        cleaned["is_half_day"] = False
        cleaned["start_at"] = start_dt
        cleaned["end_at"]   = end_dt

        # Overlap check
        try:
            if self._overlaps_existing(cleaned["start_at"], cleaned["end_at"]):
                self.add_error(None, "You already have a pending/approved leave that overlaps this period.")
        except forms.ValidationError as e:
            self.add_error(None, e)

        return cleaned

    # -------------------------------------------------------------------- save
    def save(self, commit: bool = True) -> LeaveRequest:
        obj: LeaveRequest = super().save(commit=False)

        if hasattr(self, "employee") and self.employee and not getattr(obj, "employee_id", None):
            obj.employee = self.employee

        obj.is_half_day = bool(self.cleaned_data.get("is_half_day"))

        if commit:
            obj.save()
            self.save_m2m()
        return obj
