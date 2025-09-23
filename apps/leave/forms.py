# apps/leave/forms.py
from __future__ import annotations

from datetime import timedelta, time
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


def _naive_to_ist(dt):
    if not dt:
        return dt
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, IST)
    return timezone.localtime(dt, IST)


def _choices(items: List[Tuple[int, str]]) -> List[Tuple[str, str]]:
    return [(str(i), s) for i, s in items]


class LeaveRequestForm(forms.ModelForm):
    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.none(),
        required=True,
        empty_label="-- Select Type --",
        label="Leave Type",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    start_at = forms.DateTimeField(
        required=True,
        label="From (IST)",
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        help_text="Start date & time (IST).",
    )

    end_at = forms.DateTimeField(
        required=True,
        label="To (IST)", 
        widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        help_text="End date & time (IST).",
    )

    is_half_day = forms.BooleanField(
        required=False,
        label="Half-day",
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        help_text="For a half-day, keep start & end on the same date and within 6 hours.",
    )

    reason = forms.CharField(
        required=True,
        label="Reason",
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "e.g., Medical Checkup", "class": "form-control"}),
    )

    attachment = forms.FileField(
        required=False,
        label="Attachment (optional)",
        widget=forms.FileInput(attrs={"class": "form-control"}),
        help_text="Allowed: PDF, images, DOC/DOCX/TXT (max 10 MB).",
    )

    # Multi-CC field
    cc_users = forms.ModelMultipleChoiceField(
        queryset=User.objects.none(),
        required=False,
        label="Additional CC Recipients",
        help_text="Additional recipients to CC on your leave request.",
        widget=forms.CheckboxSelectMultiple(attrs={"class": "form-check-input"}),
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

    def __init__(self, *args, user: Optional[User] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user

        # Always refresh the list so newly seeded/added types show up
        self.fields["leave_type"].queryset = LeaveType.objects.all().order_by("name")

        # Prefer instance.employee if editing; otherwise, use provided user
        if self.instance and getattr(self.instance, "employee_id", None):
            self.employee = self.instance.employee
        else:
            self.employee = user

        # Ensure instance has employee before model.save() triggers upload_to/snapshots
        try:
            if self.instance and not getattr(self.instance, "employee_id", None) and self.employee:
                self.instance.employee = self.employee
        except Exception:
            pass

        # CC choices (active users with email), exclude self
        cc_qs = User.objects.filter(is_active=True) \
            .exclude(email__isnull=True).exclude(email__exact="") \
            .order_by("first_name", "last_name", "username")
        if self.employee:
            cc_qs = cc_qs.exclude(pk=getattr(self.employee, "pk", None))
        self.fields["cc_users"].queryset = cc_qs

        # Delegate choices (active users with email), exclude self
        self.fields["delegate_to"].queryset = cc_qs

        # Load handover choices - get dates from form data or default to a week from now
        start_dt = end_dt = None
        
        if self.data:
            # Form submitted with data - extract dates
            start_raw = self.data.get("start_at")
            end_raw = self.data.get("end_at")
            try:
                if start_raw:
                    start_dt = _naive_to_ist(timezone.datetime.fromisoformat(start_raw.replace('T', ' ')))
                if end_raw:
                    end_dt = _naive_to_ist(timezone.datetime.fromisoformat(end_raw.replace('T', ' ')))
            except Exception:
                pass
        
        if not start_dt or not end_dt:
            # Default to show tasks for next week to demonstrate functionality
            now = timezone.now()
            start_dt = now
            end_dt = now + timedelta(days=7)

        self._load_handover_choices(start_dt, end_dt)

    def _load_handover_choices(self, start_at, end_at):
        """Query user's tasks in the window and present as choices."""
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

        # Checklist tasks
        cl_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import Checklist
            # Get all pending tasks for this user
            q = Checklist.objects.filter(assign_to=self.employee, status='Pending')
            
            # If we have a specific date range, filter by it, otherwise show all pending
            if start_at and end_at and (end_at - start_at).days < 30:  # Only filter if reasonable date range
                q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            
            cl_items = [(t.id, _fmt(t, "task_name")) for t in q.order_by("-planned_date")[:20]]
        except Exception as e:
            print(f"Error loading checklist tasks: {e}")
            cl_items = []

        # Delegation tasks  
        dg_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import Delegation
            q = Delegation.objects.filter(assign_to=self.employee, status='Pending')
            
            if start_at and end_at and (end_at - start_at).days < 30:
                q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            
            dg_items = [(t.id, _fmt(t, "task_name")) for t in q.order_by("-planned_date")[:20]]
        except Exception as e:
            print(f"Error loading delegation tasks: {e}")
            dg_items = []

        # Help tickets
        ht_items: List[Tuple[int, str]] = []
        try:
            from apps.tasks.models import HelpTicket
            q = HelpTicket.objects.filter(assign_to=self.employee)
            q = q.exclude(status__in=["Done", "CLOSED", "COMPLETED", "Completed", "Closed"])
            
            if start_at and end_at and (end_at - start_at).days < 30:
                q = q.filter(planned_date__range=(start_at - timedelta(days=1), end_at + timedelta(days=1)))
            
            ht_items = [(t.id, _fmt(t, "title")) for t in q.order_by("-planned_date")[:20]]
        except Exception as e:
            print(f"Error loading help tickets: {e}")
            ht_items = []

        # Set choices
        self.fields["handover_checklist"].choices = _choices(cl_items)
        self.fields["handover_delegation"].choices = _choices(dg_items)
        self.fields["handover_help_ticket"].choices = _choices(ht_items)

        # Debug info
        print(f"Loaded tasks for {self.employee}: CL:{len(cl_items)}, DG:{len(dg_items)}, HT:{len(ht_items)}")

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type", "start_at", "end_at", "is_half_day", "reason", "attachment",
            "cc_users", "delegate_to", "handover_checklist", "handover_delegation", 
            "handover_help_ticket", "handover_message",
        ]

    def clean_attachment(self):
        f = self.cleaned_data.get("attachment")
        if not f:
            return f
        name = (getattr(f, "name", "") or "").lower()
        ext = ""
        if "." in name:
            ext = name[name.rfind(".") :]
        if ext not in ALLOWED_ATTACHMENT_EXTS:
            raise forms.ValidationError("Unsupported file type. Upload PDF, image, DOC/DOCX, or TXT.")
        if getattr(f, "size", 0) and f.size > 10 * 1024 * 1024:  # 10 MB
            raise forms.ValidationError("File too large. Max 10 MB.")
        return f

    def clean_start_at(self):
        dt = self.cleaned_data.get("start_at")
        return _naive_to_ist(dt)

    def clean_end_at(self):
        dt = self.cleaned_data.get("end_at")
        return _naive_to_ist(dt)

    def _overlaps_existing(self, start_at, end_at) -> bool:
        """Detect overlap with user's PENDING/APPROVED leaves."""
        if not self.employee:
            return False

        qs = (
            LeaveRequest.objects.filter(employee=self.employee)
            .exclude(pk=self.instance.pk or 0)
            .filter(status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])
        )
        return qs.filter(start_at__lt=end_at, end_at__gt=start_at).exists()

    def clean(self):
        cleaned = super().clean()

        start_at: Optional[timezone.datetime] = cleaned.get("start_at")
        end_at: Optional[timezone.datetime] = cleaned.get("end_at")
        is_half_day: bool = bool(cleaned.get("is_half_day"))

        if not start_at or not end_at:
            return cleaned

        # Ensure tz-aware & IST-normalized
        start_at = _naive_to_ist(start_at)
        end_at = _naive_to_ist(end_at)
        cleaned["start_at"] = start_at
        cleaned["end_at"] = end_at

        # Basic order check
        if end_at <= start_at:
            self.add_error("end_at", "End must be after Start.")

        # Half-day constraints
        if is_half_day:
            if start_at.date() != end_at.date():
                self.add_error("is_half_day", "Half-day must start and end on the same date.")
            if (end_at - start_at) > timedelta(hours=6):
                self.add_error("is_half_day", "Half-day duration should be ≤ 6 hours.")

        # Prevent overlaps
        try:
            if self._overlaps_existing(start_at, end_at):
                self.add_error(None, "You already have a pending/approved leave that overlaps this period.")
        except forms.ValidationError as e:
            self.add_error(None, e)

        return cleaned

    def save(self, commit: bool = True) -> LeaveRequest:
        obj: LeaveRequest = super().save(commit=False)

        if self.employee and not getattr(obj, "employee_id", None):
            obj.employee = self.employee

        if commit:
            obj.save()
            # Important: Save the many-to-many relationships after the main object
            self.save_m2m()

        return obj