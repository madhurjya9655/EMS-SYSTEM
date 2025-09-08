# apps/leave/forms.py
from __future__ import annotations

from datetime import timedelta
from typing import Optional

from django import forms
from django.contrib.auth import get_user_model
from django.utils import timezone
from zoneinfo import ZoneInfo

from .models import LeaveRequest, LeaveStatus, LeaveType

IST = ZoneInfo("Asia/Kolkata")
User = get_user_model()

ALLOWED_ATTACHMENT_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".heic", ".doc", ".docx"}


def _naive_to_ist(dt):
    if not dt:
        return dt
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, IST)
    return timezone.localtime(dt, IST)


class LeaveRequestForm(forms.ModelForm):
    """
    Thin-but-safe ModelForm:
      - surfaces key fields,
      - prevents overlaps (pending/approved),
      - caps half-day duration to ≤ 6h (business rule),
      - friendly attachment checks,
      - defers strict same-day time gates to model.clean() (single source of truth).
    """

    leave_type = forms.ModelChoiceField(
        queryset=LeaveType.objects.all().order_by("name"),
        required=True,
        empty_label="-- Select Type --",
        label="Leave Type",
    )

    start_at = forms.DateTimeField(
        required=True,
        label="From (IST)",
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        help_text="Start date & time (IST).",
    )

    end_at = forms.DateTimeField(
        required=True,
        label="To (IST)",
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        help_text="End date & time (IST).",
    )

    is_half_day = forms.BooleanField(
        required=False,
        label="Half-day",
        help_text="For a half-day, keep start & end on the same date and within 6 hours.",
    )

    reason = forms.CharField(
        required=True,
        label="Reason",
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "e.g., Medical Checkup"}),
    )

    attachment = forms.FileField(
        required=False,
        label="Attachment (optional)",
        help_text="Allowed: PDF, images, DOC/DOCX.",
    )

    def __init__(self, *args, user: Optional[User] = None, **kwargs):
        """
        Pass the logged-in user via `LeaveRequestForm(user=request.user)`.
        We eagerly attach the employee to the instance so any model logic
        (upload_to paths, snapshots, validations) can safely access it.
        """
        super().__init__(*args, **kwargs)
        self.user = user

        # Prefer instance.employee if editing; otherwise, use provided user.
        if self.instance and getattr(self.instance, "employee_id", None):
            self.employee = self.instance.employee
        else:
            self.employee = user

        # **Critical**: ensure the instance has employee set before any save/clean hooks.
        try:
            if self.instance and not getattr(self.instance, "employee_id", None) and self.employee:
                self.instance.employee = self.employee
        except Exception:
            # Best-effort: never crash the form init
            pass

        # Help text clarity
        self.fields["start_at"].help_text = "Start date & time (IST)."
        self.fields["end_at"].help_text = "End date & time (IST)."

        # Basic Bootstrap-ish styling (optional; remove if you style via templates)
        for name, field in self.fields.items():
            if not isinstance(field.widget, (forms.CheckboxInput, forms.FileInput)):
                field.widget.attrs.setdefault("class", "form-control")
        self.fields["is_half_day"].widget.attrs.setdefault("class", "form-check-input")

    class Meta:
        model = LeaveRequest
        fields = [
            "leave_type",
            "start_at",
            "end_at",
            "is_half_day",
            "reason",
            "attachment",
        ]

    # ---------------------------
    # Field-level sanitation
    # ---------------------------
    def clean_attachment(self):
        f = self.cleaned_data.get("attachment")
        if not f:
            return f
        name = (getattr(f, "name", "") or "").lower()
        ext = ""
        if "." in name:
            ext = name[name.rfind(".") :]
        if ext not in ALLOWED_ATTACHMENT_EXTS:
            raise forms.ValidationError("Unsupported file type. Upload PDF, image, or DOC/DOCX.")
        if getattr(f, "size", 0) and f.size > 10 * 1024 * 1024:  # 10 MB
            raise forms.ValidationError("File too large. Max 10 MB.")
        return f

    def clean_start_at(self):
        dt = self.cleaned_data.get("start_at")
        return _naive_to_ist(dt)

    def clean_end_at(self):
        dt = self.cleaned_data.get("end_at")
        return _naive_to_ist(dt)

    # ---------------------------
    # Form-level validation
    # ---------------------------
    def _overlaps_existing(self, start_at, end_at) -> bool:
        """
        Detect overlap with user's PENDING/APPROVED leaves.
        [s1, e1] overlaps [s2, e2] if s1 < e2 and s2 < e1
        """
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

        # Safety: Ensure tz-aware and normalized to IST for business rules
        start_at = _naive_to_ist(start_at)
        end_at = _naive_to_ist(end_at)
        cleaned["start_at"] = start_at
        cleaned["end_at"] = end_at

        # Basic order check (model.clean also enforces)
        if end_at <= start_at:
            self.add_error("end_at", "End must be after Start.")

        # Half-day constraints (same date, ≤ 6 hours)
        if is_half_day:
            if start_at.date() != end_at.date():
                self.add_error("is_half_day", "Half-day must start and end on the same date.")
            if (end_at - start_at) > timedelta(hours=6):
                self.add_error("is_half_day", "Half-day duration should be ≤ 6 hours.")

        # Prevent overlaps with user's other PENDING/APPROVED leaves
        try:
            if self._overlaps_existing(start_at, end_at):
                raise forms.ValidationError(
                    "You already have a pending/approved leave that overlaps this period."
                )
        except forms.ValidationError as e:
            self.add_error(None, e)

        return cleaned

    # ---------------------------
    # Save
    # ---------------------------
    def save(self, commit: bool = True) -> LeaveRequest:
        """
        - Attach employee to the instance (double-sure).
        - Let the model handle snapshots, blocked_days, strict time cutoffs,
          and routing defaults.
        """
        obj: LeaveRequest = super().save(commit=False)

        # **Double-safety** before model.save() triggers upload_to/snapshots.
        if self.employee and not getattr(obj, "employee_id", None):
            obj.employee = self.employee

        if commit:
            obj.save()
        return obj
