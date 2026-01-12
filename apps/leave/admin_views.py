from __future__ import annotations

from django import forms
from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from apps.users.permissions import has_permission
from .models import LeaveRequest, LeaveStatus
from .forms import LeaveRequestForm, IST


class AdminLeaveEditForm(LeaveRequestForm):
    """
    Extends LeaveRequestForm with a Status selector for admins.
    Reuses all LeaveRequestForm validation (tz-aware & overlap checks).
    """
    status = forms.ChoiceField(
        choices=LeaveStatus.choices,
        required=True,
        label="Status",
        widget=forms.Select(attrs={"class": "form-select"}),
        help_text="Changing status here immediately affects approvals.",
    )

    class Meta(LeaveRequestForm.Meta):
        fields = [
            "duration_type", "leave_type",
            "start_at", "end_at",
            "from_time", "to_time",
            "reason", "attachment",
            "delegate_to", "handover_checklist", "handover_delegation",
            "handover_help_ticket", "handover_message",
            "status",
        ]


@has_permission("leave_admin_edit")
def admin_edit_leave(request, pk: int):
    """
    Admin can modify any leave safely:
      - Dates (full/half-day), type, reason, attachment, handover fields
      - Status (Pending/Approved/Rejected)
    Keeps tz-aware datetimes and overlap checks from base form.
    """
    leave = get_object_or_404(LeaveRequest, pk=pk)

    before = {
        "start_at": leave.start_at,
        "end_at": leave.end_at,
        "leave_type_id": getattr(leave.leave_type, "id", None),
        "status": leave.status,
        "is_half_day": leave.is_half_day,
        "reason": leave.reason,
    }

    if request.method == "POST":
        form = AdminLeaveEditForm(request.POST, request.FILES, instance=leave, user=leave.employee)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.status = form.cleaned_data.get("status", obj.status)
            obj.save()
            form.save_m2m()

            # Optional: plug in your email/signal hooks here
            try:
                if before["status"] != obj.status:
                    # send_status_change_email(obj)  # implement if needed
                    pass
                if (before["start_at"], before["end_at"], before["is_half_day"]) != (
                    obj.start_at, obj.end_at, obj.is_half_day
                ):
                    # send_window_update_email(obj)  # implement if needed
                    pass
            except Exception:
                pass

            messages.success(request, "Leave updated successfully.")
            next_url = request.GET.get("next") or reverse("leave:dashboard")
            return redirect(next_url)
        else:
            messages.error(request, "Please fix the errors below.")
    else:
        initial = {"status": leave.status}
        form = AdminLeaveEditForm(instance=leave, user=leave.employee, initial=initial)

    ctx = {
        "form": form,
        "leave": leave,
        "employee_full_name": leave.employee.get_full_name() or leave.employee.get_username(),
        "employee_email": getattr(leave.employee, "email", ""),
        "now_ist": timezone.localtime(timezone.now(), IST),
        "next_url": request.GET.get("next") or request.META.get("HTTP_REFERER") or reverse("leave:dashboard"),
    }
    return render(request, "leave/admin_edit.html", ctx)


@has_permission("leave_admin_edit")
def admin_recalc_window(request, pk: int):
    """
    Helper to refresh handover lists after a quick date tweak via querystring.
    Example: /leave/admin/edit/123/recalculate/?start=2026-01-10&end=2026-01-12
    """
    leave = get_object_or_404(LeaveRequest, pk=pk)
    start = request.GET.get("start")
    end = request.GET.get("end") or start
    try:
        dummy_post = {
            "duration_type": "FULL",
            "leave_type": getattr(leave.leave_type, "id", None),
            "start_at": start,
            "end_at": end,
            "reason": leave.reason,
        }
        # Build a form so __init__ runs _load_handover_choices; we ignore its result.
        _ = AdminLeaveEditForm(dummy_post, instance=leave, user=leave.employee)
        messages.info(request, "Handover options refreshed for the selected date range.")
    except Exception:
        messages.error(request, "Could not refresh handover options for the provided dates.")
    return redirect(reverse("leave:admin_edit_leave", args=[leave.id]))
