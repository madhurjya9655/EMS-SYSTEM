#apps/leave/admin_views.py
from __future__ import annotations

import logging
from datetime import datetime

import pytz
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.db import transaction
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django import forms

from .models import LeaveRequest, LeaveStatus, LeaveType

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


class _AdminLeaveEditForm(forms.ModelForm):
    """
    Minimal admin-side edit form.
    Lets admins correct dates, half-day flag, type, reason, and status.
    """
    start_at = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        input_formats=["%Y-%m-%dT%H:%M"],
        required=True,
        label="Start (IST)",
    )
    end_at = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        input_formats=["%Y-%m-%dT%H:%M"],
        required=True,
        label="End (IST)",
    )
    status = forms.ChoiceField(choices=LeaveStatus.choices, required=True)
    leave_type = forms.ModelChoiceField(queryset=LeaveType.objects.order_by("name"), required=True)

    class Meta:
        model = LeaveRequest
        fields = ["leave_type", "start_at", "end_at", "is_half_day", "reason", "status"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pre-fill IST-local values into datetime-local inputs
        if self.instance and self.instance.pk:
            self.initial["start_at"] = timezone.localtime(self.instance.start_at, IST).strftime("%Y-%m-%dT%H:%M")
            self.initial["end_at"] = timezone.localtime(self.instance.end_at, IST).strftime("%Y-%m-%dT%H:%M")

    def clean_start_at(self):
        val = self.cleaned_data["start_at"]
        # Treat input as IST and convert to aware
        if timezone.is_naive(val):
            val = IST.localize(val)
        return val

    def clean_end_at(self):
        val = self.cleaned_data["end_at"]
        # Treat input as IST and convert to aware
        if timezone.is_naive(val):
            val = IST.localize(val)
        return val

    def save(self, commit=True):
        obj: LeaveRequest = super().save(commit=False)
        # Force IST timezone awareness
        if timezone.is_naive(obj.start_at):
            obj.start_at = IST.localize(obj.start_at)
        if timezone.is_naive(obj.end_at):
            obj.end_at = IST.localize(obj.end_at)
        if commit:
            obj.save()
        return obj


@login_required
def admin_edit_leave(request: HttpRequest, pk: int) -> HttpResponse:
    """Admin-only: edit a leave inline (dates, type, reason, flags, status)."""
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")

    leave = get_object_or_404(
        LeaveRequest.objects.select_related("employee", "leave_type", "reporting_person"),
        pk=pk,
    )

    next_url = request.GET.get("next") or reverse("leave:dashboard")

    if request.method == "POST":
        form = _AdminLeaveEditForm(request.POST, instance=leave)
        try:
            if form.is_valid():
                with transaction.atomic():
                    obj = form.save(commit=True)
                messages.success(request, "Leave updated.")
                return redirect(next_url)
            else:
                messages.error(request, "Please correct the errors below.")
        except ValidationError as e:
            for msg in e.messages:
                messages.error(request, msg)
        except Exception:
            logger.exception("Admin edit failed for leave %s", pk)
            messages.error(request, "Failed to update leave. Please try again.")
    else:
        form = _AdminLeaveEditForm(instance=leave)

    ctx = {
        "leave": leave,
        "form": form,
        "next_url": next_url,
    }
    return render(request, "leave/admin_edit_leave.html", ctx)


@login_required
@transaction.atomic
def admin_recalc_window(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Admin-only: force recompute of IST date snapshots and blocked_days after any manual fixes.
    """
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")

    leave = get_object_or_404(LeaveRequest, pk=pk)
    try:
        # Trigger model logic (_snapshot_dates + _recompute_blocked_days) via save()
        leave.save()
        messages.success(request, "Recalculated date snapshots and blocked days.")
    except Exception:
        logger.exception("Admin recalc failed for leave %s", pk)
        messages.error(request, "Recalculation failed.")

    next_url = request.GET.get("next") or reverse("leave:admin_edit_leave", args=[pk])
    return redirect(next_url)
