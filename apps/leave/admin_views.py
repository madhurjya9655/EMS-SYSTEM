#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\leave\admin_views.py
from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

from .forms import AdminLeaveEditForm
from .models import LeaveRequest, LeaveStatus
from .utils import (
    get_admin_leave_balance_rows,
    get_leave_year_bounds,
    sync_employee_leave_balance_for_range,
)

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
User = get_user_model()


def _is_admin_or_manager(user) -> bool:
    return getattr(user, "is_superuser", False) or getattr(user, "is_staff", False)


def _safe_next(request: HttpRequest, fallback: str = "/") -> str:
    nxt = (request.POST.get("next") or request.GET.get("next") or "").strip()
    return nxt if nxt.startswith("/") else fallback


@login_required
def admin_edit_leave(request: HttpRequest, pk: int) -> HttpResponse:
    """Admin / manager edits any leave request; recalculates balance for old and new date ranges."""
    if not _is_admin_or_manager(request.user):
        return HttpResponseForbidden("Admin access required.")

    leave = get_object_or_404(
        LeaveRequest.objects.select_related("employee", "leave_type"), pk=pk
    )

    # Capture the old date range BEFORE the form mutates the instance
    old_start = leave.start_date
    old_end = leave.end_date

    if request.method == "POST":
        form = AdminLeaveEditForm(request.POST, request.FILES, instance=leave)
        if form.is_valid():
            try:
                with transaction.atomic():
                    updated_leave: LeaveRequest = form.save(commit=True)

                    # Recalculate balance for the old period (dates may have moved)
                    if old_start and old_end:
                        sync_employee_leave_balance_for_range(
                            employee=updated_leave.employee,
                            range_start=old_start,
                            range_end=old_end,
                        )

                    # Recalculate balance for the new period
                    if updated_leave.start_date and updated_leave.end_date:
                        sync_employee_leave_balance_for_range(
                            employee=updated_leave.employee,
                            range_start=updated_leave.start_date,
                            range_end=updated_leave.end_date,
                        )

                messages.success(request, "Leave updated and balance recalculated.")
                return redirect(_safe_next(request, "/"))
            except Exception:
                logger.exception("admin_edit_leave failed for pk=%s", pk)
                messages.error(request, "Could not save the leave. Please try again.")
        else:
            messages.error(request, "Please fix the errors below.")
    else:
        form = AdminLeaveEditForm(instance=leave)

    return render(
        request,
        "leave/admin_edit_leave.html",
        {
            "form": form,
            "leave": leave,
            "next_url": _safe_next(request, "/"),
        },
    )


@login_required
@require_POST
def admin_recalc_window(request: HttpRequest, pk: int) -> HttpResponse:
    """Force-recalculate the leave balance for a specific leave's date window."""
    if not _is_admin_or_manager(request.user):
        return HttpResponseForbidden("Admin access required.")

    leave = get_object_or_404(LeaveRequest, pk=pk)

    try:
        sync_employee_leave_balance_for_range(
            employee=leave.employee,
            range_start=leave.start_date,
            range_end=leave.end_date,
        )
        emp_label = leave.employee.get_full_name() or leave.employee.username
        messages.success(request, f"Balance recalculated for {emp_label}.")
    except Exception:
        logger.exception("admin_recalc_window failed for pk=%s", pk)
        messages.error(request, "Recalculation failed. Please try again.")

    return redirect(_safe_next(request, "/"))


@login_required
def admin_leave_balance(request: HttpRequest) -> HttpResponse:
    """
    Standalone page: leave balance for ALL active employees.
    Accessible by superusers and staff (managers).
    Supports leave year selector via ?year=YYYY query param (YYYY = April year start).
    """
    if not _is_admin_or_manager(request.user):
        return HttpResponseForbidden("Admin or manager access required.")

    today = date.today()
    # Current leave year starts in April; if before April, subtract 1
    current_leave_year_start = today.year if today.month >= 4 else today.year - 1

    try:
        selected_year = int(request.GET.get("year", current_leave_year_start))
    except (TypeError, ValueError):
        selected_year = current_leave_year_start

    # target_date = April 1 of the selected year → get_leave_year_bounds returns correct window
    target_date = date(selected_year, 4, 1)
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)
    leave_year_display = f"Apr {selected_year} – Mar {selected_year + 1}"

    users = User.objects.filter(is_active=True).order_by(
        "first_name", "last_name", "username"
    )
    rows = get_admin_leave_balance_rows(users=users, target_date=target_date)

    # Build a sensible range of year options: 2 years back, current, 2 ahead
    year_options = list(range(current_leave_year_start - 2, current_leave_year_start + 3))

    return render(
        request,
        "leave/admin_leave_balance.html",
        {
            "rows": rows,
            "selected_year": selected_year,
            "year_options": year_options,
            "leave_year_display": leave_year_display,
        },
    )