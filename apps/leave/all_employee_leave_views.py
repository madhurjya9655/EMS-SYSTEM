#apps\leave\all_employee_leave_views.py
from __future__ import annotations

from decimal import Decimal

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_http_methods

from apps.recruitment.models import Employee
from .all_employee_leave_forms import EmployeeLeaveBalanceAdminForm
from .models import EmployeeLeaveBalance, LeaveBalanceAudit, LeaveRequest, LeaveStatus, LeaveType
from .utils import get_leave_year_bounds

User = get_user_model()


def _can_administer_leave(user) -> bool:
    if not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    if user.has_perm("leave.change_employeeleavebalance"):
        return True
    return user.groups.filter(name__in=["Admin", "HR", "Super Admin"]).exists()


def _require_leave_admin(user) -> None:
    if not _can_administer_leave(user):
        raise PermissionDenied("You do not have permission to administer employee leave balances.")


def _employee_meta(user):
    record = getattr(user, "employee_record", None)
    mapping = getattr(user, "approver_mapping", None)
    manager = getattr(mapping, "reporting_person", None) or getattr(record, "reporting_officer", None)
    return {
        "department": getattr(record, "department", "") or getattr(getattr(user, "profile", None), "department", ""),
        "manager": manager,
        "status": "Active" if user.is_active else "Inactive",
    }


@login_required
def all_employee_leave(request):
    _require_leave_admin(request.user)

    fy_start, fy_end = get_leave_year_bounds()
    q = (request.GET.get("q") or "").strip()
    department = (request.GET.get("department") or "").strip()
    manager_id = (request.GET.get("manager") or "").strip()
    financial_year = (request.GET.get("financial_year") or "").strip()
    status = (request.GET.get("status") or "").strip()
    leave_type_id = (request.GET.get("leave_type") or "").strip()

    if financial_year:
        try:
            start_year = int(financial_year)
            fy_start = fy_start.replace(year=start_year)
            fy_end = fy_end.replace(year=start_year + 1)
        except (TypeError, ValueError):
            pass

    users = User.objects.select_related("employee_record", "profile", "approver_mapping__reporting_person").all()
    if q:
        users = users.filter(
            Q(first_name__icontains=q) | Q(last_name__icontains=q) |
            Q(username__icontains=q) | Q(email__icontains=q) |
            Q(employee_record__email__icontains=q)
        )
    if department:
        users = users.filter(employee_record__department=department)
    if manager_id.isdigit():
        users = users.filter(approver_mapping__reporting_person_id=int(manager_id))
    if status == "active":
        users = users.filter(is_active=True)
    elif status == "inactive":
        users = users.filter(is_active=False)
    if leave_type_id.isdigit():
        users = users.filter(leave_requests__leave_type_id=int(leave_type_id)).distinct()

    users = users.order_by("first_name", "last_name", "username")
    balances = {
        row.employee_id: row
        for row in EmployeeLeaveBalance.objects.filter(
            leave_year_start=fy_start,
            leave_year_end=fy_end,
            employee__in=users,
        )
    }

    rows = []
    for user in users:
        balance = balances.get(user.id)
        if balance is None:
            balance = EmployeeLeaveBalance(
                employee=user,
                leave_year_start=fy_start,
                leave_year_end=fy_end,
                total_paid_leaves=Decimal("24.0"),
                paid_leaves_taken=Decimal("0.0"),
                unpaid_leaves=Decimal("0.0"),
                remaining_paid_leaves=Decimal("24.0"),
                carry_forward_adjustment=Decimal("0.0"),
                opening_adjustment=Decimal("0.0"),
            )
        meta = _employee_meta(user)
        pending = LeaveRequest.objects.filter(employee=user, status=LeaveStatus.PENDING, start_date__lte=fy_end, end_date__gte=fy_start).count()
        approved = LeaveRequest.objects.filter(employee=user, status=LeaveStatus.APPROVED, start_date__lte=fy_end, end_date__gte=fy_start).count()
        rows.append({"user": user, "balance": balance, "meta": meta, "pending": pending, "approved": approved})

    aggregate = EmployeeLeaveBalance.objects.filter(leave_year_start=fy_start, leave_year_end=fy_end).aggregate(
        available=Sum("remaining_paid_leaves"), paid_used=Sum("paid_leaves_taken"), unpaid=Sum("unpaid_leaves")
    )
    summary = {
        "total_employees": users.count(),
        "total_available": aggregate["available"] or Decimal("0.0"),
        "total_paid_used": aggregate["paid_used"] or Decimal("0.0"),
        "total_pending": LeaveRequest.objects.filter(status=LeaveStatus.PENDING, start_date__lte=fy_end, end_date__gte=fy_start).count(),
        "total_unpaid": aggregate["unpaid"] or Decimal("0.0"),
        "negative_balance": EmployeeLeaveBalance.objects.filter(leave_year_start=fy_start, leave_year_end=fy_end, remaining_paid_leaves__lt=0).count(),
        "with_carry_forward": EmployeeLeaveBalance.objects.filter(leave_year_start=fy_start, leave_year_end=fy_end).exclude(carry_forward_adjustment=0).count(),
    }

    return render(request, "leave/admin/all_employee_leave/list.html", {
        "rows": rows,
        "summary": summary,
        "departments": Employee.objects.exclude(department="").values_list("department", flat=True).distinct().order_by("department"),
        "managers": User.objects.filter(reports_for_approval__isnull=False).distinct().order_by("first_name", "last_name"),
        "leave_types": LeaveType.objects.all().order_by("name"),
        "fy_start": fy_start,
        "fy_end": fy_end,
    })


@login_required
def employee_leave_detail(request, employee_id):
    _require_leave_admin(request.user)
    employee = get_object_or_404(User, pk=employee_id)
    balances = employee.leave_balances.order_by("-leave_year_start")
    leaves = employee.leave_requests.select_related("leave_type", "reporting_person", "approver").order_by("-start_date", "-id")
    audits = LeaveBalanceAudit.objects.filter(employee=employee).select_related("changed_by", "balance").order_by("-changed_at")[:100]
    return render(request, "leave/admin/all_employee_leave/detail.html", {
        "employee": employee, "balances": balances, "leaves": leaves, "audits": audits,
    })


@login_required
@require_http_methods(["GET", "POST"])
def edit_leave_balance(request, pk):
    _require_leave_admin(request.user)
    balance = get_object_or_404(EmployeeLeaveBalance.objects.select_related("employee"), pk=pk)
    before = LeaveBalanceAudit.snapshot(balance)
    form = EmployeeLeaveBalanceAdminForm(request.POST or None, instance=balance)
    if request.method == "POST" and form.is_valid():
        with transaction.atomic():
            changed = form.save(commit=False)
            changed.total_paid_leaves = form.cleaned_data["effective_leave"]
            changed.remaining_paid_leaves = form.cleaned_data["available_leave"]
            changed.save()
            LeaveBalanceAudit.objects.create(
                balance=changed,
                employee=changed.employee,
                changed_by=request.user,
                action=LeaveBalanceAudit.Action.UPDATED,
                before=before,
                after=LeaveBalanceAudit.snapshot(changed),
                remarks=form.cleaned_data.get("remarks", ""),
            )
        messages.success(request, "Leave balance updated successfully.")
        return redirect("leave:employee_leave_detail", employee_id=balance.employee_id)
    return render(request, "leave/admin/all_employee_leave/form.html", {"form": form, "balance": balance})


@login_required
@require_http_methods(["GET", "POST"])
def delete_leave_balance(request, pk):
    _require_leave_admin(request.user)
    balance = get_object_or_404(EmployeeLeaveBalance.objects.select_related("employee"), pk=pk)
    employee_id = balance.employee_id
    if request.method == "POST" and request.POST.get("confirm") == "DELETE":
        with transaction.atomic():
            LeaveBalanceAudit.objects.create(
                balance=None,
                employee=balance.employee,
                changed_by=request.user,
                action=LeaveBalanceAudit.Action.DELETED,
                before=LeaveBalanceAudit.snapshot(balance),
                after={},
                remarks=(request.POST.get("remarks") or "").strip(),
            )
            balance.delete()
        messages.success(request, "Incorrect leave balance record deleted. Leave history was not changed.")
        return redirect("leave:employee_leave_detail", employee_id=employee_id)
    return render(request, "leave/admin/all_employee_leave/confirm_delete.html", {"balance": balance})
