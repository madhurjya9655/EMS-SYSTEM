# apps/leave/utils.py
# UPDATED: 2026-04-05
# CHANGES:
#   1. Leave deduction happens on apply (PENDING + APPROVED deduct)
#   2. Half-day leave does NOT deduct from yearly paid leave quota
#   3. Rejected / Cancelled leave adds balance back automatically
#   4. Carry-forward adjustment supported and preserved from DB
#   5. Leave year remains April -> March
#   6. Task blocking helpers aligned with PENDING + APPROVED logic
#   7. Holiday > Sunday > Leave priority preserved

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, List, Tuple

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from .models import EmployeeLeaveBalance, LeaveRequest, LeaveStatus

User = get_user_model()

ANNUAL_PAID_LEAVE_QUOTA = Decimal("24.0")

# Applied leave should deduct immediately.
# Approved keeps deduction.
# Rejected / Cancelled automatically stop deducting because they are excluded here.
ACTIVE_LEAVE_STATUSES = [LeaveStatus.PENDING, LeaveStatus.APPROVED]


@dataclass
class LeaveBalanceSummary:
    employee: object
    leave_year_start: date
    leave_year_end: date
    total_paid_leaves: Decimal
    paid_leaves_taken: Decimal
    unpaid_leaves: Decimal
    remaining_paid_leaves: Decimal
    carry_forward_adjustment: Decimal
    base_quota: Decimal

    @property
    def leave_year_label(self) -> str:
        return f"{self.leave_year_start.strftime('%b %Y')} – {self.leave_year_end.strftime('%b %Y')}"


def get_leave_year_bounds(target_date: date | None = None) -> Tuple[date, date]:
    """
    Return the April–March leave year that contains target_date.
    April 1, YYYY -> March 31, YYYY+1
    """
    target_date = target_date or timezone.localdate()
    if target_date.month >= 4:
        start = date(target_date.year, 4, 1)
        end = date(target_date.year + 1, 3, 31)
    else:
        start = date(target_date.year - 1, 4, 1)
        end = date(target_date.year, 3, 31)
    return start, end


def get_leave_year_label(target_date: date | None = None) -> str:
    start, end = get_leave_year_bounds(target_date)
    return f"{start.strftime('%b %Y')} – {end.strftime('%b %Y')}"


def iter_leave_year_periods(range_start: date, range_end: date) -> Iterable[Tuple[date, date]]:
    """
    Yield successive (leave_year_start, leave_year_end) tuples that overlap
    with the given date range. Handles cross-boundary leaves correctly.
    """
    if range_end < range_start:
        range_start, range_end = range_end, range_start

    current = range_start
    while current <= range_end:
        year_start, year_end = get_leave_year_bounds(current)
        yield year_start, year_end
        if year_end >= range_end:
            break
        current = year_end + timedelta(days=1)


def _leave_days_within_period(
    leave: LeaveRequest, period_start: date, period_end: date
) -> Decimal:
    """
    Return the number of deductible leave days that fall inside the given period.

    BUSINESS RULES:
    - Half-day leave does NOT deduct from yearly paid leave balance
    - Full-day leave deducts each overlapping calendar day
    - Cross-year leave is split correctly at leave-year boundary
    """
    if leave.is_half_day:
        return Decimal("0.0")

    leave_start = leave.start_date
    leave_end = leave.end_date

    if not leave_start or not leave_end:
        return Decimal("0.0")

    overlap_start = max(leave_start, period_start)
    overlap_end = min(leave_end, period_end)

    if overlap_end < overlap_start:
        return Decimal("0.0")

    total_days = (overlap_end - overlap_start).days + 1
    return Decimal(str(total_days))


def _get_carry_forward_from_db(
    employee, leave_year_start: date, leave_year_end: date
) -> Decimal:
    """
    Read carry_forward_adjustment from the existing yearly balance row.
    Returns 0 if the row does not exist yet.
    """
    try:
        balance = EmployeeLeaveBalance.objects.get(
            employee=employee,
            leave_year_start=leave_year_start,
            leave_year_end=leave_year_end,
        )
        return getattr(balance, "carry_forward_adjustment", Decimal("0.0")) or Decimal("0.0")
    except EmployeeLeaveBalance.DoesNotExist:
        return Decimal("0.0")
    except Exception:
        return Decimal("0.0")


def calculate_leave_balance_for_period(
    employee, leave_year_start: date, leave_year_end: date
) -> "LeaveBalanceSummary":
    """
    Compute one employee's leave balance for one leave year.

    RULES:
    - Deduct on apply: PENDING + APPROVED count
    - Half-day does not deduct from yearly paid leave
    - Rejected / Cancelled do not count
    - Carry-forward adjustment changes effective yearly quota
    """
    active_leaves = (
        LeaveRequest.objects.filter(
            employee=employee,
            status__in=ACTIVE_LEAVE_STATUSES,
            start_date__lte=leave_year_end,
            end_date__gte=leave_year_start,
        )
        .only("start_date", "end_date", "is_half_day", "status")
        .order_by("start_date", "id")
    )

    total_taken = Decimal("0.0")
    for leave in active_leaves:
        total_taken += _leave_days_within_period(leave, leave_year_start, leave_year_end)

    carry_forward = _get_carry_forward_from_db(employee, leave_year_start, leave_year_end)

    effective_quota = ANNUAL_PAID_LEAVE_QUOTA + carry_forward
    effective_quota = max(effective_quota, Decimal("0.0"))

    paid_taken = min(total_taken, effective_quota)
    unpaid_leaves = max(total_taken - effective_quota, Decimal("0.0"))
    remaining_paid = max(effective_quota - paid_taken, Decimal("0.0"))

    return LeaveBalanceSummary(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        total_paid_leaves=effective_quota,
        paid_leaves_taken=paid_taken,
        unpaid_leaves=unpaid_leaves,
        remaining_paid_leaves=remaining_paid,
        carry_forward_adjustment=carry_forward,
        base_quota=ANNUAL_PAID_LEAVE_QUOTA,
    )


@transaction.atomic
def sync_employee_leave_balance(
    employee, leave_year_start: date, leave_year_end: date
) -> EmployeeLeaveBalance:
    """
    Create or update the yearly balance row for one employee.

    IMPORTANT:
    carry_forward_adjustment is preserved from DB and never overwritten here.
    """
    summary = calculate_leave_balance_for_period(employee, leave_year_start, leave_year_end)

    balance, created = EmployeeLeaveBalance.objects.get_or_create(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        defaults={
            "total_paid_leaves": summary.total_paid_leaves,
            "paid_leaves_taken": summary.paid_leaves_taken,
            "unpaid_leaves": summary.unpaid_leaves,
            "remaining_paid_leaves": summary.remaining_paid_leaves,
            "carry_forward_adjustment": Decimal("0.0"),
        },
    )

    if not created:
        balance.total_paid_leaves = summary.total_paid_leaves
        balance.paid_leaves_taken = summary.paid_leaves_taken
        balance.unpaid_leaves = summary.unpaid_leaves
        balance.remaining_paid_leaves = summary.remaining_paid_leaves
        balance.save(
            update_fields=[
                "total_paid_leaves",
                "paid_leaves_taken",
                "unpaid_leaves",
                "remaining_paid_leaves",
                "updated_at",
            ]
        )

    return balance


def sync_employee_leave_balance_for_range(
    employee,
    range_start: date | None,
    range_end: date | None,
) -> List[EmployeeLeaveBalance]:
    """
    Sync leave balance for every leave year touched by the given date range.
    """
    if not range_start or not range_end:
        current_start, current_end = get_leave_year_bounds()
        return [sync_employee_leave_balance(employee, current_start, current_end)]

    if range_end < range_start:
        range_start, range_end = range_end, range_start

    updated_rows: List[EmployeeLeaveBalance] = []
    for leave_year_start, leave_year_end in iter_leave_year_periods(range_start, range_end):
        updated_rows.append(
            sync_employee_leave_balance(employee, leave_year_start, leave_year_end)
        )
    return updated_rows


def get_or_sync_employee_leave_balance(
    employee, target_date: date | None = None
) -> EmployeeLeaveBalance:
    """
    Return the synced yearly balance row for the leave year containing target_date.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)
    return sync_employee_leave_balance(employee, leave_year_start, leave_year_end)


def get_employee_leave_balance_summary(
    employee, target_date: date | None = None
) -> "LeaveBalanceSummary":
    """
    Return a fresh LeaveBalanceSummary for the employee for the leave year containing target_date.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)
    balance = sync_employee_leave_balance(employee, leave_year_start, leave_year_end)
    return LeaveBalanceSummary(
        employee=employee,
        leave_year_start=balance.leave_year_start,
        leave_year_end=balance.leave_year_end,
        total_paid_leaves=balance.total_paid_leaves,
        paid_leaves_taken=balance.paid_leaves_taken,
        unpaid_leaves=balance.unpaid_leaves,
        remaining_paid_leaves=balance.remaining_paid_leaves,
        carry_forward_adjustment=getattr(balance, "carry_forward_adjustment", Decimal("0.0")) or Decimal("0.0"),
        base_quota=ANNUAL_PAID_LEAVE_QUOTA,
    )


def get_admin_leave_balance_rows(
    users=None, target_date: date | None = None
) -> List[EmployeeLeaveBalance]:
    """
    Return one synced leave balance row per active user for the leave year containing target_date.
    """
    if users is None:
        users = User.objects.filter(is_active=True).order_by(
            "first_name", "last_name", "username"
        )

    rows: List[EmployeeLeaveBalance] = []
    for user in users:
        rows.append(get_or_sync_employee_leave_balance(user, target_date))
    return rows


# =============================================================================
# TASK BLOCKING UTILITIES
# =============================================================================

def is_sunday(check_date: date) -> bool:
    return check_date.weekday() == 6


def is_holiday(check_date: date) -> bool:
    """
    Return True if the given date is a configured holiday in the database.
    """
    try:
        from apps.settings.models import Holiday
        return Holiday.objects.filter(date=check_date).exists()
    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    """
    Non-working = Holiday OR Sunday.
    """
    if is_holiday(check_date):
        return True
    if is_sunday(check_date):
        return True
    return False


def is_employee_on_leave(employee, check_date: date) -> bool:
    """
    Full-day or half-day applied leave blocks work on that date.
    Pending and approved both block task generation.
    """
    if employee is None:
        return False
    try:
        return LeaveRequest.objects.filter(
            employee=employee,
            status__in=ACTIVE_LEAVE_STATUSES,
            start_date__lte=check_date,
            end_date__gte=check_date,
        ).exists()
    except Exception:
        return False


def should_skip_task_generation(
    check_date: date, employee=None
) -> Tuple[bool, str]:
    """
    Priority order:
    1. Holiday
    2. Sunday
    3. Employee on leave
    4. Otherwise generate tasks
    """
    try:
        from apps.settings.models import Holiday
        if Holiday.objects.filter(date=check_date).exists():
            return True, "holiday"
    except Exception:
        pass

    if check_date.weekday() == 6:
        return True, "sunday"

    if employee is not None:
        if is_employee_on_leave(employee, check_date):
            return True, "leave"

    return False, ""


def get_employees_on_leave_for_date(check_date: date) -> list:
    """
    Return employee IDs who are on active leave for the given date.
    """
    try:
        leave_qs = LeaveRequest.objects.filter(
            status__in=ACTIVE_LEAVE_STATUSES,
            start_date__lte=check_date,
            end_date__gte=check_date,
        ).values_list("employee_id", flat=True).distinct()
        return list(leave_qs)
    except Exception:
        return []