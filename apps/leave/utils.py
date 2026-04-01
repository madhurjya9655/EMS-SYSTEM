#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\leave\utils.py
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


@dataclass
class LeaveBalanceSummary:
    employee: object
    leave_year_start: date
    leave_year_end: date
    total_paid_leaves: Decimal
    paid_leaves_taken: Decimal
    unpaid_leaves: Decimal
    remaining_paid_leaves: Decimal

    @property
    def leave_year_label(self) -> str:
        return f"{self.leave_year_start.strftime('%b %Y')} – {self.leave_year_end.strftime('%b %Y')}"


def get_leave_year_bounds(target_date: date | None = None) -> Tuple[date, date]:
    """
    Return the April–March leave year that contains target_date.

    April 1, YYYY → March 31, YYYY+1
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
    Return the number of leave days that fall inside the given period,
    correctly splitting cross-year leaves at the boundary.
    Half-day leaves that are entirely within the period count as 0.5.
    """
    leave_start = leave.start_date
    leave_end = leave.end_date

    if not leave_start or not leave_end:
        return Decimal("0.0")

    overlap_start = max(leave_start, period_start)
    overlap_end = min(leave_end, period_end)

    if overlap_end < overlap_start:
        return Decimal("0.0")

    # Half-day: only if the entire leave is a single day AND it falls in this period
    if leave.is_half_day and leave_start == leave_end and overlap_start == overlap_end:
        return Decimal("0.5")

    total_days = (overlap_end - overlap_start).days + 1
    return Decimal(str(total_days))


def calculate_leave_balance_for_period(
    employee, leave_year_start: date, leave_year_end: date
) -> LeaveBalanceSummary:
    """
    Compute the leave balance summary for one employee for one leave year.
    Approved leaves that span across the year boundary are split correctly.
    """
    approved_leaves = (
        LeaveRequest.objects.filter(
            employee=employee,
            status=LeaveStatus.APPROVED,
            start_date__lte=leave_year_end,
            end_date__gte=leave_year_start,
        )
        .only("start_date", "end_date", "is_half_day")
        .order_by("start_date", "id")
    )

    total_taken = Decimal("0.0")
    for leave in approved_leaves:
        total_taken += _leave_days_within_period(leave, leave_year_start, leave_year_end)

    paid_taken = min(total_taken, ANNUAL_PAID_LEAVE_QUOTA)
    unpaid_leaves = max(total_taken - ANNUAL_PAID_LEAVE_QUOTA, Decimal("0.0"))
    remaining_paid = max(ANNUAL_PAID_LEAVE_QUOTA - paid_taken, Decimal("0.0"))

    return LeaveBalanceSummary(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        total_paid_leaves=ANNUAL_PAID_LEAVE_QUOTA,
        paid_leaves_taken=paid_taken,
        unpaid_leaves=unpaid_leaves,
        remaining_paid_leaves=remaining_paid,
    )


@transaction.atomic
def sync_employee_leave_balance(
    employee, leave_year_start: date, leave_year_end: date
) -> EmployeeLeaveBalance:
    """Persist / update the EmployeeLeaveBalance row for one employee + one leave year."""
    summary = calculate_leave_balance_for_period(employee, leave_year_start, leave_year_end)
    balance, _created = EmployeeLeaveBalance.objects.update_or_create(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        defaults={
            "total_paid_leaves": summary.total_paid_leaves,
            "paid_leaves_taken": summary.paid_leaves_taken,
            "unpaid_leaves": summary.unpaid_leaves,
            "remaining_paid_leaves": summary.remaining_paid_leaves,
        },
    )
    return balance


def sync_employee_leave_balance_for_range(
    employee,
    range_start: date | None,
    range_end: date | None,
) -> List[EmployeeLeaveBalance]:
    """
    Sync balance for every leave year that the given date range touches.
    Safe to call with None dates — falls back to current leave year.
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
    """Return (and sync) the EmployeeLeaveBalance row for the leave year containing target_date."""
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)
    return sync_employee_leave_balance(employee, leave_year_start, leave_year_end)


def get_employee_leave_balance_summary(
    employee, target_date: date | None = None
) -> LeaveBalanceSummary:
    """
    Return a LeaveBalanceSummary for the employee for the leave year containing target_date.
    Always recalculates from approved leave records — never returns stale zero rows.
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
    )


def get_admin_leave_balance_rows(
    users=None, target_date: date | None = None
) -> List[EmployeeLeaveBalance]:
    """
    Return a list of EmployeeLeaveBalance rows — one per active user — for the
    leave year that contains target_date (defaults to current leave year).

    Each row is synced from real approved leave data, so it is always accurate.
    """
    if users is None:
        users = User.objects.filter(is_active=True).order_by(
            "first_name", "last_name", "username"
        )

    rows: List[EmployeeLeaveBalance] = []
    for user in users:
        rows.append(get_or_sync_employee_leave_balance(user, target_date))
    return rows