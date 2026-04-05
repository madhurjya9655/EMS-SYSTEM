# apps/leave/utils.py
# UPDATED: 2026-04-05
# CHANGES:
#   1. Leave balance now counts PENDING + APPROVED leaves (not just APPROVED)
#   2. Half-day leaves do NOT deduct from the 24-leave quota
#   3. carry_forward_adjustment field support added
#   4. is_employee_on_leave() helper added for task engine
#   5. should_skip_task_generation() added - Holiday > Sunday > Leave priority
#   6. is_holiday() and is_sunday() helpers added

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, List, Optional, Tuple

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from .models import EmployeeLeaveBalance, LeaveRequest, LeaveStatus

User = get_user_model()

ANNUAL_PAID_LEAVE_QUOTA = Decimal("24.0")

# ✅ CHANGED: Both PENDING and APPROVED leaves deduct from balance
ACTIVE_LEAVE_STATUSES = [LeaveStatus.PENDING, LeaveStatus.APPROVED]


@dataclass
class LeaveBalanceSummary:
    employee: object
    leave_year_start: date
    leave_year_end: date
    total_paid_leaves: Decimal          # Effective quota after carry-forward
    paid_leaves_taken: Decimal          # Full-day leaves only (pending + approved)
    unpaid_leaves: Decimal
    remaining_paid_leaves: Decimal
    carry_forward_adjustment: Decimal   # Negative = penalty from last year excess
    base_quota: Decimal                 # Always 24

    @property
    def leave_year_label(self) -> str:
        return f"{self.leave_year_start.strftime('%b %Y')} \u2013 {self.leave_year_end.strftime('%b %Y')}"


def get_leave_year_bounds(target_date: date | None = None) -> Tuple[date, date]:
    """
    Return the April\u2013March leave year that contains target_date.
    April 1, YYYY \u2192 March 31, YYYY+1
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
    return f"{start.strftime('%b %Y')} \u2013 {end.strftime('%b %Y')}"


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
    Return the number of leave days that fall inside the given period.

    RULES:
    - Half-day leaves do NOT deduct from quota (returns 0.0 always)
    - Full-day leaves count each overlapping calendar day
    - Cross-year leaves are split at year boundary
    """
    # ✅ NEW RULE: Half day does NOT deduct leave balance at all
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
    Safely read carry_forward_adjustment from an existing EmployeeLeaveBalance row.
    Returns 0 if row doesn't exist or field is missing.
    This is safe to call even before the balance row is created.
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
    Compute the leave balance summary for one employee for one leave year.

    KEY CHANGES FROM ORIGINAL:
    - Counts PENDING + APPROVED leaves (not just APPROVED)
    - Half-day leaves do NOT count toward the 24-leave quota (return 0 days)
    - carry_forward_adjustment (set separately in DB) reduces effective quota
      e.g. carry_forward = -4 means effective quota = 24 - 4 = 20
    """
    # ✅ CHANGED: Count PENDING + APPROVED (not just APPROVED)
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

    # Read carry-forward from DB (set via Render shell or management command)
    carry_forward = _get_carry_forward_from_db(employee, leave_year_start, leave_year_end)

    # Effective quota = 24 + carry_forward
    # carry_forward is negative for excess (penalty), so quota reduces
    effective_quota = ANNUAL_PAID_LEAVE_QUOTA + carry_forward
    # Quota cannot go below 0
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
    Persist / update the EmployeeLeaveBalance row for one employee + one leave year.

    NOTE: carry_forward_adjustment is preserved from the existing row.
    It must be set separately via Render shell (see README/docs).
    We never overwrite it here to prevent accidental loss of manual adjustments.
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
        # Update calculated fields but NEVER overwrite carry_forward_adjustment
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
    Sync balance for every leave year that the given date range touches.
    Safe to call with None dates \u2014 falls back to current leave year.
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
) -> "LeaveBalanceSummary":
    """
    Return a LeaveBalanceSummary for the employee for the leave year containing target_date.
    Always recalculates from active leave records (pending + approved).
    Never returns stale zero rows.
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
    Return a list of EmployeeLeaveBalance rows \u2014 one per active user \u2014 for the
    leave year that contains target_date (defaults to current leave year).

    Each row is synced from real active leave data (pending + approved),
    so it is always accurate.
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
    """Return True if the date is a Sunday (weekday 6)."""
    return check_date.weekday() == 6


def is_holiday(check_date: date) -> bool:
    """
    Return True if the given date is a configured holiday in the database.
    Does NOT check Sunday here \u2014 use is_non_working_day() for combined check.
    """
    try:
        from apps.settings.models import Holiday
        return Holiday.objects.filter(date=check_date).exists()
    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    """
    Return True if the date is non-working.
    Non-working = Holiday OR Sunday.
    Priority: Holiday first, then Sunday.
    """
    if is_holiday(check_date):
        return True
    if is_sunday(check_date):
        return True
    return False


def is_employee_on_leave(employee, check_date: date) -> bool:
    """
    Return True if employee has PENDING or APPROVED leave on the given date.
    Used by the recurring task engine to skip task generation.

    Both PENDING and APPROVED leaves block task generation.
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
    Master check for task generation blocking.

    Priority order (highest to lowest):
    1. Holiday (database configured) \u2192 skip
    2. Sunday (automatic)            \u2192 skip
    3. Employee on leave             \u2192 skip
    4. Normal workday                \u2192 generate task

    Returns:
        (should_skip: bool, reason: str)
        reason is one of: "holiday", "sunday", "leave", "" (empty = generate)

    Usage in recurring task engine:
        skip, reason = should_skip_task_generation(task_date, employee)
        if skip:
            logger.info("Skipping task for %s on %s: %s", employee, task_date, reason)
            return
    """
    # Priority 1: Check if it's a holiday
    try:
        from apps.settings.models import Holiday
        if Holiday.objects.filter(date=check_date).exists():
            return True, "holiday"
    except Exception:
        pass

    # Priority 2: Check if it's Sunday
    if check_date.weekday() == 6:
        return True, "sunday"

    # Priority 3: Check if employee is on leave (pending or approved)
    if employee is not None:
        if is_employee_on_leave(employee, check_date):
            return True, "leave"

    # All checks passed \u2014 generate the task
    return False, ""


def get_employees_on_leave_for_date(check_date: date) -> list:
    """
    Return list of employees who have active leave (PENDING or APPROVED) on check_date.
    Useful for dashboard filtering and task engine bulk checks.
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