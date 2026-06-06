# apps/leave/utils.py
# UPDATED: 2026-06-04
# PURPOSE:
#   Leave balance calculation utilities for BOS Lakshya ERP.
#
# CURRENT BUSINESS RULES PRESERVED:
#   1. Leave deduction happens on apply.
#      Meaning: PENDING + APPROVED leaves are counted as deducted.
#   2. Half-day leave does NOT deduct from yearly paid leave quota.
#      Meaning: half-day blocked_days may display 0.5, but paid balance deduction is 0.
#   3. Rejected / Cancelled leaves do not deduct balance.
#   4. Carry-forward adjustment is preserved and used.
#   5. Leave year remains April -> March.
#   6. Finalized master opening balances are applied safely by calculating the
#      required carry_forward_adjustment.
#   7. No leave history is modified.
#   8. No dashboard hardcoding.

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, List, Tuple

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .models import EmployeeLeaveBalance, LeaveRequest, LeaveStatus

User = get_user_model()

# Existing yearly base quota.
ANNUAL_PAID_LEAVE_QUOTA = Decimal("24.0")

# IMPORTANT:
# Keep this as-is because current production behavior deducts balance on apply.
# Pending leave deducts.
# Approved leave deducts.
# Rejected / Cancelled do not deduct.
ACTIVE_LEAVE_STATUSES = [
    LeaveStatus.PENDING,
    LeaveStatus.APPROVED,
]


# =============================================================================
# DATA STRUCTURES
# =============================================================================

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


# =============================================================================
# LEAVE YEAR HELPERS
# =============================================================================

def get_leave_year_bounds(target_date: date | None = None) -> Tuple[date, date]:
    """
    Return the April–March leave year that contains target_date.

    Example:
        2026-04-01 -> 2026-04-01 to 2027-03-31
        2027-03-15 -> 2026-04-01 to 2027-03-31
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
    Yield successive leave-year periods that overlap with the given date range.

    This keeps cross-year leaves safe.
    Example:
        Leave from 2027-03-30 to 2027-04-02 touches:
        - FY 2026-27
        - FY 2027-28
    """
    if range_end < range_start:
        range_start, range_end = range_end, range_start

    current = range_start

    while current <= range_end:
        leave_year_start, leave_year_end = get_leave_year_bounds(current)

        yield leave_year_start, leave_year_end

        if leave_year_end >= range_end:
            break

        current = leave_year_end + timedelta(days=1)


# =============================================================================
# DEDUCTION HELPERS
# =============================================================================

def _leave_days_within_period(
    leave: LeaveRequest,
    period_start: date,
    period_end: date,
) -> Decimal:
    """
    Return deductible leave days inside one leave-year period.

    CURRENT PRODUCTION RULES PRESERVED:
    - Half-day leave does NOT deduct from yearly paid leave balance.
    - Full-day leave deducts each overlapping calendar day.
    - Cross-year leave is split correctly at leave-year boundary.
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


def calculate_current_deductible_days_for_period(
    employee,
    leave_year_start: date,
    leave_year_end: date,
) -> Decimal:
    """
    Calculate currently deductible leave days for one employee and one leave year.

    CURRENT PRODUCTION RULES PRESERVED:
    - PENDING + APPROVED leaves deduct.
    - Half-day leave deducts 0.
    - REJECTED / CANCELLED leaves do not deduct.
    """
    active_leaves = (
        LeaveRequest.objects
        .filter(
            employee=employee,
            status__in=ACTIVE_LEAVE_STATUSES,
            start_date__lte=leave_year_end,
            end_date__gte=leave_year_start,
        )
        .only(
            "id",
            "start_date",
            "end_date",
            "is_half_day",
            "status",
        )
        .order_by("start_date", "id")
    )

    total_taken = Decimal("0.0")

    for leave in active_leaves:
        total_taken += _leave_days_within_period(
            leave,
            leave_year_start,
            leave_year_end,
        )

    return total_taken


# =============================================================================
# CARRY FORWARD / MASTER BALANCE HELPERS
# =============================================================================

def _get_carry_forward_from_db(
    employee,
    leave_year_start: date,
    leave_year_end: date,
) -> Decimal:
    """
    Read carry_forward_adjustment from the yearly balance row.

    Returns 0 if no row exists.
    """
    try:
        balance = EmployeeLeaveBalance.objects.get(
            employee=employee,
            leave_year_start=leave_year_start,
            leave_year_end=leave_year_end,
        )

        return (
            getattr(balance, "carry_forward_adjustment", Decimal("0.0"))
            or Decimal("0.0")
        )

    except EmployeeLeaveBalance.DoesNotExist:
        return Decimal("0.0")

    except Exception:
        return Decimal("0.0")


def required_carry_forward_for_final_available(
    *,
    final_available,
    current_deductible,
    base_quota: Decimal | None = None,
) -> Decimal:
    """
    Calculate carry_forward_adjustment required to make current system show
    the finalized available balance.

    Current formula:
        remaining_paid_leaves = base_quota + carry_forward_adjustment - current_deductible

    Therefore:
        carry_forward_adjustment = final_available + current_deductible - base_quota

    This is the safe way to import master opening balances without touching
    LeaveRequest history.
    """
    if base_quota is None:
        base_quota = ANNUAL_PAID_LEAVE_QUOTA

    return (
        Decimal(str(final_available))
        + Decimal(str(current_deductible))
        - Decimal(str(base_quota))
    )


@transaction.atomic
def apply_master_available_balance(
    *,
    employee,
    final_available,
    target_date: date | None = None,
) -> EmployeeLeaveBalance:
    """
    Apply finalized master available balance for one employee.

    This preserves current system behavior:
    - PENDING + APPROVED are already deducted.
    - Half-day deducts 0.
    - Rejected / Cancelled are ignored.
    - Existing LeaveRequest history is not modified.

    It updates only EmployeeLeaveBalance.carry_forward_adjustment in a way
    that the existing formula produces the requested final available value.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    final_available = Decimal(str(final_available))

    current_deductible = calculate_current_deductible_days_for_period(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
    )

    required_cf = required_carry_forward_for_final_available(
        final_available=final_available,
        current_deductible=current_deductible,
        base_quota=ANNUAL_PAID_LEAVE_QUOTA,
    )

    balance, _created = EmployeeLeaveBalance.objects.get_or_create(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        defaults={
            "total_paid_leaves": ANNUAL_PAID_LEAVE_QUOTA + required_cf,
            "paid_leaves_taken": current_deductible,
            "unpaid_leaves": Decimal("0.0"),
            "remaining_paid_leaves": final_available,
            "carry_forward_adjustment": required_cf,
        },
    )

    balance.carry_forward_adjustment = required_cf
    balance.total_paid_leaves = ANNUAL_PAID_LEAVE_QUOTA + required_cf
    balance.paid_leaves_taken = current_deductible
    balance.unpaid_leaves = Decimal("0.0")
    balance.remaining_paid_leaves = final_available

    balance.save(
        update_fields=[
            "carry_forward_adjustment",
            "total_paid_leaves",
            "paid_leaves_taken",
            "unpaid_leaves",
            "remaining_paid_leaves",
            "updated_at",
        ]
    )

    # Do not re-sync from LeaveRequest here.
    # For finalized balances, EmployeeLeaveBalance is source of truth.
    return balance


@transaction.atomic
def apply_master_available_balances(
    *,
    name_to_balance: dict,
    target_date: date | None = None,
    strict: bool = True,
) -> List[EmployeeLeaveBalance]:
    """
    Bulk apply finalized available balances only.

    This keeps old compatibility for final available balance imports.
    """
    prepared = []
    errors = []

    for employee_name, final_available in name_to_balance.items():
        user = find_employee_by_display_name(employee_name)

        if user is None:
            errors.append(
                {
                    "name": employee_name,
                    "error": "not_found",
                    "matches": [],
                }
            )
            continue

        if isinstance(user, list):
            errors.append(
                {
                    "name": employee_name,
                    "error": "ambiguous",
                    "matches": [
                        {
                            "id": u.id,
                            "username": getattr(u, "username", ""),
                            "email": getattr(u, "email", ""),
                            "first_name": getattr(u, "first_name", ""),
                            "last_name": getattr(u, "last_name", ""),
                        }
                        for u in user
                    ],
                }
            )
            continue

        prepared.append((user, employee_name, Decimal(str(final_available))))

    if errors and strict:
        raise ValueError(f"Employee matching errors: {errors}")

    updated_rows: List[EmployeeLeaveBalance] = []

    for user, _employee_name, final_available in prepared:
        updated_rows.append(
            apply_master_available_balance(
                employee=user,
                final_available=final_available,
                target_date=target_date,
            )
        )

    return updated_rows


def find_employee_by_display_name(full_name: str):
    """
    Find an active user by full name.

    Returns:
        User object if exactly one match
        None if no match
        list[User] if ambiguous
    """
    full_name = (full_name or "").strip()

    if not full_name:
        return None

    parts = full_name.split()
    first = parts[0]
    last = parts[-1] if len(parts) > 1 else ""

    qs = User.objects.filter(is_active=True).filter(
        Q(first_name__iexact=first, last_name__iexact=last)
        | Q(username__iexact=full_name)
    )

    count = qs.count()

    if count == 0:
        return None

    if count > 1:
        return list(qs)

    return qs.get()


# =============================================================================
# FINALIZED BALANCE ROW HELPERS
# =============================================================================

@transaction.atomic
def apply_finalized_leave_balance_row(
    *,
    employee,
    carry_forward_adjustment,
    total_paid_leaves,
    paid_leaves_taken,
    remaining_paid_leaves,
    unpaid_leaves=Decimal("0.0"),
    target_date: date | None = None,
) -> EmployeeLeaveBalance:
    """
    Store one management-approved finalized yearly balance row exactly as provided.

    This is used when the approved balance sheet is the source of truth for:
    - carry_forward_adjustment
    - total_paid_leaves / Base + CF
    - paid_leaves_taken / Used
    - remaining_paid_leaves / Available

    It does not edit LeaveRequest history.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    balance, _created = EmployeeLeaveBalance.objects.get_or_create(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        defaults={
            "total_paid_leaves": Decimal(str(total_paid_leaves)),
            "paid_leaves_taken": Decimal(str(paid_leaves_taken)),
            "unpaid_leaves": Decimal(str(unpaid_leaves)),
            "remaining_paid_leaves": Decimal(str(remaining_paid_leaves)),
            "carry_forward_adjustment": Decimal(str(carry_forward_adjustment)),
        },
    )

    balance.carry_forward_adjustment = Decimal(str(carry_forward_adjustment))
    balance.total_paid_leaves = Decimal(str(total_paid_leaves))
    balance.paid_leaves_taken = Decimal(str(paid_leaves_taken))
    balance.unpaid_leaves = Decimal(str(unpaid_leaves))
    balance.remaining_paid_leaves = Decimal(str(remaining_paid_leaves))

    balance.save(
        update_fields=[
            "carry_forward_adjustment",
            "total_paid_leaves",
            "paid_leaves_taken",
            "unpaid_leaves",
            "remaining_paid_leaves",
            "updated_at",
        ]
    )

    return balance


@transaction.atomic
def apply_finalized_leave_balance_rows(
    *,
    name_to_row: dict,
    target_date: date | None = None,
    strict: bool = True,
) -> List[EmployeeLeaveBalance]:
    """
    Bulk store management-approved finalized balance rows.

    name_to_row example:
        {
            "Arvind Sangamnerkar": {
                "cf": 8,
                "base_cf": 32,
                "used": 1,
                "available": 31,
            }
        }

    Tuple format is also supported:
        {"Arvind Sangamnerkar": (8, 32, 1, 31)}
    """
    prepared = []
    errors = []

    for employee_name, row in name_to_row.items():
        user = find_employee_by_display_name(employee_name)

        if user is None:
            errors.append(
                {
                    "name": employee_name,
                    "error": "not_found",
                    "matches": [],
                }
            )
            continue

        if isinstance(user, list):
            errors.append(
                {
                    "name": employee_name,
                    "error": "ambiguous",
                    "matches": [
                        {
                            "id": u.id,
                            "username": getattr(u, "username", ""),
                            "email": getattr(u, "email", ""),
                            "first_name": getattr(u, "first_name", ""),
                            "last_name": getattr(u, "last_name", ""),
                        }
                        for u in user
                    ],
                }
            )
            continue

        if isinstance(row, dict):
            cf = row.get("cf")
            base_cf = row.get("base_cf")
            used = row.get("used")
            available = row.get("available")
        else:
            cf, base_cf, used, available = row

        prepared.append((user, employee_name, cf, base_cf, used, available))

    if errors and strict:
        raise ValueError(f"Employee matching errors: {errors}")

    updated_rows: List[EmployeeLeaveBalance] = []

    for user, _employee_name, cf, base_cf, used, available in prepared:
        updated_rows.append(
            apply_finalized_leave_balance_row(
                employee=user,
                carry_forward_adjustment=cf,
                total_paid_leaves=base_cf,
                paid_leaves_taken=used,
                remaining_paid_leaves=available,
                target_date=target_date,
            )
        )

    return updated_rows


# =============================================================================
# BALANCE CALCULATION
# =============================================================================

def calculate_leave_balance_for_period(
    employee,
    leave_year_start: date,
    leave_year_end: date,
) -> LeaveBalanceSummary:
    """
    Compute one employee's leave balance for one leave year.

    This function is kept for investigation/reporting.
    Dashboard read path should use stored EmployeeLeaveBalance.
    """
    total_taken = calculate_current_deductible_days_for_period(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
    )

    carry_forward = _get_carry_forward_from_db(
        employee,
        leave_year_start,
        leave_year_end,
    )

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
    employee,
    leave_year_start: date,
    leave_year_end: date,
) -> EmployeeLeaveBalance:
    """
    Return the stored yearly balance row without recalculating it from LeaveRequest.

    Production rule for finalized FY balances:
    EmployeeLeaveBalance is the source of truth after management approval.

    This intentionally does not overwrite:
    - total_paid_leaves
    - paid_leaves_taken
    - unpaid_leaves
    - remaining_paid_leaves
    - carry_forward_adjustment

    LeaveRequest history is preserved separately for audit and reporting.
    """
    balance, _created = EmployeeLeaveBalance.objects.get_or_create(
        employee=employee,
        leave_year_start=leave_year_start,
        leave_year_end=leave_year_end,
        defaults={
            "total_paid_leaves": ANNUAL_PAID_LEAVE_QUOTA,
            "paid_leaves_taken": Decimal("0.0"),
            "unpaid_leaves": Decimal("0.0"),
            "remaining_paid_leaves": ANNUAL_PAID_LEAVE_QUOTA,
            "carry_forward_adjustment": Decimal("0.0"),
        },
    )

    return balance


def sync_employee_leave_balance_for_range(
    employee,
    range_start: date | None,
    range_end: date | None,
) -> List[EmployeeLeaveBalance]:
    """
    Return stored leave balance rows for every leave year touched by date range.
    Does not recalculate finalized balances from LeaveRequest.
    """
    if not range_start or not range_end:
        current_start, current_end = get_leave_year_bounds()

        return [
            sync_employee_leave_balance(
                employee,
                current_start,
                current_end,
            )
        ]

    if range_end < range_start:
        range_start, range_end = range_end, range_start

    updated_rows: List[EmployeeLeaveBalance] = []

    for leave_year_start, leave_year_end in iter_leave_year_periods(
        range_start,
        range_end,
    ):
        updated_rows.append(
            sync_employee_leave_balance(
                employee,
                leave_year_start,
                leave_year_end,
            )
        )

    return updated_rows


def get_or_sync_employee_leave_balance(
    employee,
    target_date: date | None = None,
) -> EmployeeLeaveBalance:
    """
    Return the stored yearly balance row for the leave year containing target_date.
    Does not recalculate finalized balances from LeaveRequest.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    return sync_employee_leave_balance(
        employee,
        leave_year_start,
        leave_year_end,
    )


def get_employee_leave_balance_summary(
    employee,
    target_date: date | None = None,
) -> LeaveBalanceSummary:
    """
    Return stored yearly leave balance for dashboard and approval pages.

    Management-approved EmployeeLeaveBalance rows are the source of truth.
    Do not recalculate from LeaveRequest on read.
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    balance = sync_employee_leave_balance(
        employee,
        leave_year_start,
        leave_year_end,
    )

    return LeaveBalanceSummary(
        employee=employee,
        leave_year_start=balance.leave_year_start,
        leave_year_end=balance.leave_year_end,
        total_paid_leaves=balance.total_paid_leaves,
        paid_leaves_taken=balance.paid_leaves_taken,
        unpaid_leaves=balance.unpaid_leaves,
        remaining_paid_leaves=balance.remaining_paid_leaves,
        carry_forward_adjustment=(
            getattr(balance, "carry_forward_adjustment", Decimal("0.0"))
            or Decimal("0.0")
        ),
        base_quota=ANNUAL_PAID_LEAVE_QUOTA,
    )


def get_admin_leave_balance_rows(
    users=None,
    target_date: date | None = None,
) -> List[EmployeeLeaveBalance]:
    """
    Return stored leave balance rows for admin / manager views.
    Does not recalculate finalized balances from LeaveRequest.
    """
    if users is None:
        users = User.objects.filter(is_active=True).order_by(
            "first_name",
            "last_name",
            "username",
        )

    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    rows: List[EmployeeLeaveBalance] = []

    for user in users:
        balance, _created = EmployeeLeaveBalance.objects.get_or_create(
            employee=user,
            leave_year_start=leave_year_start,
            leave_year_end=leave_year_end,
            defaults={
                "total_paid_leaves": ANNUAL_PAID_LEAVE_QUOTA,
                "paid_leaves_taken": Decimal("0.0"),
                "unpaid_leaves": Decimal("0.0"),
                "remaining_paid_leaves": ANNUAL_PAID_LEAVE_QUOTA,
                "carry_forward_adjustment": Decimal("0.0"),
            },
        )
        rows.append(balance)

    return rows


# =============================================================================
# VALIDATION / REPORTING HELPERS
# =============================================================================

def preview_master_available_balances(
    *,
    name_to_balance: dict,
    target_date: date | None = None,
) -> List[dict]:
    """
    Preview what will happen before applying master balances.

    This is useful in Render shell before running actual update.

    Returns rows like:
        {
            "employee_name": "...",
            "user_id": 1,
            "final_available": Decimal("31"),
            "current_deductible": Decimal("0"),
            "required_carry_forward": Decimal("7"),
            "current_remaining": Decimal("24"),
            "new_expected_remaining": Decimal("31"),
        }
    """
    leave_year_start, leave_year_end = get_leave_year_bounds(target_date)

    rows: List[dict] = []

    for employee_name, final_available in name_to_balance.items():
        user = find_employee_by_display_name(employee_name)

        if user is None:
            rows.append(
                {
                    "employee_name": employee_name,
                    "status": "not_found",
                    "matches": [],
                }
            )
            continue

        if isinstance(user, list):
            rows.append(
                {
                    "employee_name": employee_name,
                    "status": "ambiguous",
                    "matches": [
                        {
                            "id": u.id,
                            "username": getattr(u, "username", ""),
                            "email": getattr(u, "email", ""),
                            "first_name": getattr(u, "first_name", ""),
                            "last_name": getattr(u, "last_name", ""),
                        }
                        for u in user
                    ],
                }
            )
            continue

        final_available_dec = Decimal(str(final_available))

        current_deductible = calculate_current_deductible_days_for_period(
            employee=user,
            leave_year_start=leave_year_start,
            leave_year_end=leave_year_end,
        )

        required_cf = required_carry_forward_for_final_available(
            final_available=final_available_dec,
            current_deductible=current_deductible,
            base_quota=ANNUAL_PAID_LEAVE_QUOTA,
        )

        try:
            current_summary = get_employee_leave_balance_summary(
                user,
                target_date=leave_year_start,
            )
            current_remaining = current_summary.remaining_paid_leaves
            existing_cf = current_summary.carry_forward_adjustment
        except Exception:
            current_remaining = None
            existing_cf = None

        rows.append(
            {
                "employee_name": employee_name,
                "status": "ok",
                "user_id": user.id,
                "username": getattr(user, "username", ""),
                "email": getattr(user, "email", ""),
                "final_available": final_available_dec,
                "current_deductible": current_deductible,
                "required_carry_forward": required_cf,
                "existing_carry_forward": existing_cf,
                "current_remaining": current_remaining,
                "new_expected_remaining": final_available_dec,
                "leave_year_start": leave_year_start,
                "leave_year_end": leave_year_end,
            }
        )

    return rows


def verify_master_available_balances(
    *,
    name_to_balance: dict,
    target_date: date | None = None,
) -> List[dict]:
    """
    Verify current DB balance against master sheet values.

    Returns rows with:
        status = match / mismatch / not_found / ambiguous
    """
    rows: List[dict] = []

    for employee_name, expected_value in name_to_balance.items():
        user = find_employee_by_display_name(employee_name)

        if user is None:
            rows.append(
                {
                    "employee_name": employee_name,
                    "status": "not_found",
                    "expected": Decimal(str(expected_value)),
                    "actual": None,
                }
            )
            continue

        if isinstance(user, list):
            rows.append(
                {
                    "employee_name": employee_name,
                    "status": "ambiguous",
                    "expected": Decimal(str(expected_value)),
                    "actual": None,
                    "matches": [
                        {
                            "id": u.id,
                            "username": getattr(u, "username", ""),
                            "email": getattr(u, "email", ""),
                            "first_name": getattr(u, "first_name", ""),
                            "last_name": getattr(u, "last_name", ""),
                        }
                        for u in user
                    ],
                }
            )
            continue

        expected = Decimal(str(expected_value))

        summary = get_employee_leave_balance_summary(
            user,
            target_date,
        )

        actual = summary.remaining_paid_leaves

        rows.append(
            {
                "employee_name": employee_name,
                "status": "match" if actual == expected else "mismatch",
                "user_id": user.id,
                "expected": expected,
                "actual": actual,
                "total_paid_leaves": summary.total_paid_leaves,
                "paid_leaves_taken": summary.paid_leaves_taken,
                "unpaid_leaves": summary.unpaid_leaves,
                "carry_forward_adjustment": summary.carry_forward_adjustment,
                "leave_year_start": summary.leave_year_start,
                "leave_year_end": summary.leave_year_end,
            }
        )

    return rows


# =============================================================================
# TASK BLOCKING UTILITIES
# =============================================================================

def is_sunday(check_date: date) -> bool:
    return check_date.weekday() == 6


def is_holiday(check_date: date) -> bool:
    """
    Return True if the given date is configured as a holiday.
    """
    try:
        from apps.settings.models import Holiday

        return Holiday.objects.filter(date=check_date).exists()

    except Exception:
        return False


def is_non_working_day(check_date: date) -> bool:
    """
    Non-working day = Holiday OR Sunday.
    """
    if is_holiday(check_date):
        return True

    if is_sunday(check_date):
        return True

    return False


def is_employee_on_leave(employee, check_date: date) -> bool:
    """
    Return True if employee has active leave on the given date.

    Current production behavior:
    - Pending leave blocks work.
    - Approved leave blocks work.
    - Rejected / Cancelled leave does not block work.
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
    check_date: date,
    employee=None,
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

    if employee is not None and is_employee_on_leave(employee, check_date):
        return True, "leave"

    return False, ""


def get_employees_on_leave_for_date(check_date: date) -> list:
    """
    Return employee IDs who are on active leave for the given date.

    Active leave means:
    - PENDING
    - APPROVED
    """
    try:
        leave_qs = (
            LeaveRequest.objects
            .filter(
                status__in=ACTIVE_LEAVE_STATUSES,
                start_date__lte=check_date,
                end_date__gte=check_date,
            )
            .values_list("employee_id", flat=True)
            .distinct()
        )

        return list(leave_qs)

    except Exception:
        return []