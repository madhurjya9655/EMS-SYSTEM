# apps/leave/management/commands/recalculate_leave_balances.py
# USAGE:
#   python manage.py recalculate_leave_balances
#   python manage.py recalculate_leave_balances --year 2026
#   python manage.py recalculate_leave_balances --employee-id 7
#   python manage.py recalculate_leave_balances --all-years
#
# NOTE: carry_forward_adjustment is set separately via Render PostgreSQL shell.
# This command only recalculates the computed fields (used, remaining, unpaid).
# It never overwrites carry_forward_adjustment.

from __future__ import annotations

from datetime import date

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand

User = get_user_model()


class Command(BaseCommand):
    help = (
        "Recalculate leave balances for all (or specific) employees. "
        "Counts both PENDING and APPROVED leaves. "
        "Half-day leaves do NOT deduct from quota. "
        "carry_forward_adjustment is read from DB and respected (never overwritten)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--year",
            type=int,
            default=None,
            help=(
                "Leave year start (e.g., 2026 means Apr 2026 - Mar 2027). "
                "Default: current leave year."
            ),
        )
        parser.add_argument(
            "--employee-id",
            type=int,
            default=None,
            dest="employee_id",
            help="Only recalculate for this specific employee ID.",
        )
        parser.add_argument(
            "--all-years",
            action="store_true",
            dest="all_years",
            help="Recalculate for both the previous and current leave year.",
        )

    def handle(self, *args, **options):
        from apps.leave.utils import (
            get_leave_year_bounds,
            sync_employee_leave_balance,
        )

        year = options.get("year")
        employee_id = options.get("employee_id")
        all_years = options.get("all_years", False)

        # Determine which leave years to process
        if year:
            periods = [(date(year, 4, 1), date(year + 1, 3, 31))]
        elif all_years:
            # Previous year + current year
            today = date.today()
            cur_start, cur_end = get_leave_year_bounds(today)
            # Previous year
            prev_end = date(cur_start.year, 3, 31)
            prev_start = date(cur_start.year - 1, 4, 1)
            periods = [(prev_start, prev_end), (cur_start, cur_end)]
        else:
            cur_start, cur_end = get_leave_year_bounds()
            periods = [(cur_start, cur_end)]

        # Determine which users to process
        if employee_id:
            try:
                users = [User.objects.get(id=employee_id)]
                self.stdout.write(
                    self.style.WARNING(
                        f"Processing only employee ID {employee_id}: "
                        f"{users[0].get_full_name() or users[0].username}"
                    )
                )
            except User.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f"Employee ID {employee_id} not found.")
                )
                return
        else:
            users = list(User.objects.filter(is_active=True).order_by("id"))
            self.stdout.write(
                self.style.SUCCESS(f"Processing {len(users)} active employees.")
            )

        total_ok = 0
        total_failed = 0

        for leave_year_start, leave_year_end in periods:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n{'='*60}"
                    f"\nLeave Year: {leave_year_start} \u2192 {leave_year_end}"
                    f"\n{'='*60}"
                )
            )

            for user in users:
                try:
                    balance = sync_employee_leave_balance(user, leave_year_start, leave_year_end)
                    cf = getattr(balance, "carry_forward_adjustment", 0) or 0
                    cf_str = f"CF:{cf:+.0f}" if cf != 0 else "CF: 0"
                    self.stdout.write(
                        f"  ID {user.id:4d} | "
                        f"{(user.get_full_name() or user.username):30s} | "
                        f"Quota: {balance.total_paid_leaves:5.1f} | "
                        f"Used: {balance.paid_leaves_taken:5.1f} | "
                        f"Remaining: {balance.remaining_paid_leaves:5.1f} | "
                        f"Unpaid: {balance.unpaid_leaves:4.1f} | "
                        f"{cf_str}"
                    )
                    total_ok += 1
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  ID {user.id:4d} | "
                            f"{(user.get_full_name() or user.username):30s} | "
                            f"ERROR: {e}"
                        )
                    )
                    total_failed += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}"
                f"\nDone! Success: {total_ok} | Failed: {total_failed}"
                f"\n{'='*60}"
                f"\nNOTE: carry_forward_adjustment is preserved from DB."
                f"\nTo set carry-forward penalties, use the Render PostgreSQL shell:"
                f"\n  UPDATE leave_employeeleavebalance"
                f"\n  SET carry_forward_adjustment = -4,"
                f"\n      total_paid_leaves = 24 + (-4),"
                f"\n      remaining_paid_leaves = GREATEST(24 + (-4) - paid_leaves_taken, 0)"
                f"\n  WHERE employee_id = 14"
                f"\n  AND leave_year_start = '2026-04-01';"
                f"\n  Then run: python manage.py recalculate_leave_balances"
            )
        )