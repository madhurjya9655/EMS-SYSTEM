# apps/tasks/management/commands/roll_recurrences.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time
from typing import Optional

import pytz
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]
EVENING_HOUR = 19
EVENING_MINUTE = 0


# ------------------------------ helpers ------------------------------ #
def _is_working_day(d) -> bool:
    try:
        # Sunday = 6
        return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()
    except Exception:
        return d.weekday() != 6


def _next_working_day(d):
    cur = d
    for _ in range(0, 90):
        if _is_working_day(cur):
            return cur
        cur = cur + timedelta(days=1)
    return cur


def _aware_in_project_tz(dt: datetime) -> datetime:
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, tz)
    return dt.astimezone(tz)


def get_next_planned_datetime(prev_dt: datetime, mode: str, frequency: int | None) -> Optional[datetime]:
    """
    FINAL RULE for this command:
      • Step the DATE by Daily/Weekly/Monthly/Yearly with freq (1..10).
      • Shift to the **next working day** (Mon–Sat, not a Holiday).
      • Pin the TIME to **19:00 IST** (7 PM).
      • Return as aware datetime in the project timezone.

    This mirrors the “skip Sundays/holidays” requirement so 10:00 AM dashboard/email
    naturally aligns to the next working day’s 7 PM plan.
    """
    if (mode or "") not in RECURRING_MODES:
        return None

    try:
        step = int(frequency or 1)
    except Exception:
        step = 1
    step = max(1, min(step, 10))

    base = _aware_in_project_tz(prev_dt)
    cur_ist = base.astimezone(IST)

    if mode == "Daily":
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == "Weekly":
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == "Yearly":
        cur_ist = cur_ist + relativedelta(years=step)

    # Shift date to next working day, then pin to 19:00 IST
    next_date = _next_working_day(cur_ist.date())
    next_ist = IST.localize(datetime.combine(next_date, dt_time(EVENING_HOUR, EVENING_MINUTE)))
    return next_ist.astimezone(timezone.get_current_timezone())


def _has_field(model, field_name: str) -> bool:
    return any(getattr(f, "name", None) == field_name for f in model._meta.get_fields())


# ------------------------------ command ------------------------------ #
class Command(BaseCommand):
    help = (
        "Roll due recurring CHECKLIST tasks only (no pre-creation).\n"
        "• Creates the next occurrence ONLY when it is due now (<= current IST time).\n"
        "• Next occurrence is scheduled at 19:00 IST on the next working day (Sun/holidays skipped).\n"
        "• Delegations are treated as one-time by policy and are not rolled."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--action",
            choices=["roll", "cleanup", "validate", "all"],
            default="all",
            help="Which action to perform.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without making changes.",
        )
        parser.add_argument(
            "--days-ahead",
            type=int,
            default=0,
            help="(Ignored) Previously used to pre-create future items. Now rolling only creates items due by NOW.",
        )
        parser.add_argument(
            "--cleanup-completed-days",
            type=int,
            default=90,
            help="Delete completed items older than this many days (for cleanup).",
        )
        parser.add_argument(
            "--user-id",
            type=int,
            help="Limit to a specific assignee (user id) for roll/validate.",
        )

    def handle(self, *args, **opts):
        action = opts["action"]
        dry_run = opts.get("dry_run", False)

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no data will be modified.\n"))

        if action in ("roll", "all"):
            count_chk = self._roll_due_checklists(opts, dry_run)
            # Delegations are one-time by design; we explicitly skip them.
            self.stdout.write(
                self.style.SUCCESS(
                    f"Rolled due recurrences — Checklist created: {count_chk}, Delegation created: 0 (skipped by design)"
                )
            )

        if action in ("cleanup", "all"):
            deleted_chk, deleted_dlg = self._cleanup(opts, dry_run)
            if dry_run:
                self.stdout.write(
                    self.style.WARNING(
                        f"[DRY RUN] Would delete — Checklist: {deleted_chk}, Delegation: {deleted_dlg}"
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Deleted — Checklist: {deleted_chk}, Delegation: {deleted_dlg}"
                    )
                )

        if action in ("validate", "all"):
            issues = self._validate(opts)
            if issues:
                self.stdout.write(self.style.WARNING(f"Validation issues ({len(issues)}):"))
                for msg in issues:
                    self.stdout.write(f"  - {msg}")
            else:
                self.stdout.write(self.style.SUCCESS("All recurrence configurations look good."))

    # ------------------------------- ROLL (DUE ONLY) ------------------------------- #
    def _roll_due_checklists(self, opts, dry_run: bool) -> int:
        """
        Create the next occurrence only when it is due (<= now in IST).
        Uses get_next_planned_datetime() here which enforces:
          - 19:00 IST
          - shift Sun/holidays to the next working day
        """
        user_id = opts.get("user_id")
        now_ist = timezone.now().astimezone(IST)

        filters = {"mode__in": RECURRING_MODES, "frequency__gte": 1}
        if user_id:
            filters["assign_to_id"] = user_id

        series = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )
        created_count = 0

        for s in series:
            latest = (
                Checklist.objects
                .filter(**s)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not latest:
                continue

            # Compute the next scheduled occurrence using the canonical rules above
            next_dt = get_next_planned_datetime(latest.planned_date, latest.mode, latest.frequency)
            if not next_dt:
                continue

            # Only create if it's due now (<= current IST time)
            if next_dt.astimezone(IST) > now_ist:
                continue

            # Do not create if any pending of the same series already exists at that timestamp (±1 min)
            exists = Checklist.objects.filter(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
                status="Pending",
            ).exists()
            if exists:
                continue

            if dry_run:
                created_count += 1
                self.stdout.write(
                    f"[DRY RUN] Would create Checklist: '{latest.task_name}' at {next_dt}"
                )
                continue

            try:
                with transaction.atomic():
                    kwargs = dict(
                        assign_by=getattr(latest, "assign_by", None),
                        task_name=latest.task_name,
                        assign_to=latest.assign_to,
                        planned_date=next_dt,  # computed at 19:00 IST on next working day
                        priority=getattr(latest, "priority", None),
                        attachment_mandatory=getattr(latest, "attachment_mandatory", False),
                        mode=latest.mode,
                        frequency=latest.frequency,
                        status="Pending",
                    )
                    # Mirror optional fields when present
                    for opt in (
                        "message",
                        "time_per_task_minutes",
                        "remind_before_days",
                        "assign_pc",
                        "notify_to",
                        "auditor",
                        "set_reminder",
                        "reminder_mode",
                        "reminder_frequency",
                        "reminder_starting_time",
                        "checklist_auto_close",
                        "checklist_auto_close_days",
                        "group_name",
                        "actual_duration_minutes",
                    ):
                        if hasattr(latest, opt):
                            kwargs[opt] = getattr(latest, opt)

                    Checklist.objects.create(**kwargs)
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f"Created Checklist: '{latest.task_name}' at {next_dt}")
                )
            except Exception as e:
                logger.error("Failed to create Checklist recurrence: %s", e)

        return created_count

    # ------------------------------ CLEANUP ------------------------------ #
    def _cleanup(self, opts, dry_run: bool):
        days = opts["cleanup_completed_days"]
        cutoff = timezone.now() - timedelta(days=days)

        deleted_chk = self._cleanup_model(Checklist, cutoff, dry_run)
        deleted_dlg = self._cleanup_model(Delegation, cutoff, dry_run)
        return deleted_chk, deleted_dlg

    def _cleanup_model(self, model, cutoff, dry_run: bool) -> int:
        if _has_field(model, "completed_at"):
            qs = model.objects.filter(status="Completed", completed_at__lt=cutoff)
        else:
            qs = model.objects.filter(status="Completed", planned_date__lt=cutoff)

        count = qs.count()
        if dry_run or count == 0:
            return count

        try:
            with transaction.atomic():
                deleted, _ = qs.delete()
            return deleted
        except Exception as e:
            logger.error("Cleanup failed for %s: %s", model.__name__, e)
            return 0

    # ------------------------------ VALIDATE ----------------------------- #
    def _validate(self, opts):
        user_id = opts.get("user_id")
        issues = []

        def _series_issues(model, name):
            f = {}
            if user_id:
                f["assign_to_id"] = user_id

            invalid = model.objects.filter(mode__isnull=False).exclude(mode__in=RECURRING_MODES)
            for obj in invalid:
                issues.append(f"{name} {obj.id}: invalid mode '{obj.mode}'")

            missing_freq = model.objects.filter(mode__in=RECURRING_MODES, frequency__isnull=True, **f)
            for obj in missing_freq:
                issues.append(f"{name} {obj.id}: missing frequency for mode '{obj.mode}'")

        _series_issues(Checklist, "Checklist")

        # Delegations should be one-time; flag any with a mode set
        bad_delegations = Delegation.objects.exclude(mode__isnull=True).exclude(mode__exact="")
        for d in bad_delegations:
            issues.append("Delegation {}: has recurring fields but delegations are one-time only".format(d.id))

        return issues
