from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time
from typing import Optional

import pytz
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.db.models import Q

from apps.tasks.models import Checklist, Delegation
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]
EVENING_HOUR = 19
EVENING_MINUTE = 0


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
      • Step the DATE by Daily/Weekly/Monthly/Yearly with freq (>=1).
      • Shift to the **next working day** (Mon–Sat, not a Holiday).
      • Pin the TIME to **19:00 IST** (7 PM).
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

    next_date = _next_working_day(cur_ist.date())
    next_ist = IST.localize(datetime.combine(next_date, dt_time(EVENING_HOUR, EVENING_MINUTE)))
    return next_ist.astimezone(timezone.get_current_timezone())


def _series_q(assign_to_id: int, task_name: str, mode: str, frequency: int | None, group_name: str | None):
    """Legacy-tolerant grouping: treat NULL frequency as 1."""
    try:
        freq = max(int(frequency or 1), 1)
    except Exception:
        freq = 1
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq, None])
    return q, freq


class Command(BaseCommand):
    help = (
        "Roll due recurring CHECKLIST tasks only.\n"
        "STRICT: create next only when stepping base is the latest COMPLETED and there is NO Pending in series.\n"
        "Next planned is 19:00 IST on next working day (Sun/holiday skipped). Delegations are one-time."
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
            self.stdout.write(
                self.style.SUCCESS(
                    f"Rolled due recurrences — Checklist created: {count_chk}, Delegation created: 0 (one-time by design)"
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
        Create next ONLY when:
          • No Pending exists in the tolerant series
          • There is a COMPLETED base
          • The next due time (19:00 IST working day) is <= now
        """
        user_id = opts.get("user_id")
        now = timezone.now()
        now_ist = now.astimezone(IST)

        filters = {"mode__in": RECURRING_MODES}
        if user_id:
            filters["assign_to_id"] = user_id

        series = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )
        created_count = 0

        for s in series:
            q_series, freq_norm = _series_q(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            # Skip if any Pending exists
            if Checklist.objects.filter(status="Pending").filter(q_series).exists():
                continue

            base = (
                Checklist.objects.filter(status="Completed")
                .filter(q_series)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not base or not base.planned_date:
                continue

            next_dt = get_next_planned_datetime(base.planned_date, s["mode"], freq_norm)
            if not next_dt:
                continue

            # Only if due now (<= IST now)
            if next_dt.astimezone(IST) > now_ist:
                continue

            # No dupe pending in ±1 minute
            exists = Checklist.objects.filter(
                status="Pending",
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency__in=[freq_norm, None],
                group_name=s["group_name"],
                planned_date__gte=next_dt - timedelta(minutes=1),
                planned_date__lt=next_dt + timedelta(minutes=1),
            ).exists()
            if exists:
                continue

            if dry_run:
                created_count += 1
                self.stdout.write(f"[DRY RUN] Would create Checklist: '{s['task_name']}' at {next_dt}")
                continue

            try:
                with transaction.atomic():
                    kwargs = dict(
                        assign_by=getattr(base, "assign_by", None),
                        task_name=base.task_name,
                        assign_to=base.assign_to,
                        planned_date=next_dt,
                        priority=getattr(base, "priority", None),
                        attachment_mandatory=getattr(base, "attachment_mandatory", False),
                        mode=base.mode,
                        frequency=freq_norm,
                        status="Pending",
                    )
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
                        if hasattr(base, opt):
                            kwargs[opt] = getattr(base, opt)
                    Checklist.objects.create(**kwargs)
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"Created Checklist: '{base.task_name}' at {next_dt}"))
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
        # prefer completed_at if exists; else planned_date
        if any(getattr(f, "name", None) == "completed_at" for f in model._meta.get_fields()):
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

        f = {}
        if user_id:
            f["assign_to_id"] = user_id

        invalid = Checklist.objects.filter(mode__isnull=False).exclude(mode__in=RECURRING_MODES)
        for obj in invalid:
            issues.append(f"Checklist {obj.id}: invalid mode '{obj.mode}'")

        # Delegations must be one-time; flag any with recurring fields
        bad_delegations = Delegation.objects.exclude(mode__isnull=True).exclude(mode__exact="")
        for d in bad_delegations:
            issues.append(f"Delegation {d.id}: has recurring fields but delegations are one-time only")

        return issues
