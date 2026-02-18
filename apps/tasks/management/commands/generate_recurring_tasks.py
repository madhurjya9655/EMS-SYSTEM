# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\generate_recurring_tasks.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone

from apps.settings.models import Holiday
from apps.tasks.models import Checklist, Delegation

# ✅ FINAL recurrence math for Checklist generation (pins 19:00 IST; no holiday shift inside)
from apps.tasks.recurrence_utils import (
    normalize_mode,
    RECURRING_MODES,
    get_next_planned_date,
)

from apps.tasks.utils import send_delegation_assignment_to_user
from apps.tasks.services.blocking import guard_assign  # ✅ day-of email guard

# ✅ unified leave blocking source of truth (time-aware)
from apps.tasks.utils.blocking import is_user_blocked_at

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)

ASSIGN_ANCHOR_T = dt_time(10, 0)
DUE_T = dt_time(19, 0)


def _safe_console_text(s: object) -> str:
    try:
        return ("" if s is None else str(s)).encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        try:
            return repr(s)
        except Exception:
            return ""


def _to_ist(dt: datetime) -> datetime:
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, tz)
    return dt.astimezone(IST)


def _after_10am_today(now_ist: datetime | None = None) -> bool:
    now_ist = (now_ist or timezone.now()).astimezone(IST)
    anchor = now_ist.replace(hour=ASSIGN_ANCHOR_T.hour, minute=ASSIGN_ANCHOR_T.minute, second=0, microsecond=0)
    return now_ist >= anchor


def _is_holiday_or_sunday(d: date) -> bool:
    try:
        if d.weekday() == 6:
            return True
    except Exception:
        pass
    try:
        return Holiday.objects.filter(date=d).exists()
    except Exception:
        return False


def _get_user(user_id: int):
    from django.contrib.auth import get_user_model
    User = get_user_model()
    return User.objects.filter(id=user_id, is_active=True).first()


def _is_user_blocked_on_date_at_10am(user_id: int, d: date) -> bool:
    user = _get_user(user_id)
    if not user:
        return False
    anchor_ist = IST.localize(datetime.combine(d, ASSIGN_ANCHOR_T))
    return bool(is_user_blocked_at(user, anchor_ist))


def _push_to_next_allowed_date(user_id: int, d: date) -> date:
    """Advance date until it's not a Sunday/holiday and not blocked by leave at 10:00 IST."""
    cur = d
    for _ in range(0, 120):
        if (not _is_holiday_or_sunday(cur)) and (not _is_user_blocked_on_date_at_10am(user_id, cur)):
            return cur
        cur += timedelta(days=1)
    return cur


def _series_q(assign_to_id: int, task_name: str, mode: str, frequency: int | None, group_name: str | None):
    """Legacy-tolerant grouping: treat NULL frequency as 1. Exclude tombstoned rows."""
    freq = max(int(frequency or 1), 1)
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode, is_skipped_due_to_leave=False)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq, None])
    return q, freq


def _is_self_assigned(obj) -> bool:
    try:
        return bool(obj.assign_by_id and obj.assign_to_id and obj.assign_by_id == obj.assign_to_id)
    except Exception:
        return False


def _send_delegation_10am_emails(today_ist: date, user_id: int | None, dry_run: bool) -> int:
    """
    Delegations: day-of reminder after 10:00 IST with leave/self-assign guards.

    NOTE: Checklist day-of emails are handled by consolidated 10:00 IST digest
          (apps/tasks/tasks.py::send_due_today_assignments). This command does NOT email checklists.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return 0
    if SEND_RECUR_EMAILS_ONLY_AT_10AM and not _after_10am_today():
        return 0

    start_today_ist = IST.localize(datetime.combine(today_ist, dt_time.min))
    end_today_ist = IST.localize(datetime.combine(today_ist, dt_time.max))
    start_proj = start_today_ist.astimezone(timezone.get_current_timezone())
    end_proj = end_today_ist.astimezone(timezone.get_current_timezone())

    qs = Delegation.objects.filter(
        status="Pending",
        planned_date__gte=start_proj,
        planned_date__lte=end_proj,
        is_skipped_due_to_leave=False,
    ).select_related("assign_to", "assign_by")

    if user_id:
        qs = qs.filter(assign_to_id=user_id)

    sent = 0
    for obj in qs:
        if _is_self_assigned(obj):
            logger.info(_safe_console_text(f"Skip delegation 10AM email for DL-{obj.id}: self-assigned"))
            continue

        # leave guard at 10:00 IST
        try:
            p_ist = _to_ist(obj.planned_date)
            anchor_ist = p_ist.replace(hour=ASSIGN_ANCHOR_T.hour, minute=ASSIGN_ANCHOR_T.minute, second=0, microsecond=0)
            if not guard_assign(obj.assign_to, anchor_ist):
                logger.info(_safe_console_text(f"Skip delegation 10AM email for DL-{obj.id}: assignee blocked (leave @ 10:00 IST)"))
                continue
        except Exception:
            # fail-safe: if we can't evaluate, do not send
            continue

        try:
            subject = f"Today’s Delegation – {obj.task_name} (due 7 PM)"
            complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[obj.id])}"
            if not dry_run:
                send_delegation_assignment_to_user(
                    delegation=obj,
                    complete_url=complete_url,
                    subject_prefix=subject,
                )
            sent += 1
        except Exception as e:
            logger.error("Failed to send delegation 10AM email for DL-%s: %s", getattr(obj, "id", "?"), e)

    if sent:
        logger.info(_safe_console_text(f"Sent {sent} Delegation reminders for {today_ist}"))
    return sent


class Command(BaseCommand):
    help = (
        "Recurring Checklist generator (completion-gated) + Delegation day-of reminders.\n"
        "STRICT RULES:\n"
        "• Next checklist occurrence spawns ONLY after completion (stepping base = latest COMPLETED).\n"
        "• If ANY Pending exists in the series, DO NOT generate.\n"
        "• Next checklist planned datetime = 19:00 IST pinned by recurrence_utils, then this command shifts\n"
        "  off holiday/Sunday and off leave (leave check at 10:00 IST).\n"
        "• CHECKLIST emails are NOT sent here when using consolidated 10:00 IST digest.\n"
        "• Delegations are one-time; send 10:00 IST day-of reminders only (with leave/self-assign guards)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id).")
        parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to DB.")
        parser.add_argument("--no-email", action="store_true", help="Skip sending the 10:00 reminders for delegations.")

    def handle(self, *args, **opts):
        user_id = opts.get("user_id")
        dry_run = bool(opts.get("dry_run", False))
        send_emails = not bool(opts.get("no_email", False))

        now = timezone.now()
        now_ist = now.astimezone(IST)
        today_ist = now_ist.date()

        created_total = 0
        email_total = 0
        per_user_created: dict[int, int] = {}

        # Series seeds (exclude tombstoned)
        filters = {"mode__in": RECURRING_MODES, "is_skipped_due_to_leave": False}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        logger.info(_safe_console_text(f"[RECUR] Starting @ {now_ist:%Y-%m-%d %H:%M IST} | seeds={seeds.count()}"))

        # 1) Generate next ONLY if no Pending and there is a Completed base
        for s in seeds:
            mode_norm = normalize_mode(s["mode"])
            if mode_norm not in RECURRING_MODES:
                continue

            q_series, freq_norm = _series_q(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=mode_norm,
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            # Skip if any pending exists
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

            next_dt = get_next_planned_date(base.planned_date, mode_norm, freq_norm)
            if not next_dt:
                continue

            # Shift off holiday/leave (10AM anchor)
            next_date = _to_ist(next_dt).date()
            safe_date = _push_to_next_allowed_date(s["assign_to_id"], next_date)
            if safe_date != next_date:
                next_dt = IST.localize(datetime.combine(safe_date, DUE_T)).astimezone(
                    timezone.get_current_timezone()
                )

            # Prevent recreating "today" here (avoids resurrecting deleted today tasks).
            try:
                if _to_ist(next_dt).date() == today_ist:
                    continue
            except Exception:
                continue

            # dupe guard within tolerant series
            dupe = (
                Checklist.objects.filter(status="Pending")
                .filter(q_series)
                .filter(
                    planned_date__gte=next_dt - timedelta(minutes=1),
                    planned_date__lt=next_dt + timedelta(minutes=1),
                )
                .exists()
            )
            if dupe:
                continue

            if dry_run:
                created_total += 1
                per_user_created[s["assign_to_id"]] = per_user_created.get(s["assign_to_id"], 0) + 1
                logger.info(_safe_console_text(
                    f"[DRY RUN] Would create '{s['task_name']}' for user_id={s['assign_to_id']} "
                    f"at {_to_ist(next_dt):%Y-%m-%d %H:%M IST}"
                ))
            else:
                try:
                    with transaction.atomic():
                        obj = Checklist.objects.create(
                            assign_by=base.assign_by,
                            task_name=base.task_name,
                            message=getattr(base, "message", "") or "",
                            assign_to=base.assign_to,
                            planned_date=next_dt,
                            priority=getattr(base, "priority", None),
                            attachment_mandatory=getattr(base, "attachment_mandatory", False),
                            mode=base.mode,
                            frequency=freq_norm,
                            time_per_task_minutes=getattr(base, "time_per_task_minutes", 0) or 0,
                            remind_before_days=getattr(base, "remind_before_days", 0) or 0,
                            assign_pc=getattr(base, "assign_pc", None),
                            notify_to=getattr(base, "notify_to", None),
                            auditor=getattr(base, "auditor", None),
                            set_reminder=getattr(base, "set_reminder", False),
                            reminder_mode=getattr(base, "reminder_mode", None),
                            reminder_frequency=getattr(base, "reminder_frequency", None),
                            reminder_starting_time=getattr(base, "reminder_starting_time", None),
                            checklist_auto_close=getattr(base, "checklist_auto_close", False),
                            checklist_auto_close_days=getattr(base, "checklist_auto_close_days", 0) or 0,
                            group_name=getattr(base, "group_name", None),
                            actual_duration_minutes=0,
                            status="Pending",
                            is_skipped_due_to_leave=False,
                        )
                    created_total += 1
                    per_user_created[s["assign_to_id"]] = per_user_created.get(s["assign_to_id"], 0) + 1
                    logger.info(_safe_console_text(
                        f"✅ Created next CL-{obj.id} '{obj.task_name}' for user_id={s['assign_to_id']} "
                        f"at {_to_ist(obj.planned_date):%Y-%m-%d %H:%M IST}"
                    ))
                except Exception as e:
                    logger.exception("Failed creating next occurrence for %s: %s", s, e)

        # 2) Send day-of reminders (Delegations only)
        if send_emails:
            email_total += _send_delegation_10am_emails(today_ist, user_id, dry_run)

        if per_user_created:
            for uid, count in per_user_created.items():
                logger.info(_safe_console_text(f"[RECUR GEN] user_id={uid} → created {count} occurrence(s)"))
        else:
            logger.info(_safe_console_text("[RECUR GEN] No new occurrences were needed today."))

        parts = [f"Created {created_total} checklist occurrence(s)"]
        if send_emails:
            parts.append(f"Emailed {email_total} delegation reminder(s)")
        if dry_run:
            self.stdout.write(self.style.WARNING("[DRY RUN] " + ", ".join(parts)))
        else:
            self.stdout.write(self.style.SUCCESS(", ".join(parts)))
