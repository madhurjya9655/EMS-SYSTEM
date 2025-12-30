from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time, date

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.urls import reverse
from django.utils import timezone
from django.db.models import Q

from apps.tasks.models import Checklist, Delegation
from apps.tasks.recurrence import (
    normalize_mode,
    RECURRING_MODES,
    get_next_planned_date,     # ALWAYS 19:00 IST on next working day (Sun/holiday → shift)
)
from apps.tasks.utils import send_checklist_assignment_to_user, send_delegation_assignment_to_user

from apps.settings.models import Holiday
from apps.leave.models import LeaveRequest

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)


def _safe_console_text(s: object) -> str:
    try:
        return ("" if s is None else str(s)).encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        try:
            return repr(s)
        except Exception:
            return ""


def _to_ist(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt.astimezone(IST)


def _after_10am_today(now_ist: datetime | None = None) -> bool:
    now_ist = (now_ist or timezone.now()).astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return now_ist >= anchor


def _is_holiday(d: date) -> bool:
    return d.weekday() == 6 or Holiday.objects.filter(date=d).exists()


def _is_user_on_leave(user_id: int, d: date) -> bool:
    try:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        user = User.objects.filter(id=user_id).first()
        return bool(user and LeaveRequest.is_user_blocked_on(user, d))
    except Exception:
        return False


def _push_to_next_allowed_date(user_id: int, d: date) -> date:
    """Advance date until it's not a Sunday/holiday and not within user's leave window."""
    for _ in range(0, 120):
        if (not _is_holiday(d)) and (not _is_user_on_leave(user_id, d)):
            return d
        d += timedelta(days=1)
    return d


def _series_q(assign_to_id: int, task_name: str, mode: str, frequency: int | None, group_name: str | None):
    """Legacy-tolerant grouping: treat NULL frequency as 1."""
    freq = max(int(frequency or 1), 1)
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq, None])
    return q, freq


def _send_checklist_recur_email(obj: Checklist) -> None:
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return
    if SEND_RECUR_EMAILS_ONLY_AT_10AM and not _after_10am_today():
        logger.info(_safe_console_text(f"Skip recur email for CL-{obj.id}: before 10:00 IST"))
        return

    try:
        planned_ist = obj.planned_date.astimezone(IST) if obj.planned_date else None
        pretty_time = planned_ist.strftime("%H:%M") if planned_ist else "19:00"

        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        subject = f"Today’s Checklist – {obj.task_name} (due {pretty_time})"

        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=subject,
        )
        logger.info(_safe_console_text(f"Sent checklist reminder for CL-{obj.id} to user_id={obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Failed to send recurring reminder for CL-{obj.id}: {e}"))


def _send_delegation_10am_emails(today_ist: date, user_id: int | None, dry_run: bool) -> int:
    """Delegations are one-time; send day-of reminder after 10:00 IST."""
    if not _after_10am_today():
        return 0

    start_today_ist = IST.localize(datetime.combine(today_ist, dt_time.min))
    end_today_ist = IST.localize(datetime.combine(today_ist, dt_time.max))
    start_proj = start_today_ist.astimezone(timezone.get_current_timezone())
    end_proj = end_today_ist.astimezone(timezone.get_current_timezone())

    qs = Delegation.objects.filter(
        status="Pending",
        planned_date__gte=start_proj,
        planned_date__lte=end_proj,
    ).select_related("assign_to")

    if user_id:
        qs = qs.filter(assign_to_id=user_id)

    sent = 0
    for obj in qs:
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
        "Recurring Checklist generator & 10:00 reminder sender.\n"
        "STRICT RULES:\n"
        "• Next occurrence spawns ONLY after completion (stepping base = latest COMPLETED).\n"
        "• If ANY Pending exists in the series, DO NOT generate.\n"
        "• Next occurs on a working day at 19:00 IST (Sun/holiday → shift), also shifted off leave.\n"
        "• Emails for 'today' items go AFTER 10:00 IST to assignee only.\n"
        "• Delegations are one-time; send 10:00 IST day-of reminders only."
    )

    def add_arguments(self, parser):
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id).")
        parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to DB.")
        parser.add_argument("--no-email", action="store_true", help="Skip sending the 10:00 reminders.")

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
        per_user_emailed: dict[int, int] = {}

        # Series seeds
        filters = {"mode__in": RECURRING_MODES}
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
            q_series, freq_norm = _series_q(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=normalize_mode(s["mode"]),
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

            next_dt = get_next_planned_date(base.planned_date, s["mode"], freq_norm)
            if not next_dt:
                continue

            # shift off holiday/leave
            next_date = next_dt.astimezone(IST).date()
            safe_date = _push_to_next_allowed_date(s["assign_to_id"], next_date)
            if safe_date != next_date:
                next_dt = IST.localize(datetime.combine(safe_date, dt_time(19, 0))).astimezone(
                    timezone.get_current_timezone()
                )

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
                    f"at {next_dt.astimezone(IST):%Y-%m-%d %H:%M IST}"
                ))
            else:
                try:
                    with transaction.atomic():
                        obj = Checklist.objects.create(
                            assign_by=base.assign_by,
                            task_name=base.task_name,
                            message=base.message,
                            assign_to=base.assign_to,
                            planned_date=next_dt,
                            priority=base.priority,
                            attachment_mandatory=base.attachment_mandatory,
                            mode=base.mode,
                            frequency=freq_norm,
                            time_per_task_minutes=base.time_per_task_minutes,
                            remind_before_days=base.remind_before_days,
                            assign_pc=base.assign_pc,
                            notify_to=base.notify_to,
                            auditor=getattr(base, "auditor", None),
                            set_reminder=base.set_reminder,
                            reminder_mode=base.reminder_mode,
                            reminder_frequency=base.reminder_frequency,
                            reminder_starting_time=base.reminder_starting_time,
                            checklist_auto_close=base.checklist_auto_close,
                            checklist_auto_close_days=base.checklist_auto_close_days,
                            group_name=getattr(base, "group_name", None),
                            actual_duration_minutes=0,
                            status="Pending",
                        )
                    created_total += 1
                    per_user_created[s["assign_to_id"]] = per_user_created.get(s["assign_to_id"], 0) + 1
                    logger.info(_safe_console_text(
                        f"✅ Created next CL-{obj.id} '{obj.task_name}' for user_id={s['assign_to_id']} "
                        f"at {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
                    ))
                except Exception as e:
                    logger.exception("Failed creating next occurrence for %s: %s", s, e)

        # 2) Send reminders for TODAY (after 10:00 IST)
        if send_emails and SEND_EMAILS_FOR_AUTO_RECUR and (not SEND_RECUR_EMAILS_ONLY_AT_10AM or _after_10am_today()):
            start_today_ist = IST.localize(datetime.combine(today_ist, dt_time.min))
            end_today_ist = IST.localize(datetime.combine(today_ist, dt_time.max))
            start_proj = start_today_ist.astimezone(timezone.get_current_timezone())
            end_proj = end_today_ist.astimezone(timezone.get_current_timezone())

            email_qs = Checklist.objects.filter(
                status="Pending",
                planned_date__gte=start_proj,
                planned_date__lte=end_proj,
                mode__in=RECURRING_MODES,
            )
            if user_id:
                email_qs = email_qs.filter(assign_to_id=user_id)

            for obj in email_qs.select_related("assign_to"):
                if not dry_run:
                    _send_checklist_recur_email(obj)
                email_total += 1
                per_user_emailed[obj.assign_to_id] = per_user_emailed.get(obj.assign_to_id, 0) + 1

            # Delegations day-of
            email_total += _send_delegation_10am_emails(today_ist, user_id, dry_run)

        # Summaries
        if per_user_created:
            for uid, count in per_user_created.items():
                logger.info(_safe_console_text(f"[RECUR GEN] user_id={uid} → created {count} occurrence(s)"))
        else:
            logger.info(_safe_console_text(f"[RECUR GEN] No new occurrences were needed today."))

        if per_user_emailed or email_total:
            for uid, count in per_user_emailed.items():
                logger.info(_safe_console_text(f"[RECUR MAIL] user_id={uid} → sent {count} checklist reminder(s)"))
        else:
            if send_emails:
                logger.info(_safe_console_text(f"[RECUR MAIL] No reminders sent (before 10:00 IST or none due)."))

        parts = [f"Created {created_total} checklist occurrence(s)"]
        if send_emails:
            parts.append(f"Emailed {email_total} reminder(s) (checklists + delegations)")
        if dry_run:
            msg = "[DRY RUN] " + ", ".join(parts)
            self.stdout.write(self.style.WARNING(msg))
        else:
            self.stdout.write(self.style.SUCCESS(", ".join(parts)))
