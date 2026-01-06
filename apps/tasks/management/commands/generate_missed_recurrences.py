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

from apps.tasks.models import Checklist
from apps.tasks.recurrence import (
    get_next_planned_date,   # final rule: 19:00 IST on working days
    RECURRING_MODES,
)
from apps.tasks.utils import send_checklist_assignment_to_user
from apps.tasks.services.blocking import guard_assign  # ✅ leave-aware email guard

# Leave/holiday checks
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
    for _ in range(0, 120):
        if (not _is_holiday(d)) and (not _is_user_on_leave(user_id, d)):
            return d
        d += timedelta(days=1)
    return d


def _series_q(assign_to_id: int, task_name: str, mode: str, frequency: int | None, group_name: str | None):
    """Treat NULL frequency as 1 for legacy tolerance."""
    freq = max(int(frequency or 1), 1)
    q = Q(assign_to_id=assign_to_id, task_name=task_name, mode=mode)
    if group_name:
        q &= Q(group_name=group_name)
    q &= Q(frequency__in=[freq, None])
    return q, freq


class Command(BaseCommand):
    help = (
        "Backfill missed recurrences WITHOUT violating the rule: next spawns ONLY after completion.\n"
        "For each series, if there is NO Pending item and there IS a Completed item, create the next at 19:00 IST\n"
        "(Sun/holiday shifted; also shifted off assignee leave). Emails go only after 10:00 AM IST on the planned day."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Show actions without writing to DB")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id)")
        parser.add_argument("--no-email", action="store_true", help="Skip email notifications")

    def handle(self, *args, **opts):
        dry_run = bool(opts.get("dry_run", False))
        user_id = opts.get("user_id")
        send_emails = not bool(opts.get("no_email", False))

        now = timezone.now()
        now_ist = now.astimezone(IST)
        today_ist = now_ist.date()

        filters = {"mode__in": RECURRING_MODES}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        created = 0
        processed = 0

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no tasks will be created."))

        for s in seeds:
            processed += 1
            q_series, freq_norm = _series_q(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
            )

            # Golden rules:
            # 1) If ANY Pending exists → do not create future
            if Checklist.objects.filter(status="Pending").filter(q_series).exists():
                continue

            # 2) Step only from the latest COMPLETED
            base = (
                Checklist.objects.filter(status="Completed")
                .filter(q_series)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not base or not base.planned_date:
                continue

            next_planned = get_next_planned_date(base.planned_date, s["mode"], freq_norm)
            if not next_planned:
                continue

            # shift away from holiday/leave
            next_date_ist = next_planned.astimezone(IST).date()
            safe_date = _push_to_next_allowed_date(s["assign_to_id"], next_date_ist)
            if safe_date != next_date_ist:
                next_planned = IST.localize(datetime.combine(safe_date, dt_time(19, 0))).astimezone(
                    timezone.get_current_timezone()
                )

            # Dupe guard within ±1 minute inside tolerant series
            dupe = (
                Checklist.objects.filter(status="Pending")
                .filter(q_series)
                .filter(
                    planned_date__gte=next_planned - timedelta(minutes=1),
                    planned_date__lt=next_planned + timedelta(minutes=1),
                )
                .exists()
            )
            if dupe:
                continue

            if dry_run:
                created += 1
                self.stdout.write(
                    f"[DRY RUN] Would create: {s['task_name']} → {next_planned.astimezone(IST):%Y-%m-%d %H:%M IST}"
                )
                continue

            try:
                with transaction.atomic():
                    obj = Checklist.objects.create(
                        assign_by=base.assign_by,
                        task_name=base.task_name,
                        message=base.message,
                        assign_to=base.assign_to,
                        planned_date=next_planned,
                        priority=base.priority,
                        attachment_mandatory=base.attachment_mandatory,
                        mode=base.mode,
                        frequency=freq_norm,  # normalize going forward
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

                created += 1
                self.stdout.write(self.style.SUCCESS(
                    f"✅ Created: CL-{obj.id} '{obj.task_name}' @ {obj.planned_date.astimezone(IST):%Y-%m-%d %H:%M IST}"
                ))

                # Email policy — assignee only, only after 10:00 IST if planned today
                if send_emails and SEND_EMAILS_FOR_AUTO_RECUR:
                    planned_ist = obj.planned_date.astimezone(IST)
                    if planned_ist.date() == today_ist and (not SEND_RECUR_EMAILS_ONLY_AT_10AM or _after_10am_today()):
                        # ⛔ self-assign guard + leave guard at 10:00 IST
                        if getattr(obj, "assign_by_id", None) and obj.assign_by_id == obj.assign_to_id:
                            logger.info(_safe_console_text(f"Skip email for CL-{obj.id}: self-assigned"))
                        else:
                            anchor_ist = planned_ist.replace(hour=10, minute=0, second=0, microsecond=0)
                            if not guard_assign(obj.assign_to, anchor_ist):
                                logger.info(_safe_console_text(f"Skip email for CL-{obj.id}: assignee blocked (leave @ 10:00 IST)"))
                            else:
                                try:
                                    complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
                                    send_checklist_assignment_to_user(
                                        task=obj,
                                        complete_url=complete_url,
                                        subject_prefix=f"Today’s Checklist – {obj.task_name}",
                                    )
                                    logger.info(_safe_console_text(
                                        f"Sent recur email for CL-{obj.id} to user_id={obj.assign_to_id}"
                                    ))
                                except Exception as e:
                                    logger.exception("Email failure for recurring checklist %s: %s", obj.id, e)

            except Exception as e:
                logger.exception("Failed to create recurrence for %s: %s", s, e)
                self.stdout.write(self.style.ERROR(f"❌ Failed: {s['task_name']} - {e}"))

        # Summary
        if dry_run:
            self.stdout.write(self.style.WARNING(f"[DRY RUN] Would create {created} task(s) from {processed} series"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Created {created} task(s) from {processed} series"))
        if created == 0:
            self.stdout.write("No missed recurrences needed to be created.")
