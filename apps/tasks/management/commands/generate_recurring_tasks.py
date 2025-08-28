# apps/tasks/management/commands/generate_recurring_tasks.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date, time as dt_time

import pytz
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from apps.tasks.models import Checklist
from apps.tasks.recurrence_utils import (
    normalize_mode,
    RECURRING_MODES,
    compute_next_planned_datetime,  # preserves wall-clock; shifts to next working day
    is_working_day,
    next_working_day,
)
from apps.tasks.utils import send_checklist_assignment_to_user

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Email policy knobs
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
# Send emails strictly around 10:00 IST (default window 09:55–10:05)
SEND_RECUR_EMAILS_ONLY_AT_10AM = True
EMAIL_WINDOW_MINUTES = 5


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


def _within_10am_ist_window(leeway_minutes: int = EMAIL_WINDOW_MINUTES) -> bool:
    """
    True if now (IST) is within [10:00 - leeway, 10:00 + leeway].
    Default window: 09:55–10:05 IST.
    """
    now_ist = timezone.now().astimezone(IST)
    anchor = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return (anchor - timedelta(minutes=leeway_minutes)) <= now_ist <= (anchor + timedelta(minutes=leeway_minutes))


def _send_recur_email(obj: Checklist) -> None:
    """
    Send an assignee-only email reminder for a planned task (no admin CC).
    Only runs at ~10:00 IST if SEND_RECUR_EMAILS_ONLY_AT_10AM is True.
    """
    if not SEND_EMAILS_FOR_AUTO_RECUR:
        return
    if SEND_RECUR_EMAILS_ONLY_AT_10AM and not _within_10am_ist_window():
        # Not in the 10:00 window -> skip quietly
        logger.info(_safe_console_text(f"Skip recur email for CL-{obj.id}: outside 10:00 IST window"))
        return

    try:
        planned_ist = obj.planned_date.astimezone(IST) if obj.planned_date else None
        pretty_date = planned_ist.strftime("%d %b %Y") if planned_ist else "N/A"
        pretty_time = planned_ist.strftime("%H:%M") if planned_ist else "N/A"

        complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[obj.id])}"
        subject = f"✅ Task Reminder: {obj.task_name} scheduled for {pretty_date}, {pretty_time}"

        send_checklist_assignment_to_user(
            task=obj,
            complete_url=complete_url,
            subject_prefix=subject,  # exact subject
        )
        logger.info(_safe_console_text(f"Sent 10:00 reminder for CL-{obj.id} to user_id={obj.assign_to_id}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Failed to send recurring reminder for CL-{obj.id}: {e}"))


class Command(BaseCommand):
    help = (
        "Recurring Checklist generator & 10:00 reminder sender.\n"
        "Rules:\n"
        "• Always generate the next occurrence on a working day (Sunday/holiday → next working day).\n"
        "• The TIME-OF-DAY of recurrences is preserved from the previous planned_time (e.g., 19:00).\n"
        "• Generate even if previous is not completed (missed tasks remain; next still appears).\n"
        "• Emails are sent at ~10:00 IST on the planned day (subject uses date/time).\n"
        "• Dashboard hides future tasks until 10:00 IST (handled by views).\n"
        "• Idempotent dupe-guard (±1 minute) on planned_date.\n"
    )

    def add_arguments(self, parser):
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee (user id).")
        parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to DB.")
        parser.add_argument("--no-email", action="store_true", help="Skip sending the 10:00 reminders.")
        parser.add_argument(
            "--email-today-only",
            action="store_true",
            help="Only send 10:00 reminders for tasks whose planned date is today IST (default behavior).",
        )

    def handle(self, *args, **opts):
        user_id = opts.get("user_id")
        dry_run = bool(opts.get("dry_run", False))
        send_emails = not bool(opts.get("no_email", False))
        email_today_only = bool(opts.get("email_today_only", True))

        now = timezone.now()
        now_ist = now.astimezone(IST)
        today_ist = now_ist.date()

        created_total = 0
        email_total = 0
        per_user_created: dict[int, int] = {}
        per_user_emailed: dict[int, int] = {}

        # Build recurring series seeds
        filters = {"mode__in": RECURRING_MODES}
        if user_id:
            filters["assign_to_id"] = user_id

        seeds = (
            Checklist.objects.filter(**filters)
            .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
            .distinct()
        )

        logger.info(_safe_console_text(f"[RECUR] Starting @ {now_ist:%Y-%m-%d %H:%M IST} | seeds={seeds.count()}"))

        # -------- 1) GENERATE OCCURRENCES (catch-up up to TODAY) --------
        for s in seeds:
            series_key = dict(
                assign_to_id=s["assign_to_id"],
                task_name=s["task_name"],
                mode=s["mode"],
                frequency=s["frequency"],
                group_name=s["group_name"],
            )
            m = normalize_mode(s["mode"])
            if m not in RECURRING_MODES:
                continue
            freq = max(int(s["frequency"] or 1), 1)

            # Latest occurrence (any status), use as the stepping base
            last = (
                Checklist.objects.filter(**series_key)
                .order_by("-planned_date", "-id")
                .first()
            )
            if not last or not last.planned_date:
                logger.debug(_safe_console_text(f"[RECUR] No base occurrence; skip: {series_key}"))
                continue

            # Catch-up loop: keep adding next occurrences until we reach >= today IST
            safety = 0
            cur_dt = last.planned_date
            while safety < 730:  # ~2 years safety
                next_dt = compute_next_planned_datetime(cur_dt, m, freq)
                if not next_dt:
                    break

                next_dt_ist = _to_ist(next_dt)
                next_date = next_dt_ist.date()

                # If next occurrence already exists (any status) within ±1 minute → step forward
                dupe_exists = Checklist.objects.filter(
                    planned_date__gte=next_dt - timedelta(minutes=1),
                    planned_date__lt=next_dt + timedelta(minutes=1),
                    **series_key,
                ).exists()

                if dupe_exists:
                    cur_dt = next_dt  # move the stepping base forward
                    safety += 1
                    if next_date >= today_ist:
                        # We've caught up to today/future; stop generating more
                        break
                    continue

                # Create the next occurrence
                if dry_run:
                    created_total += 1
                    per_user_created[s["assign_to_id"]] = per_user_created.get(s["assign_to_id"], 0) + 1
                    logger.info(_safe_console_text(
                        f"[DRY RUN] Would create '{s['task_name']}' for user_id={s['assign_to_id']} "
                        f"at {next_dt_ist:%Y-%m-%d %H:%M IST}"
                    ))
                else:
                    try:
                        with transaction.atomic():
                            obj = Checklist.objects.create(
                                assign_by=last.assign_by,
                                task_name=last.task_name,
                                message=last.message,
                                assign_to=last.assign_to,
                                planned_date=next_dt,  # same wall-clock time; shifted to working day if needed
                                priority=last.priority,
                                attachment_mandatory=last.attachment_mandatory,
                                mode=last.mode,
                                frequency=last.frequency,
                                recurrence_end_date=getattr(last, "recurrence_end_date", None),
                                time_per_task_minutes=last.time_per_task_minutes,
                                remind_before_days=last.remind_before_days,
                                assign_pc=last.assign_pc,
                                notify_to=last.notify_to,
                                auditor=getattr(last, "auditor", None),
                                set_reminder=last.set_reminder,
                                reminder_mode=last.reminder_mode,
                                reminder_frequency=last.reminder_frequency,
                                reminder_starting_time=last.reminder_starting_time,
                                checklist_auto_close=last.checklist_auto_close,
                                checklist_auto_close_days=last.checklist_auto_close_days,
                                group_name=getattr(last, "group_name", None),
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
                        logger.exception("Failed creating next occurrence for %s: %s", series_key, e)
                        break  # avoid infinite loop on persistent failure

                cur_dt = next_dt
                safety += 1
                # Stop as soon as we have caught up to today/future
                if next_date >= today_ist:
                    break

        # -------- 2) SEND 10:00 REMINDERS FOR TODAY'S TASKS --------
        if send_emails and SEND_EMAILS_FOR_AUTO_RECUR and (not SEND_RECUR_EMAILS_ONLY_AT_10AM or _within_10am_ist_window()):
            # Fetch all PENDING tasks scheduled for TODAY IST @ any time (commonly 19:00)
            start_today_ist = IST.localize(datetime.combine(today_ist, dt_time.min))
            end_today_ist = IST.localize(datetime.combine(today_ist, dt_time.max))

            # Convert bounds to project TZ for querying
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
                if email_today_only:
                    # Already bounded to today; send
                    if not dry_run:
                        _send_recur_email(obj)
                    email_total += 1
                    per_user_emailed[obj.assign_to_id] = per_user_emailed.get(obj.assign_to_id, 0) + 1
                else:
                    # Defensive (should not happen due to bounds)
                    if obj.planned_date.astimezone(IST).date() == today_ist:
                        if not dry_run:
                            _send_recur_email(obj)
                        email_total += 1
                        per_user_emailed[obj.assign_to_id] = per_user_emailed.get(obj.assign_to_id, 0) + 1

        # -------- 3) Summaries --------
        if per_user_created:
            for uid, count in per_user_created.items():
                logger.info(_safe_console_text(f"[RECUR GEN] user_id={uid} → created {count} occurrence(s)"))
        else:
            logger.info(_safe_console_text(f"[RECUR GEN] No new occurrences were needed today."))

        if per_user_emailed:
            for uid, count in per_user_emailed.items():
                logger.info(_safe_console_text(f"[RECUR MAIL] user_id={uid} → sent {count} reminder(s)"))
        else:
            if send_emails:
                logger.info(_safe_console_text(f"[RECUR MAIL] No reminders sent (either outside 10:00 window or none due)."))

        # CLI summary line
        parts = [f"Created {created_total} task(s)"]
        if send_emails:
            parts.append(f"Emailed {email_total} reminder(s)")
        if dry_run:
            msg = "[DRY RUN] " + ", ".join(parts)
            self.stdout.write(self.style.WARNING(msg))
        else:
            self.stdout.write(self.style.SUCCESS(", ".join(parts)))
