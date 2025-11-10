# apps/tasks/services/weekly_performance.py
from __future__ import annotations

from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.template.loader import render_to_string
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation
from apps.reports.models import WeeklyScore
from apps.tasks.utils import (
    send_html_email,
    _safe_console_text,
    SITE_URL,
)
import logging

logger = logging.getLogger(__name__)
User = get_user_model()


def _week_bounds_for_last_week(today: date | None = None) -> Tuple[datetime, datetime, date, date]:
    """
    Returns (start_dt, end_dt, start_date, end_date) for last week (Mon..Sun), tz-aware.
    end_dt is exclusive (next Monday 00:00).
    """
    tz = timezone.get_current_timezone()
    today = today or timezone.localdate()
    # Monday of current week
    this_week_monday = today - timedelta(days=today.weekday())
    last_week_monday = this_week_monday - timedelta(days=7)
    last_week_sunday = last_week_monday + timedelta(days=6)

    start_dt = timezone.make_aware(datetime.combine(last_week_monday, time.min), tz)
    end_dt = timezone.make_aware(datetime.combine(this_week_monday, time.min), tz)  # exclusive
    return start_dt, end_dt, last_week_monday, last_week_sunday


def _pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0.00")
    val = (Decimal(numerator) / Decimal(denominator)) * Decimal(100)
    return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _already_mailed(user: User, week_start: date) -> bool:
    return WeeklyScore.objects.filter(user=user, week_start=week_start).exists()


def _send_congrats_email(user: User, score: Decimal, week_start: date, week_end: date) -> bool:
    """
    Send the 'congratulations' email using the unified HTML helper.
    Returns True on success, False on failure.
    """
    if not getattr(user, "email", ""):
        return False

    subject = f"Congratulations! You achieved {score}% task completion last week!"
    ctx = {
        "user": user,
        "score": score,
        "week_start": week_start,
        "week_end": week_end,
        "site_url": SITE_URL,
    }

    try:
        # Prefer template-based rendering (the template can use the context above)
        # Fallback HTML is handled by send_html_email if template rendering fails.
        send_html_email(
            subject=subject,
            template_name="email/congratulations_mail.html",
            context=ctx,
            to=[user.email],
        )
        return True
    except Exception as e:
        logger.error(_safe_console_text(f"Congrats email failed for {user}: {e}"))
        return False


def send_weekly_congratulations_mails() -> dict:
    """
    Run every Monday 10:00 IST (or project TZ): evaluate last week (Mon..Sun),
    send 'Congratulations' mail to users with >=90% completion, store WeeklyScore to avoid duplicates.
    Returns a summary dict.
    """
    if not settings.FEATURES.get("EMAIL_NOTIFICATIONS", True):
        logger.info("Weekly congrats skipped: FEATURES.EMAIL_NOTIFICATIONS=False")
        return {"sent": 0, "skipped": 0, "total_users": 0}

    start_dt, end_dt, week_start_date, week_end_date = _week_bounds_for_last_week()
    users = User.objects.filter(is_active=True).order_by("id")
    sent = skipped = 0

    for u in users:
        # Totals for last week
        total_assigned = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
        )
        total_completed = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
        )

        score = _pct(total_completed, total_assigned)

        if score >= Decimal("90.00"):
            if _already_mailed(u, week_start_date):
                skipped += 1
                continue

            ok = _send_congrats_email(u, score, week_start_date, week_end_date)
            if ok:
                try:
                    WeeklyScore.objects.create(user=u, week_start=week_start_date, score=score)
                except Exception:
                    # In case of race/duplicate, ignore and continue
                    pass
                sent += 1
            else:
                skipped += 1
        else:
            skipped += 1

    logger.info(_safe_console_text(
        f"Weekly congrats summary: users={users.count()} sent={sent} skipped={skipped} "
        f"window={week_start_date}..{week_end_date}"
    ))
    return {"sent": sent, "skipped": skipped, "total_users": users.count()}
