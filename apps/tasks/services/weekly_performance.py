# apps/tasks/services/weekly_performance.py
from __future__ import annotations

from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import transaction
from django.template.loader import render_to_string
from django.utils import timezone
from zoneinfo import ZoneInfo

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

IST = ZoneInfo("Asia/Kolkata")


# ----------------------------- date helpers (IST) ----------------------------- #
def _now_ist() -> datetime:
    return timezone.localtime(timezone.now(), IST)


def _last_week_bounds_ist(today: date | None = None) -> Tuple[datetime, datetime, date, date]:
    """
    Returns (start_dt, end_dt_exclusive, start_date, end_date_inclusive)
    for last week (Mon..Sun) in IST.
    """
    today = today or timezone.localdate()
    # Monday of this week
    this_mon = today - timedelta(days=today.weekday())
    last_mon = this_mon - timedelta(days=7)
    last_sun = last_mon + timedelta(days=6)

    start_dt = timezone.make_aware(datetime.combine(last_mon, time.min), IST)
    end_dt = timezone.make_aware(datetime.combine(this_mon, time.min), IST)  # exclusive
    return start_dt, end_dt, last_mon, last_sun


# --------------------------------- math -------------------------------------- #
def _pct(numerator: int, denominator: int) -> Decimal:
    """
    Completion % (0..100). If denominator is 0, we treat it as 100% to avoid penalizing
    users with no assignments and to keep math safe (no division by zero).
    """
    if denominator <= 0:
        return Decimal("100.00")
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
        # If the template is missing, send_html_email already falls back safely.
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


# --------------------------- scoring core (ORM-only) -------------------------- #
def upsert_weekly_scores_for_last_week() -> dict:
    """
    Pure ORM (no SQLite UDFs): computes last week's (Mon..Sun IST) completion %
    per active user and upserts WeeklyScore(user, week_start).
    """
    start_dt, end_dt, week_start_date, week_end_date = _last_week_bounds_ist()

    users = User.objects.filter(is_active=True).only("id", "first_name", "last_name", "username", "email").order_by("id")
    created = updated = 0

    for u in users:
        assigned = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
        )
        completed = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
        )

        score = _pct(completed, assigned)

        # idempotent upsert
        obj, was_created = WeeklyScore.objects.update_or_create(
            user=u,
            week_start=week_start_date,
            defaults={"score": score},
        )
        created += int(was_created)
        updated += int(not was_created)

    logger.info(
        _safe_console_text(
            f"WeeklyScore upsert done: users={users.count()} created={created} updated={updated} "
            f"window={week_start_date}..{week_end_date}"
        )
    )
    return {"created": created, "updated": updated, "total_users": users.count(), "week_start": str(week_start_date)}


# ------------------------------ mail wrapper --------------------------------- #
def send_weekly_congratulations_mails() -> dict:
    """
    Evaluate last week (Mon..Sun IST), and send 'Congratulations' mail to
    users with >=90% completion — only once per (user, week) thanks to WeeklyScore.
    """
    start_dt, end_dt, week_start_date, week_end_date = _last_week_bounds_ist()
    users = User.objects.filter(is_active=True).order_by("id")

    sent = skipped = 0

    for u in users:
        total_assigned = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt).count()
        )
        total_completed = (
            Checklist.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
            + Delegation.objects.filter(assign_to=u, planned_date__gte=start_dt, planned_date__lt=end_dt, status="Completed").count()
        )

        score = _pct(total_completed, total_assigned)

        # Only congratulate if >= 90%
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

    logger.info(
        _safe_console_text(
            f"Weekly congrats summary: users={users.count()} sent={sent} skipped={skipped} "
            f"window={week_start_date}..{week_end_date}"
        )
    )
    return {"sent": sent, "skipped": skipped, "total_users": users.count(), "week_start": str(week_start_date)}
