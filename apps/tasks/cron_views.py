# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\cron_views.py
# apps/tasks/cron_views.py
from __future__ import annotations

from datetime import date

import pytz
from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.utils import timezone

from .pending_digest import (
    send_daily_employee_pending_digest,
    send_admin_all_pending_digest,
)

# ✅ IMPORTANT:
# This module must NOT run the due-today pipeline itself.
# It should only authorize and delegate to the canonical trigger in views_cron.py
from .views_cron import start_due_today_fanout  # single source of truth

IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))


def _authorized(request, token: str = "") -> bool:
    """
    Accept either:
      • URL token (path converter): /.../<token>/
      • ?key=<CRON_SECRET>
      • X-Cron-Key: <CRON_SECRET>
      • (lenient) ?token=<CRON_SECRET> or X-CRON-TOKEN: <CRON_SECRET> (back-compat)

    If no CRON_SECRET is defined in settings, allow (useful for local/dev).
    """
    key = (
        (token or "").strip()
        or (request.GET.get("key") or "").strip()
        or (request.headers.get("X-Cron-Key") or "").strip()
        or (request.GET.get("token") or "").strip()
        or (request.headers.get("X-CRON-TOKEN") or "").strip()
    )
    expected = (getattr(settings, "CRON_SECRET", "") or "").strip()
    return True if not expected else (key == expected)


def _today_ist_date() -> str:
    return timezone.now().astimezone(IST).date().isoformat()


# -----------------------------
# Working-day helpers (IST)
# -----------------------------
def _today_ist() -> date:
    return timezone.now().astimezone(IST).date()


def _is_sunday_ist(d: date) -> bool:
    return d.weekday() == 6  # Sunday == 6


def _is_holiday_ist(d: date) -> bool:
    try:
        from apps.settings.models import Holiday  # optional
        return Holiday.objects.filter(date=d).exists()
    except Exception:
        return False


def _is_working_day_ist(d: date) -> bool:
    return (not _is_sunday_ist(d)) and (not _is_holiday_ist(d))


def due_today(request, token: str = ""):
    """
    10:00 IST fan-out endpoint.

    ✅ FIXED:
      - URL token route now works (view accepts `token`).
      - NO duplicated orchestration here.
      - This endpoint is only an AUTH + DELEGATE wrapper.
      - The canonical pipeline is owned by apps/tasks/views_cron.py

    Always returns JSON 200 (so cron provider doesn't retry aggressively).
    """
    if not _authorized(request, token=token):
        return HttpResponseForbidden("Forbidden")

    day_iso = _today_ist_date()

    try:
        # Delegate to canonical trigger.
        # background=True avoids HTTP timeouts and double work.
        res = start_due_today_fanout(day_iso=day_iso, background=True)
        return JsonResponse({"ok": True, "day": day_iso, "delegate": res}, status=200)
    except Exception as e:
        # Still return 200 to prevent cron retries spamming.
        return JsonResponse(
            {"ok": False, "day": day_iso, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )


def pending_summary_7pm(request):
    """
    19:00 IST consolidated summaries:
      - Admin mail with ALL pending.
      - One mail per employee with ONLY their pending.

    NOTE:
      • Runs only on working days (skips Sundays and configured holidays).
      • Leaves are handled inside the tasks (they suppress per-employee mails).
      • Uses non-forced mode so task-level guards remain effective.
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    today = _today_ist()
    if not _is_working_day_ist(today):
        return JsonResponse({"ok": True, "triggered": False, "reason": "non_working_day"}, status=200)

    try:
        admin = send_admin_all_pending_digest.run(force=False)
        employees = send_daily_employee_pending_digest.run(force=False)
        return JsonResponse({"ok": True, "admin": admin, "employees": employees}, status=200)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )


def employee_digest(request):
    """
    Manual trigger for a single user:
    GET /internal/cron/employee-digest/?key=...&username=<uname>&to=<override-email>
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        username = request.GET.get("username")
        to_override = request.GET.get("to")
        res = send_daily_employee_pending_digest.run(
            force=True,
            username=username,
            to_override=to_override,
        )
        return JsonResponse({"ok": True, "result": res}, status=200)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )
