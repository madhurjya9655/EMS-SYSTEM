# apps/tasks/cron_views.py
from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta

import pytz
from django.conf import settings
from django.core.cache import cache
from django.http import JsonResponse, HttpResponseForbidden
from django.utils import timezone

from .tasks import (
    send_due_today_assignments,
)
from .pending_digest import (
    send_daily_employee_pending_digest,
    send_admin_all_pending_digest,
)


IST = pytz.timezone(getattr(settings, "TIME_ZONE", "Asia/Kolkata"))


def _authorized(request) -> bool:
    """
    Accept either:
      • ?key=<CRON_SECRET>
      • X-Cron-Key: <CRON_SECRET>
      • (lenient) ?token=<CRON_SECRET> or X-CRON-TOKEN: <CRON_SECRET> (back-compat)

    If no CRON_SECRET is defined in settings, allow (useful for local/dev).
    """
    key = (
        request.GET.get("key")
        or request.headers.get("X-Cron-Key")
        or request.GET.get("token")
        or request.headers.get("X-CRON-TOKEN")
    )
    expected = getattr(settings, "CRON_SECRET", "") or ""
    return True if not expected else (key == expected)


# -----------------------------
# Shared idempotency helpers
# -----------------------------
def _today_ist_date() -> str:
    return timezone.now().astimezone(IST).date().isoformat()


def _next_3am_ist_ttl_seconds() -> int:
    now_ist = timezone.now().astimezone(IST)
    next3 = (now_ist + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    return max(int((next3 - now_ist).total_seconds()), 60)


def _fanout_done_key(day_iso: str) -> str:
    return f"due10_fanout_done:{day_iso}"


def _fanout_lock_key(day_iso: str) -> str:
    return f"due10_fanout_lock:{day_iso}"


def _acquire_fanout_lock(day_iso: str, seconds: int = 180) -> bool:
    """
    Prevent concurrent/millisecond duplicate runs (e.g., two endpoints triggered).
    Non-blocking: only the first caller acquires the lock.
    """
    return cache.add(_fanout_lock_key(day_iso), True, seconds)


def _mark_fanout_done(day_iso: str) -> None:
    cache.set(_fanout_done_key(day_iso), True, _next_3am_ist_ttl_seconds())


def _fanout_already_done(day_iso: str) -> bool:
    return bool(cache.get(_fanout_done_key(day_iso), False))


def _release_fanout_lock(day_iso: str) -> None:
    # LocMemCache has no explicit delete-if-exists race risk here; best-effort cleanup.
    try:
        cache.delete(_fanout_lock_key(day_iso))
    except Exception:
        pass


def due_today(request):
    """
    10:00 IST fan-out (safe at any time; task guards pre-10:00 IST itself).
    GET /internal/cron/due-today/?key=...   OR send header X-Cron-Key: <secret>

    HARDENED:
      • Never raises to caller.
      • Always returns JSON (status 200), even on internal error.
      • Preserves simple key/header auth semantics.
      • Adds a cross-endpoint idempotency guard to avoid duplicate fan-outs.
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    day_iso = _today_ist_date()

    # If we've already successfully fanned out for today, treat as no-op.
    if _fanout_already_done(day_iso):
        return JsonResponse({"ok": True, "triggered": False, "reason": "already_done_today"}, status=200)

    # Acquire a short processing lock so parallel triggers don't run together.
    if not _acquire_fanout_lock(day_iso, seconds=180):
        # Someone else is running it right now.
        return JsonResponse({"ok": True, "triggered": False, "reason": "already_running"}, status=200)

    try:
        # Synchronous run (not .delay); the task decides whether to skip (pre-10 gate etc).
        result = send_due_today_assignments.run()
        # Mark day done regardless of sent count to prevent duplicate second runs that would re-send.
        _mark_fanout_done(day_iso)
        return JsonResponse({"ok": True, "triggered": True, "result": result}, status=200)
    except Exception as e:
        # Keep HTTP 200 so Render Cron doesn't mark it as failed;
        # details still appear in logs and JSON.
        return JsonResponse(
            {"ok": False, "triggered": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )
    finally:
        _release_fanout_lock(day_iso)


def pending_summary_7pm(request):
    """
    19:00 IST consolidated summaries:
      - Admin mail with ALL pending.
      - One mail per employee with ONLY their pending.
    GET /internal/cron/pending-7pm/?key=...

    D1/D4 compliance:
      • Removed hardcoded 'pankaj@blueoceansteels.com' recipient.
      • Defer recipients to the digest task (which uses admin resolution logic),
        ensuring no special-case direct emails to Pankaj from this endpoint.
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        # Admin consolidated digest (recipient resolution handled inside task)
        admin = send_admin_all_pending_digest.run(force=True)

        # Per-employee digests (recipient resolution handled inside task)
        employees = send_daily_employee_pending_digest.run(force=True)

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
