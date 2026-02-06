# apps/tasks/views_cron.py
from __future__ import annotations

from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils import timezone

from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,  # pure ORM scorer
)

# Celery tasks & orchestrators (existing core)
from apps.tasks.tasks import (
    generate_recurring_checklists,    # ensure “future” rows exist (not today)
    send_due_today_assignments,       # 10:00 IST fan-out (leave aware; centrally locked)
    pre10am_unblock_and_generate,     # 09:55 IST safeguard
)

# ✅ NEW: lightweight “today-only” materializer (safe, no emails)
try:
    from apps.tasks.materializer import materialize_today_for_all  # type: ignore
except Exception:  # pragma: no cover
    materialize_today_for_all = None  # type: ignore

import threading
import datetime


def _get_cron_token(request, token: str = "") -> str:
    """
    IMPORTANT: do NOT touch request.POST here.
    We accept token via:
      - path
      - header
      - querystring
    """
    return (
        token
        or request.headers.get("X-CRON-TOKEN", "")
        or request.GET.get("token", "")
    )


def _cron_authorized(request, token: str = "") -> bool:
    expected = getattr(settings, "CRON_SECRET", "") or ""
    provided = _get_cron_token(request, token)
    # If CRON_SECRET is empty, allow (useful for dev)
    return True if not expected else (provided == expected)


@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    """
    1) Upsert WeeklyScore for last week (pure ORM; IST window; idempotent).
    2) If email feature is on, send congratulations mails (>= 90%) once per user/week.
    Always JSON (so cron logs are readable).
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        score_summary = upsert_weekly_scores_for_last_week()  # no UDFs; safe on SQLite

        mail_summary = {}
        if getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            mail_summary = send_weekly_congratulations_mails() or {}

        payload = {
            "ok": True,
            "method": request.method,
            "at": datetime.datetime.utcnow().isoformat(),
            "scores": score_summary,
            "emails": mail_summary,
        }
        return JsonResponse(payload)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


def _run_due_today_in_background():
    """
    Do the heavy work outside the HTTP request thread to avoid Gunicorn timeouts.

    Order of operations (all best-effort; each guarded so one failure doesn’t kill the run):
      0) (NEW) Materialize missing *today* rows for recurring series (skip users on leave @10:00 IST)
      1) Pre-generate future recurring rows (existing generator; never creates “today”)
      2) Send due-today emails (existing, leave-aware + cross-process lock)

    NOTE: send_due_today_assignments has a cross-process filesystem lock,
    so even if multiple triggers fire, only one fan-out runs for the day.
    """
    try:
        # 0) Today-only materialization (no emails; safe if module absent)
        try:
            if callable(materialize_today_for_all):
                materialize_today_for_all(dry_run=False)
        except Exception:
            # keep silent here; details should be in app logs
            pass

        # 1) Best-effort pre-gen (future occurrences); never crash the thread if gen fails
        try:
            generate_recurring_checklists.run(dry_run=False)
        except Exception:
            pass

        # 2) Actual fan-out (task contains duplicate guard + leave guard)
        try:
            send_due_today_assignments.run()
        except Exception:
            # Swallow to keep the background worker alive; logs already capture details
            pass
    except Exception:
        # Absolute last-resort guard
        pass


@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    10:00 AM due-today fan-out trigger.
    Returns immediately (JSON) and performs the heavy work in a background thread,
    so HTTP never times out and Render cron never sees 500.
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        # Feature flag can short-circuit quickly
        if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            return JsonResponse(
                {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
            )

        # Start background job
        t = threading.Thread(target=_run_due_today_in_background, daemon=True)
        t.start()

        return JsonResponse(
            {
                "ok": True,
                "accepted": True,
                "method": request.method,
                "at": datetime.datetime.utcnow().isoformat(),
                "note": "Fan-out running in background thread (with today-materialize pre-step)",
            }
        )
    except Exception as e:
        # Always JSON on error
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


@csrf_exempt
@require_http_methods(["GET"])
def pre10am_unblock_and_generate_hook(request, token: str = ""):
    """
    09:55 IST safeguard:
      1) Complete overdue daily 'Pending' rows (yesterday or earlier)
      2) Generate 'today' items so dashboard & 10AM mails are correct
    Optional: ?user_id=123 to target a single user.

    (Note: The due-today endpoint will still run the lightweight 'today' materializer
    before mailing, so running both is safe and idempotent.)
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        uid = request.GET.get("user_id")
        try:
            uid = int(uid) if uid else None
        except Exception:
            uid = None

        # NEW: opportunistically materialize “today” before the legacy pre10 step
        mat = {}
        try:
            if callable(materialize_today_for_all):
                mat = materialize_today_for_all(user_id=uid, dry_run=False).__dict__
        except Exception:
            mat = {"created": 0, "error": "materializer_failed"}

        result = pre10am_unblock_and_generate(user_id=uid)

        return JsonResponse(
            {
                "ok": True,
                "method": request.method,
                "at": timezone.now().isoformat(),
                "materialize_today": mat,   # visibility for logs
                **result,
            }
        )
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )
