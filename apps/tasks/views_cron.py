# apps/tasks/views_cron.py
from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,  # pure ORM scorer
)

# ⬇️ Celery tasks we want to run inline for cron hooks
from apps.tasks.tasks import (
    generate_recurring_checklists,    # ensure “today” rows exist
    send_due_today_assignments,       # 10:00 IST fan-out (leave aware)
)

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
    1) Pre-generate recurring 'today' rows
    2) Send due-today emails
    """
    try:
        # Best-effort pre-gen; never crash the thread if gen fails
        try:
            generate_recurring_checklists.run(dry_run=False)
        except Exception:
            pass

        # Actual fan-out (built-in batching/pauses in the task still apply)
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
                "note": "Fan-out running in background thread",
            }
        )
    except Exception as e:
        # Always JSON on error
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )
