# apps/tasks/views_cron.py
from __future__ import annotations

import json
from datetime import datetime, date
from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,  # pure ORM scorer
)

# ⬇️ Celery-like tasks that also expose .run()
from apps.tasks.tasks import (
    generate_recurring_checklists,    # ensure “today” rows exist
    send_due_today_assignments,       # 10:00 IST fan-out (leave aware)
)


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
        or request.GET.get("key", "")  # backward-compat with old ?key=
    )


def _cron_authorized(request, token: str = "") -> bool:
    expected = getattr(settings, "CRON_SECRET", "") or ""
    provided = _get_cron_token(request, token)
    # If CRON_SECRET is empty, allow (useful for dev)
    return True if not expected else (provided == expected)


def _json_sanitize(obj):
    """
    Make any task return JSON-safe.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return repr(obj)


def _json_ok(payload: dict, status: int = 200) -> JsonResponse:
    try:
        return JsonResponse(payload, status=status)
    except Exception:
        # Fallback: force-serialize weird objects
        safe = json.loads(json.dumps(payload, default=_json_sanitize))
        return JsonResponse(safe, status=status)


@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    """
    1) Upsert WeeklyScore for last week (pure ORM; IST window; idempotent).
    2) If email feature is on, send congratulations mails (>= 90%) once per user/week.
    Always JSON (so cron logs are readable).
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    try:
        score_summary = upsert_weekly_scores_for_last_week()  # no UDFs; safe on SQLite

        mail_summary = {}
        if getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            try:
                mail_summary = send_weekly_congratulations_mails() or {}
            except Exception as e:
                mail_summary = {"ok": False, "error": str(e)}

        payload = {
            "ok": True,
            "method": request.method,
            "at": datetime.utcnow().isoformat(),
            "scores": score_summary,
            "emails": mail_summary,
        }
        return _json_ok(payload, status=200)
    except Exception as e:
        return _json_ok(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Runs the 10:00 AM due-today fan-out INSIDE the web service (shared SQLite disk).
    Always returns JSON even on failure (so cron doesn't see a blank HTML 500).

    CHANGE:
    1) Pre-generate “today” recurring occurrences right here before mailing.
    2) Catch & serialize ANY mail errors instead of letting them 500.
    3) Make JSON serialization robust (no datetime/set bombs).
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return _json_ok(
            {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
        )

    gen_summary = {"ok": True}
    try:
        # Ensure today’s recurring rows exist to avoid race with a separate cron
        gen_summary = generate_recurring_checklists.run(dry_run=False) or {"ok": True}
    except Exception as e:
        gen_summary = {"ok": False, "error": str(e)}

    mail_result = {"ok": True}
    try:
        mail_result = send_due_today_assignments.run() or {"ok": True}
    except Exception as e:
        # DO NOT crash; surface error in JSON so cron sees 200 + details
        mail_result = {"ok": False, "error": str(e)}

    payload = {
        "ok": True,
        "method": request.method,
        "at": datetime.utcnow().isoformat(),
        "generated": gen_summary,
        "result": mail_result,
    }
    return _json_ok(payload, status=200)
