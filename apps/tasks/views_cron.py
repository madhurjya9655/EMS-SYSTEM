from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,  # NEW: pure-ORM scorer (no UDFs)
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

        # (1) Always compute & upsert weekly scores — this is where the crash used to happen.
        score_summary = upsert_weekly_scores_for_last_week()  # no UDFs; safe on SQLite

        # (2) Optionally send emails
        mail_summary = {}
        if getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            mail_summary = send_weekly_congratulations_mails() or {}

        payload = {
            "ok": True,
            "method": request.method,
            "scores": score_summary,
            "emails": mail_summary,
        }
        return JsonResponse(payload)
    except Exception as e:
        # Always JSON, never HTML (makes cron debugging possible)
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )


@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Runs the 10:00 AM due-today fan-out INSIDE the web service (shared SQLite disk).
    Always returns JSON even on failure (so cron doesn't see a blank HTML 500).
    """
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            return JsonResponse(
                {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
            )

        from apps.tasks.tasks import send_due_today_assignments
        result = send_due_today_assignments.run()
        return JsonResponse({"ok": True, "triggered": True, "method": request.method, "result": result})
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )
