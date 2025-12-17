from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import send_weekly_congratulations_mails


def _get_cron_token(request, token: str = "") -> str:
    """
    Accept token via:
      - path: /.../<token>/
      - header: X-CRON-TOKEN: <token>
      - query:  ?token=<token>
      - body:   token=<token> (form-encoded)
    """
    return (
        token
        or request.headers.get("X-CRON-TOKEN", "")
        or request.GET.get("token", "")
        or request.POST.get("token", "")
    )


def _cron_authorized(request, token: str = "") -> bool:
    expected = getattr(settings, "CRON_SECRET", "") or ""
    provided = _get_cron_token(request, token)
    return bool(expected) and provided == expected


@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    """
    Lightweight, token-gated cron hook for weekly congratulations emails.
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse(
            {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
        )

    summary = send_weekly_congratulations_mails() or {}
    return JsonResponse({"ok": True, "triggered": True, "method": request.method, **summary})


@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Token-gated cron hook for the 10:00 AM due-today fan-out.

    This MUST run inside the WEB service process (shared persistent SQLite),
    because Render Cron containers do not reliably see the disk.
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse(
            {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
        )

    try:
        # Lazy import so URL loading doesn't import celery/task modules at startup
        from apps.tasks.tasks import send_due_today_assignments

        result = send_due_today_assignments.run()
        return JsonResponse(
            {"ok": True, "triggered": True, "method": request.method, "result": result}
        )
    except Exception as e:
        # Return JSON so cron can see the real reason instead of a blank HTML 500
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=500,
        )
