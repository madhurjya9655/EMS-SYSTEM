from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import send_weekly_congratulations_mails


def _get_cron_token(request, token: str = "") -> str:
    """
    IMPORTANT: do NOT touch request.POST here.
    Some deployments/middlewares can raise parsing errors depending on content-type.
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
    return bool(expected) and provided == expected


@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    try:
        if not _cron_authorized(request, token):
            return HttpResponseForbidden("Forbidden")

        if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            return JsonResponse(
                {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method}
            )

        summary = send_weekly_congratulations_mails() or {}
        return JsonResponse({"ok": True, "triggered": True, "method": request.method, **summary})
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
