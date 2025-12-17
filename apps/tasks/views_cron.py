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

    Accepts:
      - Token in path: /internal/cron/weekly-congrats/<token>/
      - Or header:     X-CRON-TOKEN: <token>
      - Or query/body: ?token=<token>  /  token=<token>

    Allows GET (for providers that only support GET) and POST.
    Returns JSON with a simple summary; never echoes back any token.
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse(
            {
                "ok": True,
                "skipped": True,
                "reason": "feature_flag_off",
                "method": request.method,
            }
        )

    summary = send_weekly_congratulations_mails() or {}
    return JsonResponse(
        {
            "ok": True,
            "triggered": True,
            "method": request.method,
            **summary,
        }
    )


@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Token-gated cron hook for the 10:00 AM due-today fan-out.

    IMPORTANT (why this exists):
    Render Cron runs in an isolated container and may not see the persistent
    SQLite disk used by the web service. If cron runs ORM directly, it can hit
    an empty SQLite DB and fail with: "no such table: tasks_checklist".

    This hook runs INSIDE the web service process (correct DB), and triggers the
    exact same code path: apps.tasks.tasks.send_due_today_assignments.run()
    No recurrence/business logic changes.
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse(
            {
                "ok": True,
                "skipped": True,
                "reason": "feature_flag_off",
                "method": request.method,
            }
        )

    # Lazy import so loading URLs doesn't import celery/task modules at startup
    from apps.tasks.tasks import send_due_today_assignments

    result = send_due_today_assignments.run()
    return JsonResponse(
        {
            "ok": True,
            "triggered": True,
            "method": request.method,
            "result": result,
        }
    )
