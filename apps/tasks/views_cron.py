from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from apps.tasks.services.weekly_performance import send_weekly_congratulations_mails


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
    expected = getattr(settings, "CRON_SECRET", "") or ""
    provided = (
        token
        or request.headers.get("X-CRON-TOKEN", "")
        or request.GET.get("token", "")
        or request.POST.get("token", "")
    )

    if not expected or provided != expected:
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
