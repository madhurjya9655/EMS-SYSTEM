# apps/tasks/views_cron.py
from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt

# Uses your existing weekly mail logic
from apps.tasks.services.weekly_performance import send_weekly_congratulations_mails


@csrf_exempt
def weekly_congrats_hook(request, token: str):
    """
    Secure, CSRF-exempt HTTP hook that triggers weekly congrats emails.
    Protected by a shared-secret token in the URL.

    Endpoint (POST):
      /tasks/internal/cron/weekly-congrats/<token>/

    Env var expected in settings:
      CRON_SECRET
    """
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    expected = getattr(settings, "CRON_SECRET", "")
    if not expected or token != expected:
        return HttpResponseForbidden("Forbidden")

    # Optional feature flag guard
    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse({"ok": True, "skipped": True, "reason": "feature_flag_off"})

    summary = send_weekly_congratulations_mails() or {}
    # example summary: {"sent": X, "skipped": Y, "users": N, "window": "...."}
    return JsonResponse({"ok": True, **summary})
