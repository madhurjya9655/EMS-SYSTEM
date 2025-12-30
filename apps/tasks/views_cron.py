# apps/tasks/views_cron.py
from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils import timezone

# Weekly performance hooks
from apps.tasks.services.weekly_performance import (
    send_weekly_congratulations_mails,
    upsert_weekly_scores_for_last_week,  # pure ORM scorer
)

# Celery tasks we want to run inline for cron hooks (synchronous .run())
from apps.tasks.tasks import (
    generate_recurring_checklists,    # ensure “today” rows exist (recurring)
    send_due_today_assignments,       # 10:00 IST fan-out (leave aware, duplicate safe)
)


# -----------------------
# Auth helpers (key or token)
# -----------------------
def _get_cron_secret() -> str:
    return (getattr(settings, "CRON_SECRET", "") or "").strip()

def _get_credential_from_request(request, token: str = "") -> str:
    """
    IMPORTANT: do NOT touch request.POST here.
    Accept via:
      • URL path param (token)
      • Headers: X-CRON-TOKEN or X-Cron-Key
      • Query:  ?token=...  or  ?key=...
    """
    return (
        (token or "").strip()
        or (request.headers.get("X-CRON-TOKEN", "").strip())
        or (request.headers.get("X-Cron-Key", "").strip())
        or (request.GET.get("token", "").strip())
        or (request.GET.get("key", "").strip())
    )

def _cron_authorized(request, token: str = "") -> bool:
    expected = _get_cron_secret()
    provided = _get_credential_from_request(request, token)
    # If CRON_SECRET is empty, allow (useful for dev)
    return True if not expected else (provided == expected)


# -----------------------
# Weekly Congrats Hook (unchanged behavior; hardened output)
# -----------------------
@csrf_exempt
@require_http_methods(["GET", "POST"])
def weekly_congrats_hook(request, token: str = ""):
    """
    1) Upsert WeeklyScore for last week (pure ORM; IST window; idempotent).
    2) If email feature is on, send congratulations mails (>= 90%) once per user/week.
    Always JSON.
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    try:
        score_summary = upsert_weekly_scores_for_last_week()

        mail_summary = {}
        if getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
            mail_summary = send_weekly_congratulations_mails() or {}

        return JsonResponse(
            {
                "ok": True,
                "method": request.method,
                "at": timezone.now().isoformat(),
                "scores": score_summary,
                "emails": mail_summary,
            },
            status=200,
        )
    except Exception as e:
        # NOTE: Return 200 so cron never sees (22) 500; details remain in JSON + app logs
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e), "at": timezone.now().isoformat()},
            status=200,
        )


# -----------------------
# 10:00 AM “Due Today” Hook (pre-generate + mail fan-out)
# -----------------------
@csrf_exempt
@require_http_methods(["GET", "POST"])
def due_today_assignments_hook(request, token: str = ""):
    """
    Single entrypoint your cron calls.

    What it does:
      1) (Always) Pre-generate today's recurring checklists inline to remove race with a separate generator.
      2) Call the leave-aware 10:00 IST fan-out (send_due_today_assignments.run()).
         - The task itself cleanly returns 'skipped_before_10' if called too early.
         - It’s idempotent for the day, and uses in-process cache keys to prevent dupes.

    HARDENING:
      • Accepts ?key= / ?token= or headers.
      • Always returns JSON with HTTP 200 (even when internal errors happen), so your cron won’t log curl (22) 500.
      • All business logic stays in tasks.py (unchanged).
    """
    if not _cron_authorized(request, token):
        return HttpResponseForbidden("Forbidden")

    # Feature gate parity
    if not getattr(settings, "FEATURE_EMAIL_NOTIFICATIONS", True):
        return JsonResponse(
            {"ok": True, "skipped": True, "reason": "feature_flag_off", "method": request.method, "at": timezone.now().isoformat()},
            status=200,
        )

    try:
        # (1) Pre-generate today's recurring items (safe, idempotent)
        gen_summary = {"ok": True}
        try:
            # run() executes synchronously in the web dyno (shared DB)
            gen_summary = generate_recurring_checklists.run(dry_run=False) or {"ok": True}
        except Exception as e:
            # Do not abort the hook; log the generator failure and still attempt mailing.
            gen_summary = {"ok": False, "error": str(e)}

        # (2) Perform the 10:00 IST due-today fan-out
        mail_result = send_due_today_assignments.run()

        return JsonResponse(
            {
                "ok": True,
                "method": request.method,
                "at": timezone.now().isoformat(),
                "generated": gen_summary,
                "result": mail_result,  # includes 'skipped_before_10' when applicable
            },
            status=200,
        )

    except Exception as e:
        # Never leak raw 500 to cron; keep debugging info
        return JsonResponse(
            {
                "ok": False,
                "error_type": type(e).__name__,
                "error": str(e),
                "at": timezone.now().isoformat(),
            },
            status=200,
        )
