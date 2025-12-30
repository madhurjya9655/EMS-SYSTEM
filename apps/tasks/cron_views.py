# apps/tasks/cron_views.py
from django.http import JsonResponse, HttpResponseForbidden
from django.conf import settings

from .tasks import (
    send_due_today_assignments,
    send_daily_pending_task_summary,
)
from .pending_digest import (
    send_daily_employee_pending_digest,
    send_admin_all_pending_digest,
)


def _authorized(request) -> bool:
    """
    Very simple protection for cron endpoints.
    Accept either:
      • ?key=<CRON_SECRET>
      • X-Cron-Key: <CRON_SECRET>
      • (lenient) ?token=<CRON_SECRET> or X-CRON-TOKEN: <CRON_SECRET>  (kept for parity with other hook)
    """
    key = (
        request.GET.get("key")
        or request.headers.get("X-Cron-Key")
        or request.GET.get("token")
        or request.headers.get("X-CRON-TOKEN")
    )
    expected = getattr(settings, "CRON_SECRET", "") or ""
    return True if not expected else (key == expected)


def due_today(request):
    """
    10:00 IST fan-out (safe at any time; task returns skip-before-10 flag).
    GET /internal/cron/due-today/?key=...

    HARDENED:
      • Never raises to caller.
      • Always returns JSON (status 200), even on internal error.
      • Preserves auth semantics you already use (?key=... / X-Cron-Key).
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        # Call the synchronous task runner (not the Celery async delay).
        # This function itself guards "before 10 AM" and uses robust IST handling.
        result = send_due_today_assignments.run()

        # Normal success JSON
        return JsonResponse(
            {
                "ok": True,
                "triggered": True,
                "result": result,
            },
            status=200,
        )
    except Exception as e:
        # IMPORTANT: return JSON status 200 so your Render cron doesn’t log HTTP 500.
        # Error details are still visible in app logs and in this JSON.
        return JsonResponse(
            {
                "ok": False,
                "triggered": False,
                "error_type": type(e).__name__,
                "error": str(e),
            },
            status=200,
        )


def pending_summary_7pm(request):
    """
    19:00 IST consolidated summaries:
      - One email to admin (pankaj@blueoceansteels.com) with ALL pending.
      - One email per employee with ONLY their own pending (single mail per user).
    GET /internal/cron/pending-7pm/?key=...

    (Left functionally identical; wrapped in try/except + JSON 200 on error for parity.)
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        admin_to = "pankaj@blueoceansteels.com"
        admin = send_admin_all_pending_digest.run(to=admin_to, force=True)
        employees = send_daily_employee_pending_digest.run(force=True)
        return JsonResponse({"ok": True, "admin": admin, "employees": employees}, status=200)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )


def employee_digest(request):
    """
    Manual trigger for a single user:
    GET /internal/cron/employee-digest/?key=...&username=<uname>&to=<override-email>

    (Hardened to never raise; JSON 200 on error.)
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        username = request.GET.get("username")
        to_override = request.GET.get("to")
        res = send_daily_employee_pending_digest.run(
            force=True,
            username=username,
            to_override=to_override,
        )
        return JsonResponse({"ok": True, "result": res}, status=200)
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )
