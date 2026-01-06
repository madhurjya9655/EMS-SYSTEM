# apps/tasks/cron_views.py
from django.http import JsonResponse, HttpResponseForbidden
from django.conf import settings

from .tasks import (
    send_due_today_assignments,
)
from .pending_digest import (
    send_daily_employee_pending_digest,
    send_admin_all_pending_digest,
)


def _authorized(request) -> bool:
    """
    Accept either:
      • ?key=<CRON_SECRET>
      • X-Cron-Key: <CRON_SECRET>
      • (lenient) ?token=<CRON_SECRET> or X-CRON-TOKEN: <CRON_SECRET> (back-compat)

    If no CRON_SECRET is defined in settings, allow (useful for local/dev).
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
    10:00 IST fan-out (safe at any time; task guards pre-10:00 IST itself).
    GET /internal/cron/due-today/?key=...   OR send header X-Cron-Key: <secret>

    HARDENED:
      • Never raises to caller.
      • Always returns JSON (status 200), even on internal error.
      • Preserves simple key/header auth semantics.
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        # Synchronous run (not .delay); the task decides whether to skip.
        result = send_due_today_assignments.run()
        return JsonResponse({"ok": True, "triggered": True, "result": result}, status=200)
    except Exception as e:
        # Keep HTTP 200 so Render Cron doesn't mark it as failed;
        # details still appear in logs and JSON.
        return JsonResponse(
            {"ok": False, "triggered": False, "error_type": type(e).__name__, "error": str(e)},
            status=200,
        )


def pending_summary_7pm(request):
    """
    19:00 IST consolidated summaries:
      - Admin mail with ALL pending.
      - One mail per employee with ONLY their pending.
    GET /internal/cron/pending-7pm/?key=...
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
