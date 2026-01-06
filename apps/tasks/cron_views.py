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

    If settings.CRON_SECRET is blank/missing, allow all (useful for dev).
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
      • Preserves auth semantics (?key=... / X-Cron-Key / token variants).
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    try:
        # Call the synchronous task runner (not the Celery async delay).
        # This function itself guards "before 10 AM" (IST) and handles TZ.
        result = send_due_today_assignments.run()

        return JsonResponse(
            {
                "ok": True,
                "triggered": True,
                "result": result,
            },
            status=200,
        )
    except Exception as e:
        # IMPORTANT: render cron shouldn't see 500s -> always JSON 200 with error payload
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

    Safe: try/except + JSON 200 on error for parity.
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

    Safe: never raises; JSON 200 on error.
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


# ----------------------------
# Backward-compatibility shims
# ----------------------------
# Some existing URLs/places may still import hooks from views_cron.* or expect
# names like "due_today_assignments_hook". These aliases let them call into the
# hardened endpoints here without changing call-sites.

def due_today_assignments_hook(request):
    """Alias to hardened due_today() for legacy routes."""
    return due_today(request)

def pending_summary_hook(request):
    """Alias to hardened pending_summary_7pm() for any legacy hook name."""
    return pending_summary_7pm(request)

def employee_digest_hook(request):
    """Alias to hardened employee_digest() for any legacy hook name."""
    return employee_digest(request)
