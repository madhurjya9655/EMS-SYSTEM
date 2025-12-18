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
    Call with ?key=<CRON_SECRET> or header X-Cron-Key: <CRON_SECRET>.
    """
    key = request.GET.get("key") or request.headers.get("X-Cron-Key")
    return bool(key) and key == getattr(settings, "CRON_SECRET", "")


def due_today(request):
    """
    10:00 IST fan-out (safe at any time; will skip before 10:00).
    GET /internal/cron/due-today/?key=...
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")
    res = send_due_today_assignments.run()
    return JsonResponse(res)


def pending_summary_7pm(request):
    """
    19:00 IST consolidated summaries:
      - One email to admin (pankaj@blueoceansteels.com) with ALL pending.
      - One email per employee with ONLY their own pending (single mail per user).
    GET /internal/cron/pending-7pm/?key=...
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    admin_to = "pankaj@blueoceansteels.com"
    admin = send_admin_all_pending_digest.run(to=admin_to, force=True)
    employees = send_daily_employee_pending_digest.run(force=True)
    return JsonResponse({"admin": admin, "employees": employees})


def employee_digest(request):
    """
    Manual trigger for a single user:
    GET /internal/cron/employee-digest/?key=...&username=<uname>&to=<override-email>
    """
    if not _authorized(request):
        return HttpResponseForbidden("Forbidden")

    username = request.GET.get("username")
    to_override = request.GET.get("to")
    res = send_daily_employee_pending_digest.run(
        force=True,
        username=username,
        to_override=to_override,
    )
    return JsonResponse(res)
