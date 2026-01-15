# employee_management/urls.py
from __future__ import annotations

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.http import HttpResponse
from django.urls import include, path, re_path
from django.views.decorators.http import require_GET
from django.views.generic import RedirectView

# Custom login view
from apps.users.views import CustomLoginView

# ✅ Cron endpoints
# Keep old module for 7PM/admin digests…
from apps.tasks import cron_views as legacy_cron_views
# …but switch 10:00 AM fan-out to the hardened hook
from apps.tasks import views_cron as new_cron_views

# Admin titles
admin.site.site_header = "EMS Admin"
admin.site.index_title = "Administration"
admin.site.site_title = "EMS Admin"


# ---------------------------------------------------------------------
# Healthcheck
# ---------------------------------------------------------------------
@require_GET
def healthcheck(_request):
    return HttpResponse("ok", content_type="text/plain")


# Minimal robots.txt
@require_GET
def robots_txt(_request):
    return HttpResponse("User-agent: *\nDisallow:\n", content_type="text/plain")


# ---------------------------------------------------------------------
# URL patterns
# ---------------------------------------------------------------------
urlpatterns = [
    # Admin (hardened path from settings.ADMIN_URL)
    path(settings.ADMIN_URL, admin.site.urls),

    # Auth
    path("accounts/login/", CustomLoginView.as_view(), name="login"),
    path("accounts/", include("django.contrib.auth.urls")),

    # Apps
    path("leave/",         include(("apps.leave.urls",         "leave"),         namespace="leave")),
    path("petty_cash/",    include(("apps.petty_cash.urls",    "petty_cash"),    namespace="petty_cash")),
    path("sales/",         include(("apps.sales.urls",         "sales"),         namespace="sales")),
    path("reimbursement/", include(("apps.reimbursement.urls", "reimbursement"), namespace="reimbursement")),
    path("tasks/",         include(("apps.tasks.urls",         "tasks"),         namespace="tasks")),
    path("reports/",       include(("apps.reports.urls",       "reports"),       namespace="reports")),
    path("users/",         include(("apps.users.urls",         "users"),         namespace="users")),
    path("dashboard/",     include(("dashboard.urls",          "dashboard"),     namespace="dashboard")),
    path("settings/",      include(("apps.settings.urls",      "settings"),      namespace="settings")),

    # Recruitment under its own prefix
    path("recruitment/",   include(("apps.recruitment.urls",   "recruitment"),   namespace="recruitment")),

    # Root → redirect to dashboard
    path("", RedirectView.as_view(pattern_name="dashboard:home", permanent=False), name="site-root"),

    # Healthcheck aliases
    path("up",  healthcheck, name="healthcheck-no-slash"),
    path("up/", healthcheck, name="healthcheck"),
    path("healthz", healthcheck),
    path("healthz/", healthcheck),

    # Robots + favicon helpers
    path("robots.txt", robots_txt),
    re_path(r"^favicon\.ico$", RedirectView.as_view(url=f"{settings.STATIC_URL}favicon.ico", permanent=False)),

    # ✅ Internal cron endpoints (protected by CRON_SECRET)
    # 10:00 AM fan-out — now calls the hardened view that pre-generates “today” and never 500s
    path("internal/cron/due-today/", new_cron_views.due_today_assignments_hook, name="cron-due-today"),

    # Keep your original cron views for the rest (unchanged behavior)
    path("internal/cron/pending-7pm/", legacy_cron_views.pending_summary_7pm, name="cron-pending-7pm"),
    path("internal/cron/employee-digest/", legacy_cron_views.employee_digest, name="cron-employee-digest"),
]

# Serve media in DEBUG (and optionally when SERVE_MEDIA is enabled from settings)
if settings.DEBUG or getattr(settings, "SERVE_MEDIA", False):
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

# Optional: Django Debug Toolbar
if "debug_toolbar" in settings.INSTALLED_APPS:
    urlpatterns = [path("__debug__/", include("debug_toolbar.urls"))] + urlpatterns


# ---------------------------------------------------------------------
# Minimal, safe error handlers (plain text; swap to templates later)
# ---------------------------------------------------------------------
def _plain(status: int, msg: str):
    return HttpResponse(msg, content_type="text/plain", status=status)

def bad_request(request, exception=None):        # 400
    return _plain(400, "Bad Request")

def permission_denied(request, exception=None):  # 403
    return _plain(403, "Permission Denied")

def page_not_found(request, exception=None):     # 404
    return _plain(404, "Page Not Found")

def server_error(request):                       # 500
    return _plain(500, "Server Error")

handler400 = bad_request
handler403 = permission_denied
handler404 = page_not_found
handler500 = server_error
