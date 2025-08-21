import os
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.http import HttpResponse
from apps.users.views import CustomLoginView

def healthcheck(_):
    return HttpResponse("ok", content_type="text/plain")

urlpatterns = [
    path("admin/", admin.site.urls),

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

    # Settings (namespaced)
    path("settings/", include(("apps.settings.urls", "settings"), namespace="settings")),

    # Root → recruitment app
    path("", include(("apps.recruitment.urls", "recruitment"), namespace="recruitment")),

    # Healthcheck
    path("up", healthcheck),
]

# Serve media in DEBUG; optionally in prod if SERVE_MEDIA=1 (useful on Render)
if settings.DEBUG or os.getenv("SERVE_MEDIA") == "1":
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
