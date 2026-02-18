from django.urls import path
from . import views

app_name = "kam"

urlpatterns = [
    # Dashboards
    path("dashboard/", views.dashboard, name="dashboard"),
    path("manager/", views.manager_dashboard, name="manager"),
    path("manager/kpis/", views.manager_kpis, name="manager_kpis"),

    # Visit planning / execution
    path("plan/", views.weekly_plan, name="plan"),
    path("visits/", views.visits, name="visits"),
    path("visit/<int:plan_id>/approve/", views.visit_approve, name="visit_approve"),
    path("visit/<int:plan_id>/reject/", views.visit_reject, name="visit_reject"),

    # B4: Manager/Admin view batches (JSON for now; template later)
    path("batches/", views.visit_batches, name="visit_batches"),

    # Quick entries
    path("call/new/", views.call_new, name="call_new"),
    path("collection/new/", views.collection_new, name="collection_new"),

    # Customer 360
    path("customers/", views.customers, name="customers"),

    # Targets
    path("targets/", views.targets, name="targets"),
    path("targets/lines/", views.targets_lines, name="targets_lines"),

    # Reports / Plans
    path("reports/", views.reports, name="reports"),
    path("collections/plan/", views.collections_plan, name="collections_plan"),
    path("export/kpi.csv", views.export_kpi_csv, name="export_kpi_csv"),

    # Sync
    path("sync/now/", views.sync_now, name="sync_now"),
    path("sync/trigger/", views.sync_trigger, name="sync_trigger"),
    path("sync/step/", views.sync_step, name="sync_step"),
]
