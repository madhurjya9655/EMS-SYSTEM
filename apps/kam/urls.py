# FILE: apps/kam/urls.py

from django.urls import path
from . import views

app_name = "kam"

urlpatterns = [
    # ---------------------------------------------------------------------
    # Dashboards
    # ---------------------------------------------------------------------
    path("dashboard/", views.dashboard, name="dashboard"),
    path("manager/", views.manager_dashboard, name="manager"),
    path("manager/kpis/", views.manager_kpis, name="manager_kpis"),

    # ---------------------------------------------------------------------
    # Visit planning / execution
    # ---------------------------------------------------------------------
    path("plan/", views.weekly_plan, name="plan"),
    path("visits/", views.visits, name="visits"),

    # Legacy per-plan approve/reject (kept; batch approval is primary now)
    path("visit/<int:plan_id>/approve/", views.visit_approve, name="visit_approve"),
    path("visit/<int:plan_id>/reject/", views.visit_reject, name="visit_reject"),

    # ---------------------------------------------------------------------
    # Batch listing
    # ---------------------------------------------------------------------
    path("batches/", views.visit_batches, name="visit_batches"),
    path("batches/<int:batch_id>/", views.visit_batch_detail, name="visit_batch_detail"),

    # Draft delete (KAM only; server enforces status/owner)
    path("batches/<int:batch_id>/delete/", views.visit_batch_delete, name="visit_batch_delete"),

    # Manager approval actions (dashboard buttons -> POST)
    path("batches/<int:batch_id>/approve/", views.visit_batch_approve, name="visit_batch_approve"),
    path("batches/<int:batch_id>/reject/", views.visit_batch_reject, name="visit_batch_reject"),

    # Secure email links (GET -> login -> approve/reject)
    path("batches/approve/<str:token>/", views.visit_batch_approve_link, name="visit_batch_approve_link"),
    path("batches/reject/<str:token>/", views.visit_batch_reject_link, name="visit_batch_reject_link"),

    # ---------------------------------------------------------------------
    # Customer APIs for redesigned Plan Visit (checkbox table + manual CRUD)
    # ---------------------------------------------------------------------
    path("api/customers/", views.customers_api, name="customers_api"),
    path("api/customers/manual/create/", views.customer_create_manual, name="customer_create_manual"),
    path("api/customers/manual/<int:customer_id>/update/", views.customer_update_manual, name="customer_update_manual"),
    path("api/customers/manual/<int:customer_id>/delete/", views.customer_delete_manual, name="customer_delete_manual"),

    # ---------------------------------------------------------------------
    # Admin-only: KAM → Manager Mapping (routing for approval emails)
    # ---------------------------------------------------------------------
    path("admin/kam-manager-mapping/", views.admin_kam_manager_mapping, name="admin_kam_manager_mapping"),

    # ---------------------------------------------------------------------
    # Quick entries
    # ---------------------------------------------------------------------
    path("call/new/", views.call_new, name="call_new"),
    path("collection/new/", views.collection_new, name="collection_new"),

    # ---------------------------------------------------------------------
    # Customer 360
    # ---------------------------------------------------------------------
    path("customers/", views.customers, name="customers"),

    # ---------------------------------------------------------------------
    # Targets
    # ---------------------------------------------------------------------
    path("targets/", views.targets, name="targets"),
    path("targets/lines/", views.targets_lines, name="targets_lines"),

    # ---------------------------------------------------------------------
    # Reports / Plans
    # ---------------------------------------------------------------------
    path("reports/", views.reports, name="reports"),
    path("collections/plan/", views.collections_plan, name="collections_plan"),
    path("export/kpi.csv", views.export_kpi_csv, name="export_kpi_csv"),

    # ---------------------------------------------------------------------
    # Sync (UNCHANGED)
    # ---------------------------------------------------------------------
    path("sync/now/", views.sync_now, name="sync_now"),
    path("sync/trigger/", views.sync_trigger, name="sync_trigger"),
    path("sync/step/", views.sync_step, name="sync_step"),
]