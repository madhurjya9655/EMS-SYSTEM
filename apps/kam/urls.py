# FILE: apps/kam/urls.py
# UPDATED: 2026-04-14 — Added customer_search_api + customer_360_api endpoints
from django.urls import path
from . import views

app_name = "kam"

urlpatterns = [
    # ── KAM Dashboard ──────────────────────────────────────────────────
    path("", views.dashboard, name="dashboard"),
    path("manager/", views.manager_dashboard, name="manager_dashboard"),
    path("manager/", views.manager_dashboard, name="manager"),  # backward-compatible alias
    path("manager/kpis/", views.manager_kpis, name="manager_kpis"),

    # ── Manager View (analytics — 5 tabs) ─────────────────────────────
    path("manager-view/", views.manager_view, name="manager_view"),

    # ── Admin ──────────────────────────────────────────────────────────
    path("admin/kam-manager-mapping/", views.admin_kam_manager_mapping, name="admin_kam_manager_mapping"),

    # ══════════════════════════════════════════════════════════════════
    # EMPLOYEE VISIT PLAN FLOW
    # ══════════════════════════════════════════════════════════════════

    # Employee: apply / list / detail / edit
    path("plan/", views.weekly_plan, name="plan"),
    path("my-visits/", views.single_visit_list, name="employee_visit_list"),
    path("my-visits/<int:plan_id>/", views.single_visit_detail, name="single_visit_detail"),
    path("my-visits/<int:plan_id>/edit/", views.single_visit_edit, name="single_visit_edit"),

    # Manager: email token approve / reject links
    path(
        "visit/approve-link/<str:token>/",
        views.single_visit_approve_link,
        name="employee_visit_approve_link",
    ),
    path(
        "visit/reject-link/<str:token>/",
        views.single_visit_reject_link,
        name="employee_visit_reject_link",
    ),

    # Manager: inline form-POST approve / reject (from manager dashboard)
    path(
        "my-visits/<int:plan_id>/approve/",
        views.single_visit_approve,
        name="single_visit_approve",
    ),
    path(
        "my-visits/<int:plan_id>/reject/",
        views.single_visit_reject,
        name="single_visit_reject",
    ),

    # Manager: team visit list
    path("team-visits/", views.single_visit_list, name="manager_visit_list"),

    # ── Backward-compat aliases for old single_visit_* names ──────────
    path("single-visits/", views.single_visit_list, name="single_visit_list"),
    path(
        "single-visit/approve/<str:token>/",
        views.single_visit_approve_link,
        name="single_visit_approve_link",
    ),
    path(
        "single-visit/reject/<str:token>/",
        views.single_visit_reject_link,
        name="single_visit_reject_link",
    ),

    # ── Visits (actual entry — kept for KAM actuals) ───────────────────
    path("visits/", views.visits, name="visits"),

    # ── Visit History / Batch (KAM batch flow — kept for existing data) ─
    path("visit-history/", views.visit_batches_page, name="visit_batches"),
    path("visit-history/api/", views.visit_batches_api, name="visit_batches_api"),
    path("visit-history/<int:batch_id>/", views.visit_batch_detail, name="visit_batch_detail"),
    path("visit-history/<int:batch_id>/delete/", views.visit_batch_delete, name="visit_batch_delete"),
    path("visit-history/<int:batch_id>/approve/", views.visit_batch_approve, name="visit_batch_approve"),
    path("visit-history/<int:batch_id>/reject/", views.visit_batch_reject, name="visit_batch_reject"),
    path("visit-history/approve-link/<str:token>/", views.visit_batch_approve_link, name="visit_batch_approve_link"),
    path("visit-history/reject-link/<str:token>/", views.visit_batch_reject_link, name="visit_batch_reject_link"),
    path("visit-history/edit/<int:plan_id>/", views.visit_history_edit, name="visit_history_edit"),

    # Legacy visit approve/reject
    path("visits/approve/<int:plan_id>/", views.visit_approve, name="visit_approve"),
    path("visits/reject/<int:plan_id>/", views.visit_reject, name="visit_reject"),

    # ── Calls ──────────────────────────────────────────────────────────
    path("calls/new/", views.call_new, name="call_new"),

    # ── Collections (quick-entry) ──────────────────────────────────────
    path("collections/new/", views.collection_new, name="collection_new"),

    # ── Collections Plan ───────────────────────────────────────────────
    path("collections-plan/", views.collections_plan, name="collections_plan"),
    path("collections-plan/delete/<int:plan_id>/", views.collection_plan_delete, name="collection_plan_delete"),
    path("collections-plan/record-actual/<int:plan_id>/", views.collection_plan_record_actual, name="collection_plan_record_actual"),

    # ── Customers ──────────────────────────────────────────────────────
    path("customers/", views.customers, name="customers"),
    path("customers/api/", views.customers_api, name="customers_api"),
    path("customers/create/", views.customer_create_manual, name="customer_create_manual"),
    path("customers/<int:customer_id>/update/", views.customer_update_manual, name="customer_update_manual"),
    path("customers/<int:customer_id>/delete/", views.customer_delete_manual, name="customer_delete_manual"),

    # ── Customer Search + 360 APIs (NEW — for Collection Plan AJAX) ────
    path("api/customer-search/", views.customer_search_api, name="customer_search_api"),
    path("api/customer-360/<int:customer_id>/", views.customer_360_api, name="customer_360_api"),

    # ── Targets ────────────────────────────────────────────────────────
    path("targets/", views.targets, name="targets"),
    path("targets/lines/", views.targets_lines, name="targets_lines"),

    # ── Reports ────────────────────────────────────────────────────────
    path("reports/", views.reports, name="reports"),
    path("reports/export-csv/", views.export_kpi_csv, name="export_kpi_csv"),

    # ── Sync ───────────────────────────────────────────────────────────
    path("sync/now/", views.sync_now, name="sync_now"),
    path("sync/trigger/", views.sync_trigger, name="sync_trigger"),
    path("sync/step/", views.sync_step, name="sync_step"),
    path("collections/update/<int:pk>/", views.update_actual_collection, name="update_actual_collection"),
    path("collections/report/", views.collection_report, name="collection_report"),
]