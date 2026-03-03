# FILE: apps/kam/urls.py
# PURPOSE: KAM module URL configuration
# UPDATED: 2026-03-03 — Added backward-compatible manager alias and collection_plan_delete route

from django.urls import path
from . import views

app_name = "kam"

urlpatterns = [
    # ── Dashboard ──────────────────────────────────────────────────────
    path("", views.dashboard, name="dashboard"),
    path("manager/", views.manager_dashboard, name="manager_dashboard"),
    path("manager/", views.manager_dashboard, name="manager"),  # backward-compatible alias
    path("manager/kpis/", views.manager_kpis, name="manager_kpis"),

    # ── Admin ──────────────────────────────────────────────────────────
    path(
        "admin/kam-manager-mapping/",
        views.admin_kam_manager_mapping,
        name="admin_kam_manager_mapping",
    ),

    # ── Visits ─────────────────────────────────────────────────────────
    path("plan/", views.weekly_plan, name="plan"),
    path("visits/", views.visits, name="visits"),

    path("visit-batches/", views.visit_batches_page, name="visit_batches"),
    path("visit-batches/api/", views.visit_batches_api, name="visit_batches_api"),
    path("visit-batches/<int:batch_id>/", views.visit_batch_detail, name="visit_batch_detail"),
    path("visit-batches/<int:batch_id>/delete/", views.visit_batch_delete, name="visit_batch_delete"),
    path("visit-batches/<int:batch_id>/approve/", views.visit_batch_approve, name="visit_batch_approve"),
    path("visit-batches/<int:batch_id>/reject/", views.visit_batch_reject, name="visit_batch_reject"),
    path(
        "visit-batches/approve-link/<str:token>/",
        views.visit_batch_approve_link,
        name="visit_batch_approve_link",
    ),
    path(
        "visit-batches/reject-link/<str:token>/",
        views.visit_batch_reject_link,
        name="visit_batch_reject_link",
    ),

    path("visits/approve/<int:plan_id>/", views.visit_approve, name="visit_approve"),
    path("visits/reject/<int:plan_id>/", views.visit_reject, name="visit_reject"),

    # ── Calls ──────────────────────────────────────────────────────────
    path("calls/new/", views.call_new, name="call_new"),

    # ── Collections ────────────────────────────────────────────────────
    path("collections/new/", views.collection_new, name="collection_new"),

    # ── Collections Plan ───────────────────────────────────────────────
    path("collections-plan/", views.collections_plan, name="collections_plan"),
    path(
        "collections-plan/delete/<int:plan_id>/",
        views.collection_plan_delete,
        name="collection_plan_delete",
    ),

    # ── Customers ──────────────────────────────────────────────────────
    path("customers/", views.customers, name="customers"),
    path("customers/api/", views.customers_api, name="customers_api"),
    path("customers/create/", views.customer_create_manual, name="customer_create_manual"),
    path(
        "customers/<int:customer_id>/update/",
        views.customer_update_manual,
        name="customer_update_manual",
    ),
    path(
        "customers/<int:customer_id>/delete/",
        views.customer_delete_manual,
        name="customer_delete_manual",
    ),

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
]