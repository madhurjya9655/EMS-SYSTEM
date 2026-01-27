from __future__ import annotations

from django.urls import path

from . import views
from . import views_analytics
from . import views_attach  # << NEW

app_name = "reimbursement"

# Lazy loader to avoid import-time issues for the admin view
def approver_mapping_admin_view(*args, **kwargs):
    from .views import ApproverMappingAdminView
    return ApproverMappingAdminView.as_view()(*args, **kwargs)

urlpatterns = [
    # ------------------------------
    # Employee: Expenses (CRUD)
    # ------------------------------
    path("expenses/", views.ExpenseInboxView.as_view(), name="expense_inbox"),
    path("expenses/<int:pk>/edit/", views.ExpenseItemUpdateView.as_view(), name="expense_edit"),
    path("expenses/<int:pk>/resubmit/", views.ExpenseItemResubmitView.as_view(), name="expense_resubmit_finance"),
    path("expenses/<int:pk>/delete/", views.ExpenseItemDeleteView.as_view(), name="expense_delete"),

    # ------------------------------
    # Employee: Requests
    # ------------------------------
    path("apply/", views.ReimbursementCreateView.as_view(), name="apply_reimbursement"),
    path("create/", views.ReimbursementCreateView.as_view(), name="create_request"),
    path("my/", views.MyReimbursementsView.as_view(), name="my_reimbursements"),
    path("bulk-delete/", views.ReimbursementBulkDeleteView.as_view(), name="bulk_delete"),
    path("request/<int:pk>/edit/", views.ReimbursementRequestUpdateView.as_view(), name="request_edit"),
    path("request/<int:pk>/delete/", views.ReimbursementRequestDeleteView.as_view(), name="request_delete"),
    path("request/<int:pk>/resubmit/", views.ReimbursementResubmitView.as_view(), name="request_resubmit"),

    # Canonical request detail
    path("request/<int:pk>/", views.ReimbursementDetailView.as_view(), name="request_detail"),

    # ✅ BACKWARD COMPATIBILITY (ADMIN / OLD TEMPLATES) – kept intentionally
    path(
        "request/<int:pk>/",
        views.ReimbursementDetailView.as_view(),
        name="reimbursement_detail",
    ),

    # ------------------------------
    # Manager / Management (REQUEST-LEVEL ONLY)
    # ------------------------------
    path("manager/", views.ManagerQueueView.as_view(), name="manager_pending"),
    path("manager/<int:pk>/review/", views.ManagerReviewView.as_view(), name="manager_review"),
    path("management/", views.ManagementQueueView.as_view(), name="management_pending"),
    path("management/<int:pk>/review/", views.ManagementReviewView.as_view(), name="management_review"),

    # ------------------------------
    # Finance
    # ------------------------------
    path("finance/", views.FinanceQueueView.as_view(), name="finance_pending"),
    path("finance/<int:pk>/verify/", views.FinanceVerifyView.as_view(), name="finance_verify"),
    path("finance/<int:pk>/review/", views.FinanceReviewView.as_view(), name="finance_review"),
    # ✅ NEW: Finance settlement queue (manager/management approved → ready to pay)
    path("finance/settlement/", views.FinanceSettlementQueueView.as_view(), name="finance_settlement"),
    path("finance/<int:pk>/delete/", views.FinanceDeleteRequestView.as_view(), name="finance_delete"),

    # ✅ NEW: attach missing receipt to a bill line
    path("finance/line/<int:pk>/attach/", views_attach.FinanceAttachReceiptView.as_view(), name="finance_attach_receipt"),

    # ✅ NEW: Rejected Bills Queue (resubmitted bills only)
    path("finance/rejected-bills/", views.FinanceRejectedBillsQueueView.as_view(), name="finance_rejected_bills_queue"),

    # ------------------------------
    # Admin dashboards / export
    # ------------------------------
    path("admin/bills/", views.AdminBillsSummaryView.as_view(), name="admin_bills_summary"),
    path("admin/requests/", views.AdminRequestsListView.as_view(), name="admin_requests"),
    path("admin/employee-summary/", views.AdminEmployeeSummaryView.as_view(), name="admin_employee_summary"),
    path("admin/status-summary/", views.AdminStatusSummaryView.as_view(), name="admin_status_summary"),
    path("admin/approver-mapping/", approver_mapping_admin_view, name="approver_mapping_admin"),

    # Export
    path("admin/export.csv", views.ReimbursementExportCSVView.as_view(), name="admin_export"),
    path("admin/export.csv", views.ReimbursementExportCSVView.as_view(), name="admin_export_csv"),

    # ------------------------------
    # Analytics (Dashboard + APIs)
    # ------------------------------
    path("analytics/", views_analytics.AnalyticsDashboardView.as_view(), name="analytics_dashboard"),
    path("analytics/api/summary/", views_analytics.AnalyticsSummaryAPI.as_view(), name="analytics_api_summary"),
    path("analytics/api/timeseries/", views_analytics.AnalyticsTimeSeriesAPI.as_view(), name="analytics_api_timeseries"),
    path("analytics/api/categories/", views_analytics.AnalyticsCategoryAPI.as_view(), name="analytics_api_categories"),
    path("analytics/api/employees/", views_analytics.AnalyticsEmployeeAPI.as_view(), name="analytics_api_employees"),
    path("analytics/api/employees/options/", views_analytics.EmployeeOptionsAPI.as_view(), name="analytics_api_employee_options"),
    path("analytics/api/bills/", views_analytics.BillwiseTableAPI.as_view(), name="analytics_api_bills"),
    path("analytics/api/realtime/", views_analytics.AnalyticsRealtimeNumbersAPI.as_view(), name="analytics_api_realtime"),

    # ------------------------------
    # Secure receipt download
    # ------------------------------
    path("receipt/line/<int:line_id>/", views.download_receipt, name="receipt_line"),
    path("receipt/expense/<int:expense_id>/", views.download_receipt, name="receipt_expense"),

    # ------------------------------
    # Magic-link email actions
    # ------------------------------
    path("email-action/", views.reimbursement_email_action, name="email_action"),

    # ------------------------------
    # Legacy routes (intentionally kept)
    # ------------------------------
    path("legacy/apply/", views.LegacyReimbursementCreateView.as_view(), name="legacy_apply"),
    path("legacy/my/", views.LegacyMyReimbursementsView.as_view(), name="legacy_my_reimbursements"),
    path("legacy/manager/", views.LegacyManagerPendingView.as_view(), name="legacy_manager_pending"),
    path("legacy/manager/<int:pk>/review/", views.LegacyManagerReviewView.as_view(), name="legacy_manager_review"),
    path("legacy/finance/", views.LegacyFinancePendingView.as_view(), name="legacy_finance_pending"),
    path("legacy/finance/<int:pk>/review/", views.LegacyFinanceReviewView.as_view(), name="legacy_finance_review"),
]
