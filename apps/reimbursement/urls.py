# apps/reimbursement/urls.py
from __future__ import annotations

from django.urls import path

from . import views

app_name = "reimbursement"

urlpatterns = [
    # ------------------------------------------------------------------
    # Employee: Upload expenses (upload form) + edit/delete
    # ------------------------------------------------------------------
    path(
        "expenses/",
        views.ExpenseInboxView.as_view(),
        name="expense_inbox",
    ),
    path(
        "expenses/<int:pk>/edit/",
        views.ExpenseItemUpdateView.as_view(),
        name="expense_edit",
    ),
    path(
        "expenses/<int:pk>/delete/",
        views.ExpenseItemDeleteView.as_view(),
        name="expense_delete",
    ),

    # ------------------------------------------------------------------
    # Employee: Reimbursement requests
    # ------------------------------------------------------------------
    path(
        "apply/",
        views.ReimbursementCreateView.as_view(),
        name="apply_reimbursement",
    ),
    path(
        "create/",
        views.ReimbursementCreateView.as_view(),
        name="create_request",
    ),
    path(
        "my/",
        views.MyReimbursementsView.as_view(),
        name="my_reimbursements",
    ),
    # Bulk delete from "My Requests"
    path(
        "bulk-delete/",
        views.ReimbursementBulkDeleteView.as_view(),
        name="bulk_delete",
    ),
    # Edit & delete single request
    path(
        "request/<int:pk>/edit/",
        views.ReimbursementRequestUpdateView.as_view(),
        name="request_edit",
    ),
    path(
        "request/<int:pk>/delete/",
        views.ReimbursementRequestDeleteView.as_view(),
        name="request_delete",
    ),
    path(
        "<int:pk>/",
        views.ReimbursementDetailView.as_view(),
        name="reimbursement_detail",
    ),

    # ------------------------------------------------------------------
    # Manager / Management / Finance queues
    # ------------------------------------------------------------------
    path(
        "manager/",
        views.ManagerQueueView.as_view(),
        name="manager_pending",
    ),
    path(
        "manager/<int:pk>/review/",
        views.ManagerReviewView.as_view(),
        name="manager_review",
    ),

    path(
        "management/",
        views.ManagementQueueView.as_view(),
        name="management_queue",
    ),
    path(
        "management/<int:pk>/review/",
        views.ManagementReviewView.as_view(),
        name="management_review",
    ),

    path(
        "finance/",
        views.FinanceQueueView.as_view(),
        name="finance_pending",
    ),
    # explicit verify route (pre-manager step)
    path(
        "finance/<int:pk>/verify/",
        views.FinanceVerifyView.as_view(),
        name="finance_verify",
    ),
    path(
        "finance/<int:pk>/review/",
        views.FinanceReviewView.as_view(),
        name="finance_review",
    ),

    # ------------------------------------------------------------------
    # Admin dashboards
    # ------------------------------------------------------------------
    path(
        "admin/bills/",
        views.AdminBillsSummaryView.as_view(),
        name="admin_bills_summary",
    ),
    path(
        "admin/requests/",
        views.AdminRequestsListView.as_view(),
        name="admin_requests",
    ),
    path(
        "admin/employee-summary/",
        views.AdminEmployeeSummaryView.as_view(),
        name="admin_employee_summary",
    ),
    path(
        "admin/status-summary/",
        views.AdminStatusSummaryView.as_view(),
        name="admin_status_summary",
    ),
    path(
        "admin/approver-mapping/",
        views.ApproverMappingAdminView.as_view(),
        name="approver_mapping_admin",
    ),

    # ------------------------------------------------------------------
    # Secure receipt download
    # ------------------------------------------------------------------
    path(
        "receipt/line/<int:line_id>/",
        views.download_receipt,
        name="receipt_line",
    ),
    path(
        "receipt/expense/<int:expense_id>/",
        views.download_receipt,
        name="receipt_expense",
    ),

    # ------------------------------------------------------------------
    # Magic-link email actions (Approve / Reject buttons)
    # ------------------------------------------------------------------
    path(
        "email-action/",
        views.reimbursement_email_action,
        name="email_action",
    ),

    # ------------------------------------------------------------------
    # Legacy flows (keep old links working)
    # ------------------------------------------------------------------
    path(
        "legacy/apply/",
        views.LegacyReimbursementCreateView.as_view(),
        name="legacy_apply",
    ),
    path(
        "legacy/my/",
        views.LegacyMyReimbursementsView.as_view(),
        name="legacy_my_reimbursements",
    ),
    path(
        "legacy/manager/",
        views.LegacyManagerPendingView.as_view(),
        name="legacy_manager_pending",
    ),
    path(
        "legacy/manager/<int:pk>/review/",
        views.LegacyManagerReviewView.as_view(),
        name="legacy_manager_review",
    ),
    path(
        "legacy/finance/",
        views.LegacyFinancePendingView.as_view(),
        name="legacy_finance_pending",
    ),
    path(
        "legacy/finance/<int:pk>/review/",
        views.LegacyFinanceReviewView.as_view(),
        name="legacy_finance_review",
    ),
]
