# apps/leave/urls.py
from __future__ import annotations

from django.urls import path, re_path
from django.views.generic import RedirectView

from . import views

app_name = "leave"

urlpatterns = [
    # Primary dashboard + profile photo upload
    path("dashboard/", views.dashboard, name="dashboard"),
    path("dashboard/photo/upload/", views.upload_photo, name="upload_photo"),

    # Backward-compat: /leave/my → dashboard
    path("my/", RedirectView.as_view(pattern_name="leave:dashboard", permanent=False), name="my_leaves"),

    # Apply Leave (renders templates/leave/apply_leave.html)
    path("apply/", views.apply_leave, name="apply_leave"),

    # Delete functionality
    path("delete/<int:pk>/", views.delete_leave, name="delete_leave"),
    path("bulk-delete/", views.bulk_delete_leaves, name="bulk_delete"),

    # Approval page + decisions
    path("approve/<int:pk>/", views.approval_page, name="approval_page"),
    re_path(r"^manager/pending/?$", views.manager_pending, name="manager_pending"),
    re_path(r"^manager/decide/(?P<pk>\d+)/approve/?$", views.manager_decide_approve, name="manager_decide_approve"),
    re_path(r"^manager/decide/(?P<pk>\d+)/reject/?$", views.manager_decide_reject, name="manager_decide_reject"),

    # One-click email decision (token)
    re_path(r"^action/(?P<token>[^/]+)/?$", views.TokenDecisionView.as_view(), name="leave_action_via_token"),
    re_path(r"^t/(?P<token>[^/]+)/?$", views.TokenDecisionView.as_view(), name="token_decide"),

    # Optional manager widget
    path("manager/widget/", views.manager_widget, name="manager_widget"),

    # Approver Mapping editor (Admin controls routing)
    path("approver-mapping/<int:user_id>/", views.approver_mapping_edit, name="approver_mapping_edit"),
    path("approver-mapping/<int:user_id>/edit/reporting/", views.approver_mapping_edit_reporting, name="approver_mapping_edit_reporting"),
    path("approver-mapping/<int:user_id>/edit/cc/", views.approver_mapping_edit_cc, name="approver_mapping_edit_cc"),
    path("approver-mapping/save/", views.approver_mapping_save, name="approver_mapping_save"),
]
