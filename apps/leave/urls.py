from __future__ import annotations

from django.urls import path

from . import views

app_name = "leave"

urlpatterns = [
    # Employee views
    path("", views.dashboard, name="dashboard"),
    path("my/", views.my_leaves, name="my_leaves"),
    path("apply/", views.apply_leave, name="apply_leave"),
    path("delete/<int:pk>/", views.delete_leave, name="delete_leave"),
    # Bulk delete — keep both names for backward compatibility
    path("bulk-delete/", views.bulk_delete_leaves, name="bulk_delete_leaves"),
    path("bulk-delete/", views.bulk_delete_leaves, name="bulk_delete"),  # alias to fix NoReverseMatch

    # Manager queue & actions
    path("manager/pending/", views.manager_pending, name="manager_pending"),
    path("approve/<int:pk>/", views.approval_page, name="approval_page"),
    path("approve/<int:pk>/do/", views.manager_decide_approve, name="manager_decide_approve"),
    path("reject/<int:pk>/do/", views.manager_decide_reject, name="manager_decide_reject"),

    # One-click token decision (email links)
    path("t/<str:token>/", views.TokenDecisionView.as_view(), name="leave_action_via_token"),

    # Profile photo upload
    path("upload-photo/", views.upload_photo, name="upload_photo"),

    # Lightweight widget (for dashboards)
    path("manager/widget/", views.manager_widget, name="manager_widget"),

    # Approver mapping editor (admin)
    path("approver/<int:user_id>/", views.approver_mapping_edit, name="approver_mapping_edit"),
    path("approver/save/", views.approver_mapping_save, name="approver_mapping_save"),
    path("approver/<int:user_id>/field/<str:field>/", views.approver_mapping_edit_field, name="approver_mapping_edit_field"),
    path("approver/<int:user_id>/reporting/", views.approver_mapping_edit_reporting, name="approver_mapping_edit_reporting"),
    path("approver/<int:user_id>/cc/", views.approver_mapping_edit_cc, name="approver_mapping_edit_cc"),

    # CC configuration & assignment (admin)
    path("cc/config/", views.cc_config, name="cc_config"),
    path("cc/config/add/", views.cc_config_add, name="cc_config_add"),
    path("cc/config/<int:pk>/remove/", views.cc_config_remove, name="cc_config_remove"),
    path("cc/assign/", views.cc_assign, name="cc_assign"),
]
