#apps\leave\urls.py
from __future__ import annotations

from django.urls import path

from . import admin_views
from . import views

app_name = "leave"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("my/", views.my_leaves, name="my_leaves"),
    path("apply/", views.apply_leave, name="apply_leave"),
    path("edit/<int:pk>/", views.edit_leave, name="edit_leave"),
    path("delete/<int:pk>/", views.delete_leave, name="delete_leave"),
    path("bulk-delete/", views.bulk_delete_leaves, name="bulk_delete_leaves"),
    path("bulk_delete/", views.bulk_delete_leaves, name="bulk_delete"),
    path("manager/pending/", views.manager_pending, name="manager_pending"),
    path("approve/<int:pk>/", views.approval_page, name="approval_page"),
    path("approve/<int:pk>/do/", views.manager_decide_approve, name="manager_decide_approve"),
    path("reject/<int:pk>/do/", views.manager_decide_reject, name="manager_decide_reject"),
    path("t/<str:token>/", views.TokenDecisionView.as_view(), name="leave_action_via_token"),
    path("upload-photo/", views.upload_photo, name="upload_photo"),
    path("manager/widget/", views.manager_widget, name="manager_widget"),
    path("widgets/my-handovers/", views.my_handovers_widget, name="my_handovers_widget"),
    path("approver/<int:user_id>/", views.approver_mapping_edit, name="approver_mapping_edit"),
    path("approver/save/", views.approver_mapping_save, name="approver_mapping_save"),
    path("approver/<int:user_id>/field/<str:field>/", views.approver_mapping_edit_field, name="approver_mapping_edit_field"),
    path("approver/<int:user_id>/reporting/", views.approver_mapping_edit_reporting, name="approver_mapping_edit_reporting"),
    path("approver/<int:user_id>/cc/", views.approver_mapping_edit_cc, name="approver_mapping_edit_cc"),
    path("cc/config/", views.cc_config, name="cc_config"),
    path("cc/config/add/", views.cc_config_add, name="cc_config_add"),
    path("cc/config/<int:pk>/remove/", views.cc_config_remove, name="cc_config_remove"),
    path("cc/assign/", views.cc_assign, name="cc_assign"),
    path("admin/email-settings/", views.leave_email_settings, name="leave_email_settings"),
    path("admin/edit/<int:pk>/", admin_views.admin_edit_leave, name="admin_edit_leave"),
    path("admin/edit/<int:pk>/recalculate/", admin_views.admin_recalc_window, name="admin_recalc_window"),
    path("admin/leave-balance/", admin_views.admin_leave_balance, name="admin_leave_balance"),
]