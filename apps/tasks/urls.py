# apps/tasks/urls.py
from django.urls import path
from . import views
from .views_reports import recurring_report
from .views_cron import weekly_congrats_hook, pre10am_unblock_and_generate_hook
from . import cron_views  # <-- use hardened cron endpoints here

app_name = "tasks"

urlpatterns = [
    # -----------------
    # Checklist
    # -----------------
    path("checklist/",                    views.list_checklist,        name="list_checklist"),
    path("checklist/add/",                views.add_checklist,         name="add_checklist"),
    path("checklist/edit/<int:pk>/",      views.edit_checklist,        name="edit_checklist"),
    path("checklist/delete/<int:pk>/",    views.delete_checklist,      name="delete_checklist"),
    path("checklist/complete/<int:pk>/",  views.complete_checklist,    name="complete_checklist"),
    path("checklist/reassign/<int:pk>/",  views.reassign_checklist,    name="reassign_checklist"),
    path("checklist/<int:pk>/",           views.checklist_details,     name="checklist_detail"),

    # -----------------
    # Delegation
    # -----------------
    path("delegation/",                   views.list_delegation,       name="list_delegation"),
    path("delegation/add/",               views.add_delegation,        name="add_delegation"),
    path("delegation/edit/<int:pk>/",     views.edit_delegation,       name="edit_delegation"),
    path("delegation/delete/<int:pk>/",   views.delete_delegation,     name="delete_delegation"),
    # alias keeping URL name stable
    path("delegation/reassign/<int:pk>/", views.edit_delegation,       name="reassign_delegation"),
    path("delegation/complete/<int:pk>/", views.complete_delegation,   name="complete_delegation"),
    path("delegation/<int:pk>/",          views.delegation_details,    name="delegation_detail"),

    # -----------------
    # FMS
    # -----------------
    path("fms/",                          views.list_fms,              name="list_fms"),

    # -----------------
    # Bulk Upload
    # -----------------
    path("bulkupload/",                     views.bulk_upload,                  name="bulk_upload"),
    path("bulkupload/checklist-template/",  views.download_checklist_template,  name="download_checklist_template"),
    path("bulkupload/delegation-template/", views.download_delegation_template, name="download_delegation_template"),

    # -----------------
    # Help Ticket
    # -----------------
    path("help_ticket/",                    views.list_help_ticket,      name="list_help_ticket"),
    path("help_ticket/assigned-to-me/",     views.assigned_to_me,        name="assigned_to_me"),
    path("help_ticket/assigned-by-me/",     views.assigned_by_me,        name="assigned_by_me"),
    path("help_ticket/add/",                views.add_help_ticket,       name="add_help_ticket"),
    path("help_ticket/edit/<int:pk>/",      views.edit_help_ticket,      name="edit_help_ticket"),
    path("help_ticket/delete/<int:pk>/",    views.delete_help_ticket,    name="delete_help_ticket"),
    path("help_ticket/complete/<int:pk>/",  views.complete_help_ticket,  name="complete_help_ticket"),
    path("help_ticket/close/<int:pk>/",     views.close_help_ticket,     name="close_help_ticket"),
    path("help_ticket/note/<int:pk>/",      views.note_help_ticket,      name="note_help_ticket"),
    path("help_ticket/details/<int:pk>/",   views.help_ticket_details,   name="help_ticket_details"),
    path("help_ticket/detail/<int:pk>/",    views.help_ticket_detail,    name="help_ticket_detail"),

    # -----------------
    # Reports
    # -----------------
    path("reports/recurring/",              recurring_report,            name="recurring_report"),

    # -----------------
    # HTTP cron hooks (internal)
    # -----------------
    # Weekly congrats stays on the existing module:
    path("internal/cron/weekly-congrats/<str:token>/", weekly_congrats_hook,      name="cron_weekly_congrats_with_token"),
    path("internal/cron/weekly-congrats/",             weekly_congrats_hook,      name="cron_weekly_congrats"),

    # ✅ Route "due-today" to hardened view that accepts ?key / headers and always JSON-200 on errors
    path("internal/cron/due-today/<str:token>/",       cron_views.due_today,       name="cron_due_today_with_token"),
    path("internal/cron/due-today/",                   cron_views.due_today,       name="cron_due_today"),

    # ✅ Add explicit endpoints for the consolidated 7PM summaries (admin + per-employee)
    path("internal/cron/pending-7pm/",                 cron_views.pending_summary_7pm,  name="cron_pending_7pm"),

    # ✅ Manual employee digest trigger (username/to via querystring)
    path("internal/cron/employee-digest/",             cron_views.employee_digest,      name="cron_employee_digest"),

    # NEW: 09:55 IST unblock + generate (kept on existing module as-is)
    path("internal/cron/pre10am-unblock/<str:token>/", pre10am_unblock_and_generate_hook, name="cron_pre10_with_token"),
    path("internal/cron/pre10am-unblock/",             pre10am_unblock_and_generate_hook, name="cron_pre10"),
]
