from django.urls import path
from . import views
from .views_reports import recurring_report
from .views_cron import weekly_congrats_hook

app_name = "tasks"

urlpatterns = [
    path("checklist/",                    views.list_checklist,        name="list_checklist"),
    path("checklist/add/",                views.add_checklist,         name="add_checklist"),
    path("checklist/edit/<int:pk>/",      views.edit_checklist,        name="edit_checklist"),
    path("checklist/delete/<int:pk>/",    views.delete_checklist,      name="delete_checklist"),
    path("checklist/complete/<int:pk>/",  views.complete_checklist,    name="complete_checklist"),
    path("checklist/reassign/<int:pk>/",  views.reassign_checklist,    name="reassign_checklist"),
    path("checklist/<int:pk>/",           views.checklist_details,     name="checklist_detail"),

    path("delegation/",                   views.list_delegation,       name="list_delegation"),
    path("delegation/add/",               views.add_delegation,        name="add_delegation"),
    path("delegation/edit/<int:pk>/",     views.edit_delegation,       name="edit_delegation"),
    path("delegation/delete/<int:pk>/",   views.delete_delegation,     name="delete_delegation"),
    path("delegation/reassign/<int:pk>/", views.reassign_delegation,   name="reassign_delegation"),
    path("delegation/complete/<int:pk>/", views.complete_delegation,   name="complete_delegation"),
    path("delegation/<int:pk>/",          views.delegation_details,    name="delegation_detail"),

    path("fms/",                          views.list_fms,              name="list_fms"),

    path("bulkupload/",                     views.bulk_upload,                  name="bulk_upload"),
    path("bulkupload/checklist-template/",  views.download_checklist_template,  name="download_checklist_template"),
    path("bulkupload/delegation-template/", views.download_delegation_template, name="download_delegation_template"),

    path("help_ticket/",                    views.list_help_ticket,      name="list_help_ticket"),
    path("help_ticket/assigned-to-me/",     views.assigned_to_me,        name="assigned_to_me"),
    path("help_ticket/assigned-by-me/",     views.assigned_by_me,        name="assigned_by_me"),
    path("help_ticket/add/",                views.add_help_ticket,       name="add_help_ticket"),
    path("help_ticket/edit/<int:pk>/",      views.edit_help_ticket,      name="edit_help_ticket"),
    path("help_ticket/delete/<int:pk>/",    views.delete_help_ticket,    name="delete_help_ticket"),
    path("help_ticket/complete/<int:pk>/",  views.complete_help_ticket,  name="complete_help_ticket"),
    path("help_ticket/note/<int:pk>/",      views.note_help_ticket,      name="note_help_ticket"),
    path("help_ticket/details/<int:pk>/",   views.help_ticket_details,   name="help_ticket_details"),

    path("reports/recurring/",              recurring_report,            name="recurring_report"),

    # HTTP cron hook (POST only)
    path("internal/cron/weekly-congrats/<str:token>/", weekly_congrats_hook, name="cron_weekly_congrats"),
]
