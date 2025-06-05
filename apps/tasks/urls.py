from django.urls import path
from . import views

app_name = "tasks"

urlpatterns = [
    # Checklist
    path('checklist/',              views.list_checklist,          name='list_checklist'),
    path('checklist/add/',          views.add_checklist,           name='add_checklist'),
    path('checklist/edit/<int:pk>/',   views.edit_checklist,      name='edit_checklist'),
    path('checklist/delete/<int:pk>/', views.delete_checklist,    name='delete_checklist'),
    path('checklist/reassign/<int:pk>/', views.reassign_checklist, name='reassign_checklist'),

    # Delegation
    path('delegation/',             views.list_delegation,         name='list_delegation'),
    path('delegation/add/',         views.add_delegation,          name='add_delegation'),
    path('delegation/edit/<int:pk>/',   views.edit_delegation,     name='edit_delegation'),
    path('delegation/delete/<int:pk>/', views.delete_delegation,   name='delete_delegation'),
    path('delegation/reassign/<int:pk>/', views.reassign_delegation, name='reassign_delegation'),

    # FMS (placeholder)
    path('fms/',                    views.list_fms,               name='list_fms'),

    # Bulk Upload
    path('bulkupload/',             views.bulk_upload,            name='bulk_upload'),

    # Help Ticket (placeholder)
    path('help_ticket/',            views.list_help_ticket,       name='list_help_ticket'),
]
