from django.urls import path
from . import views

app_name = "tasks"

urlpatterns = [
    path('checklist/', views.list_checklist, name='list_checklist'),
    path('checklist/add/', views.add_checklist, name='add_checklist'),
    path('checklist/edit/<int:pk>/', views.edit_checklist, name='edit_checklist'),
    path('checklist/delete/<int:pk>/', views.delete_checklist, name='delete_checklist'),
    path('delegation/', views.list_delegation, name='list_delegation'),
    path('delegation/add/', views.add_delegation, name='add_delegation'),
    path('delegation/edit/<int:pk>/', views.edit_delegation, name='edit_delegation'),
    path('delegation/delete/<int:pk>/', views.delete_delegation, name='delete_delegation'),
    path('fms/', views.list_fms, name='list_fms'),
    path('bulkupload/', views.bulk_upload, name='bulk_upload'),
    path('help_ticket/', views.list_help_ticket, name='list_help_ticket'),
]
