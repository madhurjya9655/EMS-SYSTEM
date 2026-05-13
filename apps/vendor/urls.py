#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\vendor\urls.py
from django.urls import path

from . import views

app_name = 'vendor'

urlpatterns = [
    # Dashboard
    path('', views.dashboard, name='dashboard'),

    # New payment request
    path('new/', views.new_request, name='new_request'),

    # My requests
    # IMPORTANT:
    # Keep /vendor/my/ because your system/browser is already calling it.
    path('my/', views.my_requests, name='my_requests'),

    # Optional friendly alias.
    # Do not use this name in templates. Main name remains my_requests above.
    path('my-requests/', views.my_requests, name='my_requests_alt'),

    # Approval queue
    path('approval-queue/', views.approval_queue, name='approval_queue'),

    # Detail page
    path('detail/<int:pk>/', views.detail, name='detail'),

    # Workflow actions
    path('<int:pk>/resubmit/', views.resubmit, name='resubmit'),
    path('<int:pk>/finance-action/', views.finance_action, name='finance_action'),
    path('<int:pk>/senior-action/', views.senior_action, name='senior_action'),

    # Admin setup
    path('admin-setup/', views.admin_setup, name='admin_setup'),

    # Vendor master CRUD
    path('vendors/add/', views.add_vendor, name='add_vendor'),
    path('vendors/<int:pk>/edit/', views.edit_vendor, name='edit_vendor'),
    path('vendors/<int:pk>/delete/', views.delete_vendor, name='delete_vendor'),
    path('vendors/<int:pk>/toggle/', views.toggle_vendor, name='toggle_vendor'),

    # AJAX API for vendor type auto-fill
    path(
        'api/vendors/<int:vendor_id>/type/',
        views.vendor_type_api,
        name='vendor_type_api',
    ),
]