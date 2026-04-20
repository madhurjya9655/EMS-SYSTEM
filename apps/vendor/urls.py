from django.urls import path
from . import views

app_name = 'vendor'

urlpatterns = [
    path('',                                    views.dashboard,       name='dashboard'),
    path('new/',                                views.new_request,     name='new_request'),
    path('my/',                                 views.my_requests,     name='my_requests'),
    path('queue/',                              views.approval_queue,  name='approval_queue'),
    path('<int:pk>/',                           views.detail,          name='detail'),
    path('<int:pk>/resubmit/',                  views.resubmit,        name='resubmit'),
    path('<int:pk>/finance/',                   views.finance_action,  name='finance_action'),
    path('<int:pk>/senior/',                    views.senior_action,   name='senior_action'),

    # Admin setup
    path('setup/',                              views.admin_setup,     name='admin_setup'),

    # Vendor CRUD
    path('setup/vendor/add/',                   views.add_vendor,      name='add_vendor'),
    path('setup/vendor/<int:pk>/edit/',         views.edit_vendor,     name='edit_vendor'),
    path('setup/vendor/<int:pk>/delete/',       views.delete_vendor,   name='delete_vendor'),
    path('setup/vendor/<int:pk>/toggle/',       views.toggle_vendor,   name='toggle_vendor'),
]