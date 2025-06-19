from django.urls import path
from . import views

app_name = 'petty_cash'

urlpatterns = [
    path('',              views.list_requests,     name='list_requests'),
    path('apply/',        views.apply_request,     name='apply_request'),
    path('manager/',      views.manager_requests,  name='manager_requests'),
    path('manager/<int:pk>/', views.manager_detail, name='manager_detail'),
    path('finance/',      views.finance_requests,  name='finance_requests'),
    path('finance/<int:pk>/', views.finance_detail, name='finance_detail'),
]
