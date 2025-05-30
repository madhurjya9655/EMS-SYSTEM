from django.urls import path
from . import views

app_name = 'petty_cash'

urlpatterns = [
    path('',         views.my_requests,      name='list_requests'),
    path('manager/', views.manager_requests, name='manager_requests'),
    path('finance/', views.finance_requests, name='finance_requests'),
]
