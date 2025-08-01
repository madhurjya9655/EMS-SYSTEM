from django.urls import path
from .views import (
    PlanListView,
    PlanCreateView,
    ActualUpdateView,
    SalesDashboardView,
)

app_name = 'sales'

urlpatterns = [
    path('plan/',               PlanListView.as_view(),    name='sales_plan_list'),
    path('plan/add/',           PlanCreateView.as_view(),  name='sales_plan_add'),
    path('plan/<int:pk>/edit/', ActualUpdateView.as_view(), name='sales_plan_edit'),
    path('dashboard/',          SalesDashboardView.as_view(), name='sales_dashboard'),
]
