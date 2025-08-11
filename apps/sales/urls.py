from django.urls import path
from .views import (
    PlanListView, PlanCreateView, ActualUpdateView, SalesDashboardView,
    CollectionPlanListView, CollectionPlanCreateView, CollectionPlanUpdateView,
    CallPlanListView, CallPlanCreateView, CallPlanUpdateView,
    EnquiryReportListView,
    NBDListView, NBDCreateView,
    VisitDetailsListView, VisitDetailsCreateView, VisitDetailsUpdateView,
)

app_name = 'sales'

urlpatterns = [
    path('dashboard/',                SalesDashboardView.as_view(),          name='dashboard'),

    # 1. Sales Plan
    path('plan/',                     PlanListView.as_view(),                name='sales_plan_list'),
    path('plan/add/',                 PlanCreateView.as_view(),              name='sales_plan_add'),
    path('plan/<int:pk>/edit/',       ActualUpdateView.as_view(),            name='sales_plan_edit'),

    # 2. Collection Plan
    path('collection/',               CollectionPlanListView.as_view(),      name='collection_plan_list'),
    path('collection/add/',           CollectionPlanCreateView.as_view(),    name='collection_plan_add'),
    path('collection/<int:pk>/edit/', CollectionPlanUpdateView.as_view(),    name='collection_plan_edit'),

    # 3. Call Plan
    path('call/',                     CallPlanListView.as_view(),            name='call_plan_list'),
    path('call/add/',                 CallPlanCreateView.as_view(),          name='call_plan_add'),
    path('call/<int:pk>/edit/',       CallPlanUpdateView.as_view(),          name='call_plan_edit'),

    # 4. Enquiry Report (view only, no add/edit)
    path('enquiry/',                  EnquiryReportListView.as_view(),       name='enquiry_report_list'),

    # 5. New Business Development (NBD)
    path('nbd/',                      NBDListView.as_view(),                 name='nbd_list'),
    path('nbd/add/',                  NBDCreateView.as_view(),               name='nbd_add'),

    # 6. Visit Details
    path('visit/',                    VisitDetailsListView.as_view(),        name='visit_details_list'),
    path('visit/add/',                VisitDetailsCreateView.as_view(),      name='visit_details_add'),
    path('visit/<int:pk>/edit/',      VisitDetailsUpdateView.as_view(),      name='visit_details_edit'),
]
