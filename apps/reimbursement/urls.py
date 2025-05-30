from django.urls import path
from . import views

app_name = 'reimbursement'

urlpatterns = [
    path('apply/',             views.ReimbursementCreateView.as_view(),   name='apply_reimbursement'),
    path('my/',                views.MyReimbursementsView.as_view(),     name='my_reimbursements'),
    path('manager/',           views.ManagerPendingView.as_view(),       name='manager_pending'),
    path('manager/<int:pk>/review/', views.ManagerReviewView.as_view(),  name='manager_review'),
    path('finance/',           views.FinancePendingView.as_view(),       name='finance_pending'),
    path('finance/<int:pk>/review/', views.FinanceReviewView.as_view(),  name='finance_review'),
    path('<int:pk>/',          views.ReimbursementDetailView.as_view(),  name='reimbursement_detail'),
]
