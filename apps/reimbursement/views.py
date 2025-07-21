from django.contrib.auth.mixins import LoginRequiredMixin
from apps.users.mixins import PermissionRequiredMixin
from django.urls import reverse_lazy
from django.views.generic import ListView, CreateView, UpdateView, DetailView
from .models import Reimbursement
from .forms import ReimbursementForm, ManagerReviewForm, FinanceReviewForm
from apps.users.permissions import has_permission

# Employee: List their reimbursements
class MyReimbursementsView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    permission_code = 'reimbursement_list'
    model = Reimbursement
    template_name = 'reimbursement/my_requests.html'
    context_object_name = 'requests'

    def get_queryset(self):
        return Reimbursement.objects.filter(employee=self.request.user)

# Employee: Submit new reimbursement
class ReimbursementCreateView(LoginRequiredMixin, PermissionRequiredMixin, CreateView):
    permission_code = 'reimbursement_apply'
    model = Reimbursement
    form_class = ReimbursementForm
    template_name = 'reimbursement/apply.html'
    success_url = reverse_lazy('reimbursement:my_reimbursements')

    def form_valid(self, form):
        form.instance.employee = self.request.user
        return super().form_valid(form)

# Employee: View detail of their request
class ReimbursementDetailView(LoginRequiredMixin, PermissionRequiredMixin, DetailView):
    permission_code = 'reimbursement_list'
    model = Reimbursement
    template_name = 'reimbursement/request_detail.html'
    context_object_name = 'request'

# Manager: View pending for manager review
class ManagerPendingView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    permission_code = 'reimbursement_manager_pending'
    model = Reimbursement
    template_name = 'reimbursement/manager_pending.html'
    context_object_name = 'requests'

    def get_queryset(self):
        return Reimbursement.objects.filter(status='PM')

# Manager: Review a single request
class ManagerReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    permission_code = 'reimbursement_manager_review'
    model = Reimbursement
    form_class = ManagerReviewForm
    template_name = 'reimbursement/manager_review.html'
    success_url = reverse_lazy('reimbursement:manager_pending')

# Finance: View pending for finance review
class FinancePendingView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    permission_code = 'reimbursement_finance_pending'
    model = Reimbursement
    template_name = 'reimbursement/finance_pending.html'
    context_object_name = 'requests'

    def get_queryset(self):
        return Reimbursement.objects.filter(status='PF')

# Finance: Review a single request
class FinanceReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    permission_code = 'reimbursement_finance_review'
    model = Reimbursement
    form_class = FinanceReviewForm
    template_name = 'reimbursement/finance_review.html'
    success_url = reverse_lazy('reimbursement:finance_pending')
