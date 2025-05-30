from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy
from django.views.generic import ListView, CreateView, UpdateView, DetailView
from .models import Reimbursement
from .forms import ReimbursementForm, ManagerReviewForm, FinanceReviewForm

class MyReimbursementsView(LoginRequiredMixin, ListView):
    model = Reimbursement
    template_name = 'reimbursement/my_requests.html'
    context_object_name = 'requests'
    def get_queryset(self):
        return Reimbursement.objects.filter(employee=self.request.user)

class ReimbursementCreateView(LoginRequiredMixin, CreateView):
    model = Reimbursement
    form_class = ReimbursementForm
    template_name = 'reimbursement/apply.html'
    def form_valid(self, form):
        form.instance.employee = self.request.user
        return super().form_valid(form)
    success_url = reverse_lazy('reimbursement:my_reimbursements')

class ReimbursementDetailView(LoginRequiredMixin, DetailView):
    model = Reimbursement
    template_name = 'reimbursement/request_detail.html'
    context_object_name = 'request'

class ManagerRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        return self.request.user.groups.filter(name='Manager').exists()

class ManagerPendingView(LoginRequiredMixin, ManagerRequiredMixin, ListView):
    model = Reimbursement
    template_name = 'reimbursement/manager_pending.html'
    context_object_name = 'requests'
    def get_queryset(self):
        return Reimbursement.objects.filter(status='PM')

class ManagerReviewView(LoginRequiredMixin, ManagerRequiredMixin, UpdateView):
    model = Reimbursement
    form_class = ManagerReviewForm
    template_name = 'reimbursement/manager_review.html'
    success_url = reverse_lazy('reimbursement:manager_pending')

class FinanceRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        return self.request.user.groups.filter(name='Finance').exists()

class FinancePendingView(LoginRequiredMixin, FinanceRequiredMixin, ListView):
    model = Reimbursement
    template_name = 'reimbursement/finance_pending.html'
    context_object_name = 'requests'
    def get_queryset(self):
        return Reimbursement.objects.filter(status='PF')

class FinanceReviewView(LoginRequiredMixin, FinanceRequiredMixin, UpdateView):
    model = Reimbursement
    form_class = FinanceReviewForm
    template_name = 'reimbursement/finance_review.html'
    success_url = reverse_lazy('reimbursement:finance_pending')
