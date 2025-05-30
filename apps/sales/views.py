from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.views.generic import CreateView, ListView, UpdateView, TemplateView
from django.urls import reverse_lazy
from .models import SalesKPI
from .forms import KPIPlanForm, KPIActualForm

class SalesTeamRequiredMixin(UserPassesTestMixin):
    def test_func(self):
        return self.request.user.groups.filter(
            name__in=['Sales Executive','Sales Manager','Marketing Executive','Admin']
        ).exists()

class PlanCreateView(LoginRequiredMixin, SalesTeamRequiredMixin, CreateView):
    model = SalesKPI
    form_class = KPIPlanForm
    template_name = 'sales/plan_form.html'
    def form_valid(self, form):
        form.instance.employee = self.request.user
        return super().form_valid(form)
    success_url = reverse_lazy('sales:sales_plan_list')

class PlanListView(LoginRequiredMixin, SalesTeamRequiredMixin, ListView):
    model = SalesKPI
    template_name = 'sales/plan_list.html'
    context_object_name = 'kpis'
    def get_queryset(self):
        return SalesKPI.objects.filter(employee=self.request.user)

class ActualUpdateView(LoginRequiredMixin, SalesTeamRequiredMixin, UpdateView):
    model = SalesKPI
    form_class = KPIActualForm
    template_name = 'sales/actual_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')

class ManagerDashboardView(LoginRequiredMixin, SalesTeamRequiredMixin, TemplateView):
    template_name = 'sales/dashboard.html'
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        kpis = SalesKPI.objects.all().order_by('metric','period_start')
        summary = {}
        for choice, label in SalesKPI.METRIC_CHOICES:
            items = kpis.filter(metric=choice)
            total_target = sum(item.target for item in items)
            total_actual = sum(item.actual for item in items)
            summary[label] = {'target': total_target, 'actual': total_actual}
        context['summary'] = summary
        context['kpis'] = kpis
        return context
