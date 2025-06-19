from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import CreateView, ListView, UpdateView, TemplateView
from django.urls import reverse_lazy
from .models import SalesKPI
from .forms import KPIPlanForm, KPIActualForm

class PlanCreateView(LoginRequiredMixin, CreateView):
    model = SalesKPI
    form_class = KPIPlanForm
    template_name = 'sales/plan_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')
    def form_valid(self, form):
        form.instance.employee = self.request.user
        return super().form_valid(form)

class PlanListView(LoginRequiredMixin, ListView):
    model = SalesKPI
    template_name = 'sales/plan_list.html'
    context_object_name = 'kpis'
    def get_queryset(self):
        return SalesKPI.objects.filter(employee=self.request.user)

class ActualUpdateView(LoginRequiredMixin, UpdateView):
    model = SalesKPI
    form_class = KPIActualForm
    template_name = 'sales/actual_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')

class ManagerDashboardView(LoginRequiredMixin, TemplateView):
    template_name = 'sales/dashboard.html'
    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        kpis = SalesKPI.objects.all().order_by('metric', 'period_start')
        summary = {}
        for choice, label in SalesKPI.METRIC_CHOICES:
            items = kpis.filter(metric=choice)
            summary[label] = {
                'target': sum(i.target for i in items),
                'actual': sum(i.actual for i in items)
            }
        ctx['summary'] = summary
        ctx['kpis'] = kpis
        return ctx