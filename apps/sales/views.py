from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.views.generic import CreateView, ListView, UpdateView, TemplateView
from django.urls import reverse_lazy
from .models import SalesKPI
from .forms import KPIPlanForm, KPIActualForm

class HasPermissionMixin:
    permission_code = None

    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_superuser:
            try:
                profile = request.user.profile
            except Exception:
                raise PermissionDenied
            perms = getattr(profile, 'permissions', None)
            if not perms or (self.permission_code and self.permission_code not in perms):
                raise PermissionDenied
        return super().dispatch(request, *args, **kwargs)

class PlanCreateView(LoginRequiredMixin, HasPermissionMixin, CreateView):
    permission_code = 'sales_plan_add'
    model = SalesKPI
    form_class = KPIPlanForm
    template_name = 'sales/plan_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')

    def form_valid(self, form):
        form.instance.employee = self.request.user
        return super().form_valid(form)

class PlanListView(LoginRequiredMixin, HasPermissionMixin, ListView):
    permission_code = 'sales_plan_list'
    model = SalesKPI
    template_name = 'sales/plan_list.html'
    context_object_name = 'kpis'

    def get_queryset(self):
        return SalesKPI.objects.filter(employee=self.request.user)

class ActualUpdateView(LoginRequiredMixin, HasPermissionMixin, UpdateView):
    permission_code = 'sales_plan_edit'
    model = SalesKPI
    form_class = KPIActualForm
    template_name = 'sales/actual_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')

class SalesDashboardView(LoginRequiredMixin, HasPermissionMixin, TemplateView):
    permission_code = 'sales_dashboard'
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
