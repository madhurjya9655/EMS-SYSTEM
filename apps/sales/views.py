from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.views.generic import CreateView, UpdateView, TemplateView, View
from django.urls import reverse_lazy
from .models import SalesKPI, Customer
from .forms import (
    KPIPlanForm, KPIActualForm, SalesDashboardFilterForm,
    CollectionPlanForm, CollectionActualForm,
    CallPlanForm, CallActualForm,
    VisitDetailsForm, NBDForm,
)
from apps.users.permissions import has_permission
from django.contrib.auth import get_user_model
from django.shortcuts import render
from .google_sheets_utils import get_sheet_data_for_user

User = get_user_model()

TAB_SALES_PLAN = 'Sheet1'
TAB_COLLECTION_PLAN = 'CollectionData'
TAB_CALL_PLAN = 'CallData'
TAB_ENQUIRY_REPORT = 'EnquiryData'
TAB_NBD = 'NBDData'
TAB_VISIT_DETAILS = 'VisitDetailsData'

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

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.employee = self.request.user
        form.instance.metric = 'sales'
        form.instance.actual = 0
        return super().form_valid(form)

class PlanListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'sales_plan_list'
    template_name = 'sales/plan_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_SALES_PLAN, user_full_name)
        cleaned_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cleaned_rows.append({
                'customer_name': row.get('Customer Name') or row.get('Customer_Name') or row.get('customer_name') or '',
                'metric': row.get('Metric') or row.get('metric') or '',
                'plan_mt': row.get('Plan (MT)') or row.get('Plan_MT') or row.get('plan_mt') or row.get('Total Plan') or '',
                'actual_mt': row.get('Actual (MT)') or row.get('Actual_MT') or row.get('actual_mt') or row.get('WR Actual') or '',
                'period': row.get('Period') or row.get('period') or '',
                'period_start': row.get('Period_Start') or row.get('period_start') or '',
                'period_end': row.get('Period_End') or row.get('period_end') or '',
                'id': row.get('ID') or row.get('pk') or '',
            })
        return render(request, self.template_name, {'kpis': cleaned_rows, 'error_message': error_message})

class ActualUpdateView(LoginRequiredMixin, HasPermissionMixin, UpdateView):
    permission_code = 'sales_plan_edit'
    model = SalesKPI
    form_class = KPIActualForm
    template_name = 'sales/actual_form.html'
    success_url = reverse_lazy('sales:sales_plan_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        instance = form.save(commit=False)
        instance.actual = 0
        instance.save()
        return super().form_valid(form)

class SalesDashboardView(LoginRequiredMixin, HasPermissionMixin, TemplateView):
    permission_code = 'sales_dashboard'
    template_name = 'sales/sales_dashboard.html'

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        user_full_name = self.request.user.get_full_name()
        sales, sales_error = get_sheet_data_for_user(TAB_SALES_PLAN, user_full_name)
        collections, collections_error = get_sheet_data_for_user(TAB_COLLECTION_PLAN, user_full_name)
        calls, calls_error = get_sheet_data_for_user(TAB_CALL_PLAN, user_full_name)
        enquiries, enquiries_error = get_sheet_data_for_user(TAB_ENQUIRY_REPORT, user_full_name)
        nbds, nbds_error = get_sheet_data_for_user(TAB_NBD, user_full_name)
        visits, visits_error = get_sheet_data_for_user(TAB_VISIT_DETAILS, user_full_name)
        ctx['metrics'] = [{
            'name': row.get('Metric') or row.get('metric') or '',
            'target': row.get('Target') or row.get('Plan (MT)') or row.get('plan_mt') or row.get('Total Plan') or '',
            'planned': row.get('Planned') or row.get('Plan (MT)') or row.get('plan_mt') or row.get('Total Plan') or '',
            'actual': row.get('Actual') or row.get('Actual (MT)') or row.get('actual_mt') or row.get('WR Actual') or '',
            'conversion': '',
            'total_marks': '',
            'bonus_marks': '',
            'marks_obtained': '',
        } for row in sales]
        ctx['error_message'] = sales_error or collections_error or calls_error or enquiries_error or nbds_error or visits_error
        ctx['total_marks'] = ''
        ctx['total_bonus'] = ''
        ctx['total_obtained'] = ''
        ctx['form'] = SalesDashboardFilterForm(self.request.GET or None)  # <-- FIXED LINE
        ctx['num_customers'] = len(sales)
        ctx['date_range'] = ''
        ctx['total_overdue'] = ''
        return ctx

class CollectionPlanListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'collection_plan_list'
    template_name = 'sales/collection_plan_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_COLLECTION_PLAN, user_full_name)
        cleaned_rows = []
        for row in rows:
            cleaned_rows.append({
                'customer': {'name': row.get('Customer Name') or row.get('customer_name') or ''},
                'overdue_amount': row.get('Overdue Amount') or row.get('overdue_amount') or '',
                'planned': row.get('Planned Collection') or row.get('planned_collection') or row.get('Plan (MT)') or '',
                'actual': row.get('Actual Collection') or row.get('actual_collection') or row.get('Actual (MT)') or '',
                'period_start': row.get('Period_Start') or row.get('period_start') or '',
                'period_end': row.get('Period_End') or row.get('period_end') or '',
            })
        return render(request, self.template_name, {'collections': cleaned_rows, 'error_message': error_message})

class CollectionPlanCreateView(LoginRequiredMixin, HasPermissionMixin, CreateView):
    permission_code = 'collection_plan_add'
    model = SalesKPI
    form_class = CollectionPlanForm
    template_name = 'sales/collection_plan_form.html'
    success_url = reverse_lazy('sales:collection_plan_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.employee = self.request.user
        form.instance.metric = 'collection'
        form.instance.actual = 0
        form.instance.target = form.instance.target
        return super().form_valid(form)

class CollectionPlanUpdateView(LoginRequiredMixin, HasPermissionMixin, UpdateView):
    permission_code = 'collection_plan_edit'
    model = SalesKPI
    form_class = CollectionActualForm
    template_name = 'sales/collection_plan_form.html'
    success_url = reverse_lazy('sales:collection_plan_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

class CallPlanListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'call_plan_list'
    template_name = 'sales/call_plan_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_CALL_PLAN, user_full_name)
        cleaned_rows = []
        for row in rows:
            cleaned_rows.append({
                'customer': {'name': row.get('Customer Name') or row.get('customer_name') or ''},
                'person_name': row.get('Person Name') or row.get('person_name') or '',
                'call_date': row.get('Date of Call') or row.get('call_date') or '',
                'summary': row.get('Summary') or row.get('summary') or '',
            })
        return render(request, self.template_name, {'calls': cleaned_rows, 'error_message': error_message})

class CallPlanCreateView(LoginRequiredMixin, HasPermissionMixin, CreateView):
    permission_code = 'call_plan_add'
    model = SalesKPI
    form_class = CallPlanForm
    template_name = 'sales/call_plan_form.html'
    success_url = reverse_lazy('sales:call_plan_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.employee = self.request.user
        form.instance.metric = 'calls'
        return super().form_valid(form)

class CallPlanUpdateView(LoginRequiredMixin, HasPermissionMixin, UpdateView):
    permission_code = 'call_plan_edit'
    model = SalesKPI
    form_class = CallActualForm
    template_name = 'sales/call_plan_form.html'
    success_url = reverse_lazy('sales:call_plan_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

class EnquiryReportListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'enquiry_report_list'
    template_name = 'sales/enquiry_report_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_ENQUIRY_REPORT, user_full_name)
        return render(request, self.template_name, {'enquiries': rows, 'error_message': error_message})

class NBDListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'nbd_list'
    template_name = 'sales/nbd_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_NBD, user_full_name)
        return render(request, self.template_name, {'nbds': rows, 'error_message': error_message})

class NBDCreateView(LoginRequiredMixin, HasPermissionMixin, CreateView):
    permission_code = 'nbd_add'
    model = SalesKPI
    form_class = NBDForm
    template_name = 'sales/nbd_form.html'
    success_url = reverse_lazy('sales:nbd_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.employee = self.request.user
        form.instance.metric = 'nbd'
        return super().form_valid(form)

class VisitDetailsListView(LoginRequiredMixin, HasPermissionMixin, View):
    permission_code = 'visit_details_list'
    template_name = 'sales/visit_details_list.html'

    def get(self, request, *args, **kwargs):
        user_full_name = request.user.get_full_name()
        rows, error_message = get_sheet_data_for_user(TAB_VISIT_DETAILS, user_full_name)
        return render(request, self.template_name, {'visits': rows, 'error_message': error_message})

class VisitDetailsCreateView(LoginRequiredMixin, HasPermissionMixin, CreateView):
    permission_code = 'visit_details_add'
    model = SalesKPI
    form_class = VisitDetailsForm
    template_name = 'sales/visit_details_form.html'
    success_url = reverse_lazy('sales:visit_details_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.employee = self.request.user
        form.instance.metric = 'visits'
        return super().form_valid(form)

class VisitDetailsUpdateView(LoginRequiredMixin, HasPermissionMixin, UpdateView):
    permission_code = 'visit_details_edit'
    model = SalesKPI
    form_class = VisitDetailsForm
    template_name = 'sales/visit_details_form.html'
    success_url = reverse_lazy('sales:visit_details_list')

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs
