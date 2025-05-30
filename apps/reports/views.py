from datetime import date, timedelta
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from .forms_reports import PCReportFilterForm
from apps.tasks.models import Checklist, Delegation

@login_required
def list_doer_tasks(request):
    form = PCReportFilterForm(request.GET or None)
    items = Checklist.objects.select_related('assign_by','assign_to')
    if form.is_valid():
        d = form.cleaned_data
        if d['doer']:
            items = items.filter(assign_to=d['doer'])
        if d['department']:
            items = items.filter(group_name__icontains=d['department'])
        if d['date_from']:
            items = items.filter(planned_date__date__gte=d['date_from'])
        if d['date_to']:
            items = items.filter(planned_date__date__lte=d['date_to'])
    return render(request, 'reports/list_doer_tasks.html', {'form': form, 'items': items})

@login_required
def list_fms_tasks(request):
    form = PCReportFilterForm(request.GET or None)
    items = Delegation.objects.select_related('assign_by','assign_to')
    if form.is_valid():
        d = form.cleaned_data
        if d['doer']:
            items = items.filter(assign_to=d['doer'])
        if d['department']:
            items = items.filter(assign_by__groups__name__icontains=d['department'])
        if d['date_from']:
            items = items.filter(planned_date__gte=d['date_from'])
        if d['date_to']:
            items = items.filter(planned_date__lte=d['date_to'])
    return render(request, 'reports/list_fms_tasks.html', {'form': form, 'items': items})

@login_required
def weekly_mis_score(request):
    form = PCReportFilterForm(request.GET or None)
    r = {}
    if form.is_valid() and form.cleaned_data['doer']:
        d = form.cleaned_data
        start = d['date_from'] or (date.today() - timedelta(days=7))
        end   = d['date_to']   or date.today()
        doer  = d['doer']
        assigned = Checklist.objects.filter(assign_to=doer, planned_date__date__range=(start,end)).count() \
                 + Delegation.objects.filter(assign_to=doer, planned_date__range=(start,end)).count()
        r = {'assigned': assigned}
    return render(request, 'reports/weekly_mis_score.html', {'form': form, 'r': r})

@login_required
def performance_score(request):
    form = PCReportFilterForm(request.GET or None)
    p = {}
    if form.is_valid() and form.cleaned_data['doer']:
        d = form.cleaned_data
        tasks = Checklist.objects.filter(assign_to=d['doer'], planned_date__date__range=(d['date_from'],d['date_to'])) \
              | Delegation.objects.filter(assign_to=d['doer'], planned_date__range=(d['date_from'],d['date_to']))
        total = tasks.count()
        # if you track completion, count that here
        p = {'total': total}
    return render(request, 'reports/performance_score.html', {'form': form, 'p': p})

@login_required
def auditor_report(request):
    form = PCReportFilterForm(request.GET or None)
    return render(request, 'reports/auditor_report.html', {'form': form})
