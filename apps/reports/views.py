from datetime import date, timedelta
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.db.models import Sum, Count, Q, Avg,F
from .forms_reports import PCReportFilterForm
from apps.tasks.models import Checklist, Delegation, HelpTicket
from django.contrib.auth import get_user_model
import csv
import json

User = get_user_model()

def get_week_dates(frm, to):
    if frm and to:
        return frm, to
    today = date.today()
    start = today - timedelta(days=today.weekday())
    return start, start + timedelta(days=6)

def week_number(start):
    fy_start = date(start.year if start.month >= 4 else start.year-1, 4, 1)
    return ((start - fy_start).days // 7) + 1

def calculate_stats(doer, start, end):
    stats = []
    categories = [
        ('Checklist', Checklist, 'planned_date__range', ('status', 'Completed')),
        ('Delegation', Delegation, 'planned_date__range', None),
        ('Help Ticket', HelpTicket, 'planned_date__range', ('status', 'Closed')),
    ]
    for name, Model, date_kw, status in categories:
        planned = Model.objects.filter(assign_to=doer, **{date_kw: (start, end)}).count()
        if status:
            completed = Model.objects.filter(
                assign_to=doer,
                **{date_kw: (start, end)},
                **{status[0]: status[1]}
            ).count()
        else:
            completed = planned
        pct = round(((completed - planned) / planned) * 100, 2) if planned else 0
        stats.append({
            'category': name,
            'planned': planned,
            'completed': completed,
            'percent': pct,
        })
    return stats

def ordinal(n):
    if 4 <= n % 100 <= 20:
        suf = 'th'
    else:
        suf = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n:02d}{suf}"

@login_required
def list_doer_tasks(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    items = Checklist.objects.select_related('assign_by', 'assign_to')
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
    form = PCReportFilterForm(request.GET or None, user=request.user)
    items = Delegation.objects.select_related('assign_by', 'assign_to')
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
    form = PCReportFilterForm(request.GET or None, user=request.user)
    rows = []
    if form.is_valid() and form.cleaned_data.get('doer'):
        doer = form.cleaned_data['doer']
        frm, to = get_week_dates(form.cleaned_data['date_from'], form.cleaned_data['date_to'])
        prev_frm = frm - timedelta(days=7)
        prev_to = frm - timedelta(days=1)

        this_week = calculate_stats(doer, frm, to)
        last_week = calculate_stats(doer, prev_frm, prev_to)

        for i, data in enumerate(this_week):
            rows.append({
                'category': data['category'],
                'last_pct': last_week[i]['percent'],
                'planned': data['planned'],
                'completed': data['completed'],
                'percent': data['percent'],
            })

    return render(request, 'reports/weekly_mis_score.html', {
        'form': form,
        'rows': rows,
    })

def get_checklist_score(doer, date_from, date_to):
    checklist_all = Checklist.objects.filter(
        assign_to=doer,
        planned_date__date__range=[date_from, date_to]
    )
    
    planned_all = checklist_all.count()
    completed_all = checklist_all.filter(status='Completed').count()
    not_completed_all = planned_all - completed_all
    not_completed_pct_all = round((not_completed_all / planned_all * 100), 2) if planned_all > 0 else 0
    
    # Adjusting the logic for on-time completion without 'completed_date'
    checklist_ontime = checklist_all.filter(
        assign_to=doer,
        planned_date__date__range=[date_from, date_to],
        status='Completed'
    )
    
    planned_ontime = planned_all
    completed_ontime = checklist_ontime.count()
    not_completed_ontime = planned_ontime - completed_ontime
    not_completed_pct_ontime = round((not_completed_ontime / planned_ontime * 100), 2) if planned_ontime > 0 else 0
    
    return [
        {
            'task_type': 'All work should be done',
            'planned_task': planned_all,
            'completed_task': completed_all,
            'not_completed_pct': not_completed_pct_all,
        },
        {
            'task_type': 'All work should be done ontime',
            'planned_task': planned_ontime,
            'completed_task': completed_ontime,
            'not_completed_pct': not_completed_pct_ontime,
        }
    ]

def get_delegation_score(doer, date_from, date_to):
    delegation_all = Delegation.objects.filter(
        assign_to=doer,
        planned_date__range=[date_from, date_to]
    )
    
    planned_all = delegation_all.count()
    completed_all = planned_all
    not_completed_all = 0
    not_completed_pct_all = 0
    
    return [
        {
            'task_type': 'All work should be done',
            'planned_task': planned_all,
            'completed_task': completed_all,
            'not_completed_pct': not_completed_pct_all,
        }
    ]

def get_fms_score(doer, date_from, date_to):
    return [
        {
            'task_type': 'All work should be done',
            'planned_task': 0,
            'completed_task': 0,
            'not_completed_pct': 0,
        },
        {
            'task_type': 'All work should be done ontime',
            'planned_task': 0,
            'completed_task': 0,
            'not_completed_pct': 0,
        }
    ]

def get_audit_score(doer, date_from, date_to):
    audit_all = HelpTicket.objects.filter(
        assign_to=doer,
        planned_date__date__range=[date_from, date_to]
    )
    
    planned_all = audit_all.count()
    completed_all = audit_all.filter(status='Closed').count()
    not_completed_all = planned_all - completed_all
    not_completed_pct_all = round((not_completed_all / planned_all * 100), 2) if planned_all > 0 else 0
    
    audit_ontime = HelpTicket.objects.filter(
        assign_to=doer,
        planned_date__date__range=[date_from, date_to],
        status='Closed',
        updated_at__lte=F('planned_date')
    )
    
    planned_ontime = planned_all
    completed_ontime = audit_ontime.count()
    not_completed_ontime = planned_ontime - completed_ontime
    not_completed_pct_ontime = round((not_completed_ontime / planned_ontime * 100), 2) if planned_ontime > 0 else 0
    
    return [
        {
            'task_type': 'All work should be done',
            'planned_task': planned_all,
            'completed_task': completed_all,
            'not_completed_pct': not_completed_pct_all,
        },
        {
            'task_type': 'All work should be done ontime',
            'planned_task': planned_ontime,
            'completed_task': completed_ontime,
            'not_completed_pct': not_completed_pct_ontime,
        }
    ]

def calculate_summary(checklist_data, delegation_data, fms_data, audit_data):
    def get_avg_completion_rate(data):
        if not data:
            return 0.0
        total_planned = sum(item['planned_task'] for item in data)
        total_completed = sum(item['completed_task'] for item in data)
        return round((total_completed / total_planned * 100), 2) if total_planned > 0 else 0.0
    
    checklist_avg = get_avg_completion_rate(checklist_data)
    delegation_avg = get_avg_completion_rate(delegation_data)
    fms_avg = get_avg_completion_rate(fms_data)
    audit_avg = get_avg_completion_rate(audit_data)
    
    overall_avg = round((checklist_avg + delegation_avg + fms_avg + audit_avg) / 4, 2)
    
    checklist_ontime = checklist_data[1]['completed_task'] / checklist_data[1]['planned_task'] * 100 if checklist_data[1]['planned_task'] > 0 else 0
    delegation_ontime = delegation_avg
    fms_ontime = fms_avg
    audit_ontime = audit_data[1]['completed_task'] / audit_data[1]['planned_task'] * 100 if audit_data[1]['planned_task'] > 0 else 0
    overall_ontime = round((checklist_ontime + delegation_ontime + fms_ontime + audit_ontime) / 4, 2)
    
    return {
        'checklist_avg': checklist_avg,
        'delegation_avg': delegation_avg,
        'fms_avg': fms_avg,
        'audit_avg': audit_avg,
        'overall_avg': overall_avg,
        'checklist_ontime': round(checklist_ontime, 2),
        'delegation_ontime': round(delegation_ontime, 2),
        'fms_ontime': round(fms_ontime, 2),
        'audit_ontime': round(audit_ontime, 2),
        'overall_ontime': overall_ontime,
    }

def export_performance_csv(performance_data, header):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="performance_report.csv"'
    
    writer = csv.writer(response)
    writer.writerow([header])
    writer.writerow([])
    
    if performance_data.get('checklist_score'):
        writer.writerow(['Checklist Score'])
        writer.writerow(['Task Type', 'Planned Task', 'Completed Task', '% Not Completed'])
        for item in performance_data['checklist_score']:
            writer.writerow([item['task_type'], item['planned_task'], item['completed_task'], item['not_completed_pct']])
        writer.writerow([])
    
    if performance_data.get('delegation_score'):
        writer.writerow(['Delegation Score'])
        writer.writerow(['Task Type', 'Planned Task', 'Completed Task', '% Not Completed'])
        for item in performance_data['delegation_score']:
            writer.writerow([item['task_type'], item['planned_task'], item['completed_task'], item['not_completed_pct']])
        writer.writerow([])
    
    if performance_data.get('fms_score'):
        writer.writerow(['FMS Score'])
        writer.writerow(['Task Type', 'Planned Task', 'Completed Task', '% Not Completed'])
        for item in performance_data['fms_score']:
            writer.writerow([item['task_type'], item['planned_task'], item['completed_task'], item['not_completed_pct']])
        writer.writerow([])
    
    if performance_data.get('audit_score'):
        writer.writerow(['Audit Score'])
        writer.writerow(['Task Type', 'Planned Task', 'Completed Task', '% Not Completed'])
        for item in performance_data['audit_score']:
            writer.writerow([item['task_type'], item['planned_task'], item['completed_task'], item['not_completed_pct']])
        writer.writerow([])
    
    if performance_data.get('summary'):
        writer.writerow(['Summary'])
        writer.writerow(['Average Score', 'Checklist', 'Delegation', 'FMS', 'Audit', 'Overall Average'])
        summary = performance_data['summary']
        writer.writerow(['% work should be done', summary['checklist_avg'], summary['delegation_avg'], summary['fms_avg'], summary['audit_avg'], summary['overall_avg']])
        writer.writerow(['% work should be done ontime', summary['checklist_ontime'], summary['delegation_ontime'], summary['fms_ontime'], summary['audit_ontime'], summary['overall_ontime']])
    
    return response

@login_required
def performance_score(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    header = ''
    performance_data = None

    if form.is_valid() and (form.cleaned_data.get('doer') or not request.user.is_staff):
        doer = form.cleaned_data.get('doer') or request.user
        date_from, date_to = get_week_dates(form.cleaned_data.get('date_from'), form.cleaned_data.get('date_to'))

        phone = doer.profile.phone if hasattr(doer, 'profile') else ''
        dept = doer.profile.department if hasattr(doer, 'profile') else ''
        header = f"{doer.get_full_name().upper()}({phone})(DEPARTMENT - {dept}) - {ordinal(date_from.day)} {date_from.strftime('%b, %Y')} - {ordinal(date_to.day)} {date_to.strftime('%b, %Y')}"

        checklist_data = get_checklist_score(doer, date_from, date_to)
        delegation_data = get_delegation_score(doer, date_from, date_to)
        fms_data = get_fms_score(doer, date_from, date_to)
        audit_data = get_audit_score(doer, date_from, date_to)

        performance_data = {
            'checklist_score': checklist_data,
            'delegation_score': delegation_data,
            'fms_score': fms_data,
            'audit_score': audit_data,
            'summary': calculate_summary(checklist_data, delegation_data, fms_data, audit_data)
        }

        if request.GET.get('export') == 'csv':
            return export_performance_csv(performance_data, header)

    return render(request, 'reports/performance_score.html', {
        'form': form,
        'header': header,
        'performance_data': performance_data,
    })

@login_required
def auditor_report(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    return render(request, 'reports/auditor_report.html', {'form': form})