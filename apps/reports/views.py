from datetime import date, timedelta
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models import Sum, F
from .forms_reports import PCReportFilterForm
from apps.tasks.models import Checklist, Delegation
from django.contrib.auth import get_user_model

User = get_user_model()

def get_week_dates(frm, to):
    if frm and to:
        return frm, to
    today = date.today()
    start = today - timedelta(days=today.weekday())
    return start, start + timedelta(days=6)

@login_required
def list_doer_tasks(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    items = Checklist.objects.select_related('assign_by', 'assign_to')
    if form.is_valid():
        d = form.cleaned_data
        if d['doer']:
            items = items.filter(assign_to=d['doer'])
        if d['department']:
            items = items.filter(assign_to__groups__name=d['department'])
        if d['date_from']:
            items = items.filter(planned_date__date__gte=d['date_from'])
        if d['date_to']:
            items = items.filter(planned_date__date__lte=d['date_to'])
    return render(request, 'reports/list_doer_tasks.html', {
        'form': form,
        'items': items,
    })

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
    return render(request, 'reports/list_fms_tasks.html', {
        'form': form,
        'items': items,
    })

@login_required
def weekly_mis_score(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    rows = []
    header = ''
    total_hours = ''
    week_start = None
    pending_checklist = pending_delegation = delayed_checklist = delayed_delegation = 0

    if form.is_valid() and form.cleaned_data.get('doer'):
        doer = form.cleaned_data['doer']
        frm, to = get_week_dates(form.cleaned_data['date_from'], form.cleaned_data['date_to'])
        week_start = frm
        prev_frm = frm - timedelta(days=7)
        prev_to  = frm - timedelta(days=1)

        # Checklist & Delegation stats
        for Model, label in [(Checklist, 'Checklist'), (Delegation, 'Delegation')]:
            date_kw = 'planned_date__date' if Model is Checklist else 'planned_date'
            look_this = {f"{date_kw}__range": (frm, to)}
            look_last = {f"{date_kw}__range": (prev_frm, prev_to)}

            planned      = Model.objects.filter(assign_to=doer, **look_this).count()
            planned_last = Model.objects.filter(assign_to=doer, **look_last).count()

            if Model is Checklist:
                completed      = Model.objects.filter(assign_to=doer, **look_this, status='Completed').count()
                completed_last = Model.objects.filter(assign_to=doer, **look_last, status='Completed').count()
            else:
                completed      = planned
                completed_last = planned_last

            pct      = round((completed      / planned      * 100), 2) if planned      else 0
            pct_last = round((completed_last / planned_last * 100), 2) if planned_last else 0

            rows.append({
                'category':  label,
                'last_pct':  pct_last,
                'planned':   planned,
                'completed': completed,
                'percent':   pct,
            })

        # total minutes per category
        mins_check = 0
        for rec in Checklist.objects.filter(assign_to=doer, planned_date__date__range=(frm, to)):
            m = rec.time_per_task_minutes or 0
            if rec.mode == 'Daily':
                run_start = max(frm, rec.planned_date.date())
                days      = (to - run_start).days + 1
                occ       = (days + rec.frequency - 1)//rec.frequency
            else:
                occ = 1
            mins_check += m * occ

        mins_deg = Delegation.objects.filter(assign_to=doer, planned_date__range=(frm, to))\
                                      .aggregate(total=Sum('time_per_task_minutes'))['total'] or 0

        # inject time into rows
        for row in rows:
            if row['category'] == 'Checklist':
                row['time'] = f"{mins_check//60:02d}:{mins_check%60:02d}"
            else:
                row['time'] = f"{mins_deg//60:02d}:{mins_deg%60:02d}"

        total = mins_check + mins_deg
        total_hours = f"{total//60:02d}:{total%60:02d}"

        # pending before week start
        pending_checklist  = Checklist.objects.filter(assign_to=doer, planned_date__date__lt=frm, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=frm).count()

        # delayed this week
        delayed_checklist = Checklist.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()
        delayed_delegation = 0

        phone = getattr(doer.profile, 'phone', '')
        dept  = getattr(doer.profile, 'department', '')
        header = f"{doer.get_full_name().upper()} ({phone}) – {frm:%d %b, %Y} to {to:%d %b, %Y} [{dept}]"

    return render(request, 'reports/weekly_mis_score.html', {
        'form':               form,
        'rows':               rows,
        'header':             header,
        'total_hours':        total_hours,
        'week_start':         week_start,
        'pending_checklist':  pending_checklist,
        'pending_delegation': pending_delegation,
        'delayed_checklist':  delayed_checklist,
        'delayed_delegation': delayed_delegation,
    })

@login_required
def performance_score(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    header             = ''
    checklist_data     = []
    delegation_data    = []
    summary            = {}
    time_checklist     = ''
    time_delegation    = ''
    total_hours        = ''
    week_start         = None
    pending_checklist  = pending_delegation = delayed_checklist = delayed_delegation = 0

    if form.is_valid() and (form.cleaned_data.get('doer') or not request.user.is_staff):
        doer = form.cleaned_data.get('doer') or request.user
        frm, to = get_week_dates(form.cleaned_data.get('date_from'), form.cleaned_data.get('date_to'))
        week_start = frm

        # Checklist stats
        qs = Checklist.objects.filter(assign_to=doer, planned_date__date__range=(frm, to))
        p  = qs.count()
        c  = qs.filter(status='Completed').count()
        pct_not = round((p - c) / p * 100, 2) if p else 0
        pct_on  = round((p - c) / p * 100, 2) if p else 0
        checklist_data = [
            {'task_type':'All work should be done',      'planned':p, 'completed':c, 'pct':pct_not},
            {'task_type':'All work should be done ontime','planned':p, 'completed':c, 'pct':pct_on},
        ]

        # Delegation stats
        qs2 = Delegation.objects.filter(assign_to=doer, planned_date__range=(frm, to))
        p2  = qs2.count()
        delegation_data = [
            {'task_type':'All work should be done',      'planned':p2, 'completed':p2, 'pct':0},
            {'task_type':'All work should be done ontime','planned':p2, 'completed':p2, 'pct':0},
        ]

        # time
        m_check = 0
        for rec in qs:
            m = rec.time_per_task_minutes or 0
            if rec.mode == 'Daily':
                sr = max(frm, rec.planned_date.date())
                days = (to - sr).days + 1
                occ  = (days + rec.frequency - 1)//rec.frequency
            else:
                occ = 1
            m_check += m * occ
        time_checklist = f"{m_check//60:02d}:{m_check%60:02d}"

        m_deg = qs2.aggregate(total=Sum('time_per_task_minutes'))['total'] or 0
        time_delegation = f"{m_deg//60:02d}:{m_deg%60:02d}"

        total = m_check + m_deg
        total_hours = f"{total//60:02d}:{total%60:02d}"

        # summary
        sa = round((c / p * 100), 2) if p else 0
        so = sa
        da = round((p2 / p2 * 100), 2) if p2 else 0
        summary = {
            'checklist_avg':     sa,
            'checklist_ontime':  so,
            'delegation_avg':    da,
            'delegation_ontime': da,
            'overall_avg':       round((sa + da)/2, 2),
            'overall_ontime':    round((so + da)/2, 2),
        }

        # pending & delayed
        pending_checklist  = Checklist.objects.filter(assign_to=doer, planned_date__date__lt=frm, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=frm).count()
        delayed_checklist  = Checklist.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()
        delayed_delegation = 0

        phone = getattr(doer.profile, 'phone', '')
        dept  = getattr(doer.profile, 'department', '')
        header = f"{doer.get_full_name().upper()} ({phone}) – {frm:%d %b, %Y} to {to:%d %b, %Y} [{dept}]"

    return render(request, 'reports/performance_score.html', {
        'form':               form,
        'header':             header,
        'checklist_data':     checklist_data,
        'delegation_data':    delegation_data,
        'time_checklist':     time_checklist,
        'time_delegation':    time_delegation,
        'summary':            summary,
        'total_hours':        total_hours,
        'week_start':         week_start,
        'pending_checklist':  pending_checklist,
        'pending_delegation': pending_delegation,
        'delayed_checklist':  delayed_checklist,
        'delayed_delegation': delayed_delegation,
    })
