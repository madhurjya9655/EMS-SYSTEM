from datetime import date, timedelta
import csv, io
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.db.models import Sum, F
from django.core.exceptions import ObjectDoesNotExist
from django.urls import reverse

from .forms_reports import PCReportFilterForm, WeeklyMISCommitmentForm
from apps.tasks.models import Checklist, Delegation
from .models import WeeklyCommitment
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
    commitment_form = None
    commitment_message = ''
    rows = []
    header = ''
    total_hours = ''
    week_start = None
    avg_scores = None
    pending_checklist = pending_delegation = delayed_checklist = delayed_delegation = 0

    if form.is_valid() and form.cleaned_data.get('doer'):
        doer = form.cleaned_data['doer']
        frm, to = get_week_dates(form.cleaned_data['date_from'], form.cleaned_data['date_to'])
        week_start = frm
        prev_frm = frm - timedelta(days=7)
        prev_to = frm - timedelta(days=1)

        # --- COMMITMENT LOGIC START ---
        this_week_commitment = WeeklyCommitment.objects.filter(user=doer, week_start=frm).first()
        last_week_commitment = WeeklyCommitment.objects.filter(user=doer, week_start=prev_frm).first()

        # POST: save commitment (ONLY checklist & delegation)
        if request.method == "POST" and 'update_commitment' in request.POST:
            commitment_form = WeeklyMISCommitmentForm(request.POST)
            if commitment_form.is_valid():
                cleaned = commitment_form.cleaned_data
                if not this_week_commitment:
                    this_week_commitment = WeeklyCommitment(user=doer, week_start=frm)
                this_week_commitment.checklist = cleaned.get("checklist") or 0
                this_week_commitment.checklist_desc = cleaned.get("checklist_desc") or ""
                this_week_commitment.delegation = cleaned.get("delegation") or 0
                this_week_commitment.delegation_desc = cleaned.get("delegation_desc") or ""
                this_week_commitment.save()
                commitment_message = "Commitment updated successfully."
                return redirect(request.path + "?" + request.META.get('QUERY_STRING', ''))
        else:
            # Pre-populate commitment (ONLY checklist & delegation)
            initial = {}
            if this_week_commitment:
                initial = {
                    "checklist": this_week_commitment.checklist,
                    "checklist_desc": this_week_commitment.checklist_desc,
                    "delegation": this_week_commitment.delegation,
                    "delegation_desc": this_week_commitment.delegation_desc,
                }
            commitment_form = WeeklyMISCommitmentForm(initial=initial)

        # SCORE TABLE ROWS (Checklist/Delegation)
        for Model, label in [(Checklist, 'Checklist'), (Delegation, 'Delegation')]:
            date_kw = 'planned_date__date' if Model is Checklist else 'planned_date'
            look_this = {f"{date_kw}__range": (frm, to)}
            look_last = {f"{date_kw}__range": (prev_frm, prev_to)}

            planned = Model.objects.filter(assign_to=doer, **look_this).count()
            planned_last = Model.objects.filter(assign_to=doer, **look_last).count()

            completed = Model.objects.filter(assign_to=doer, **look_this, status='Completed').count()
            completed_last = Model.objects.filter(assign_to=doer, **look_last, status='Completed').count()

            pct = round((completed / planned * 100), 2) if planned else 0
            pct_last = round((completed_last / planned_last * 100), 2) if planned_last else 0

            rows.append({
                'category': label,
                'last_pct': pct_last,
                'planned': planned,
                'completed': completed,
                'percent': pct,
            })

        # Average Score Calculation (for table at the bottom)
        checklist_planned = rows[0]['planned']
        checklist_completed = rows[0]['completed']
        checklist_ontime = Checklist.objects.filter(assign_to=doer, planned_date__date__range=(frm, to), status='Completed', completed_at__lte=F('planned_date')).count()
        checklist_pct = round((checklist_completed / checklist_planned * 100), 2) if checklist_planned else 0
        checklist_ontime_pct = round((checklist_ontime / checklist_planned * 100), 2) if checklist_planned else 0

        delegation_planned = rows[1]['planned']
        delegation_completed = rows[1]['completed']
        delegation_ontime = Delegation.objects.filter(assign_to=doer, planned_date__range=(frm, to), status='Completed', completed_at__lte=F('planned_date')).count()
        delegation_pct = round((delegation_completed / delegation_planned * 100), 2) if delegation_planned else 0
        delegation_ontime_pct = round((delegation_ontime / delegation_planned * 100), 2) if delegation_planned else 0

        avg_scores = {
            'checklist': checklist_pct,
            'delegation': delegation_pct,
            'average': round((checklist_pct + delegation_pct) / 2, 2),
            'checklist_ontime': checklist_ontime_pct,
            'delegation_ontime': delegation_ontime_pct,
            'average_ontime': round((checklist_ontime_pct + delegation_ontime_pct) / 2, 2)
        }

        pending_checklist = Checklist.objects.filter(assign_to=doer, planned_date__date__lt=frm, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=frm, status='Pending').count()

        delayed_checklist = Checklist.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()
        delayed_delegation = Delegation.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()

        try:
            phone = doer.profile.phone or ''
        except ObjectDoesNotExist:
            phone = ''
        try:
            dept = doer.profile.department or ''
        except ObjectDoesNotExist:
            dept = ''

        header = f"{doer.get_full_name().upper()} ({phone}) – {frm:%d %b, %Y} to {to:%d %b, %Y} [{dept}]"

    return render(request, 'reports/weekly_mis_score.html', {
        'form':               form,
        'commitment_form':    commitment_form,
        'commitment_message': commitment_message,
        'rows':               rows,
        'header':             header,
        'total_hours':        total_hours,
        'week_start':         week_start,
        'pending_checklist':  pending_checklist,
        'pending_delegation': pending_delegation,
        'delayed_checklist':  delayed_checklist,
        'delayed_delegation': delayed_delegation,
        'this_week_commitment': this_week_commitment if form.is_valid() and form.cleaned_data.get('doer') else None,
        'last_week_commitment': last_week_commitment if form.is_valid() and form.cleaned_data.get('doer') else None,
        'avg_scores':         avg_scores,
    })

@login_required
def performance_score(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    header = ''
    checklist_data = []
    delegation_data = []
    summary = {}
    time_checklist = ''
    time_delegation = ''
    total_hours = ''
    week_start = None
    pending_checklist = pending_delegation = delayed_checklist = delayed_delegation = 0

    if form.is_valid() and (form.cleaned_data.get('doer') or not request.user.is_staff):
        doer = form.cleaned_data.get('doer') or request.user
        frm, to = get_week_dates(form.cleaned_data.get('date_from'), form.cleaned_data.get('date_to'))
        week_start = frm

        qs = Checklist.objects.filter(assign_to=doer, planned_date__date__range=(frm, to))
        p = qs.count()
        completed = qs.filter(status='Completed').count()
        score_not = round((completed / p * 100) - 100, 2) if p else 0
        on_time = qs.filter(status='Completed', completed_at__lte=F('planned_date')).count()
        score_on = round((on_time / p * 100) - 100, 2) if p else 0
        checklist_data = [
            {'task_type': 'All work should be done',       'planned': p, 'completed': completed, 'pct': score_not},
            {'task_type': 'All work should be done ontime','planned': p, 'completed': on_time,   'pct': score_on},
        ]

        qs2 = Delegation.objects.filter(assign_to=doer, planned_date__range=(frm, to))
        p2 = qs2.count()
        completed2 = qs2.filter(status='Completed').count()
        score2_not = round((completed2 / p2 * 100) - 100, 2) if p2 else 0
        on_time2 = qs2.filter(status='Completed', completed_at__lte=F('planned_date')).count()
        score2_on = round((on_time2 / p2 * 100) - 100, 2) if p2 else 0
        delegation_data = [
            {'task_type': 'All work should be done',       'planned': p2, 'completed': completed2, 'pct': score2_not},
            {'task_type': 'All work should be done ontime','planned': p2, 'completed': on_time2,   'pct': score2_on},
        ]

        m_check = 0
        for rec in qs:
            m = rec.time_per_task_minutes or 0
            if rec.mode == 'Daily':
                sr = max(frm, rec.planned_date.date())
                days = (to - sr).days + 1
                occ = (days + rec.frequency - 1) // rec.frequency
            else:
                occ = 1
            m_check += m * occ
        time_checklist = f"{m_check//60:02d}:{m_check%60:02d}"

        m_deg = qs2.aggregate(total=Sum('time_per_task_minutes'))['total'] or 0
        time_delegation = f"{m_deg//60:02d}:{m_deg%60:02d}"

        total = m_check + m_deg
        total_hours = f"{total//60:02d}:{total%60:02d}"

        summary = {
            'checklist_avg':     score_not,
            'checklist_ontime':  score_on,
            'delegation_avg':    score2_not,
            'delegation_ontime': score2_on,
            'overall_avg':       round((score_not + score2_not) / 2, 2),
            'overall_ontime':    round((score_on  + score2_on)  / 2, 2),
        }

        pending_checklist = Checklist.objects.filter(assign_to=doer, planned_date__date__lt=frm, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=frm, status='Pending').count()
        delayed_checklist = Checklist.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()
        delayed_delegation = Delegation.objects.filter(
            assign_to=doer,
            completed_at__date__range=(frm, to),
            completed_at__gt=F('planned_date')
        ).count()

        try:
            phone = doer.profile.phone or ''
        except ObjectDoesNotExist:
            phone = ''
        try:
            dept = doer.profile.department or ''
        except ObjectDoesNotExist:
            dept = ''

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
