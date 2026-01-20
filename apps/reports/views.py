from datetime import date, datetime, time, timedelta
from typing import Tuple

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Sum, F
from django.shortcuts import render, redirect
from django.utils import timezone

from .forms_reports import PCReportFilterForm, WeeklyMISCommitmentForm
from .models import WeeklyCommitment
from apps.tasks.models import Checklist, Delegation

User = get_user_model()


# -------- date helpers --------
def get_week_dates(frm: date | None, to: date | None) -> Tuple[date, date]:
    """
    Robust range builder:
    - If both dates given, return them in ascending order.
    - If only frm, make a 7-day window [frm, frm+6].
    - If only to,   make a 7-day window [to-6, to].
    - If neither,   current local week (Mon..Sun).
    """
    if frm and to:
        if frm > to:
            frm, to = to, frm
        return frm, to
    if frm and not to:
        return frm, frm + timedelta(days=6)
    if to and not frm:
        return to - timedelta(days=6), to
    today = timezone.localdate()
    start = today - timedelta(days=today.weekday())
    return start, start + timedelta(days=6)


def day_bounds(d: date):
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(d, time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def span_bounds(d_from: date, d_to_inclusive: date):
    s, _ = day_bounds(d_from)
    _, e = day_bounds(d_to_inclusive)
    return s, e


# -------- time helpers --------
def calculate_checklist_total_time(qs) -> int:
    # sum of actual time taken (minutes)
    return sum(task.actual_duration_minutes or 0 for task in qs)


def calculate_delegation_total_time(qs) -> int:
    # sum of actual time taken (minutes)
    return qs.aggregate(total=Sum('actual_duration_minutes'))['total'] or 0


def calculate_assigned_total_time(qs) -> int:
    # sum of planned/assigned minutes
    return qs.aggregate(total=Sum('time_per_task_minutes'))['total'] or 0


def minutes_to_hhmm(minutes: int) -> str:
    h = int(minutes) // 60
    m = int(minutes) % 60
    return f"{h:02d}:{m:02d}"


# -------- percentage helpers --------
def percent_not_completed(planned: int, completed: int) -> float:
    """
    Return NEGATIVE percentage for 'not completed'.
    Example: planned=5, completed=2 -> -(3/5*100) = -60.0
    """
    if planned == 0:
        return 0.0
    return -round(((planned - completed) / planned) * 100, 2)


# ----------------- REPORTS -----------------
@login_required
def list_doer_tasks(request):
    """
    Report of checklist tasks, with bulk and single delete.
    Adds optional Status filter (All/Pending/Completed).

    ISSUE 20 fix:
    - Persist filter state (session-based).
    - Redirect back to the same filtered URL after POST (bulk/single delete).
    """
    FILTER_SESSION_KEY = "reports__doer_tasks_filter"
    FILTER_KEYS = {"doer", "department", "date_from", "date_to", "status"}

    # ---- handle deletes (POST) ----
    if request.method == "POST":
        action = request.POST.get("action", "")
        return_url = request.POST.get("return_url") or request.META.get("HTTP_REFERER")

        if action == "bulk_delete":
            ids = request.POST.getlist("sel")
            if not ids:
                messages.warning(request, "No rows selected.")
                return redirect(return_url or "reports:doer_tasks")
            try:
                deleted, _ = Checklist.objects.filter(pk__in=ids).delete()
                if deleted:
                    messages.success(request, f"Deleted {deleted} task(s).")
                else:
                    messages.info(request, "No tasks were deleted.")
            except Exception as e:
                messages.error(request, f"Error during bulk delete: {e}")
            return redirect(return_url or "reports:doer_tasks")

        if action == "delete_one":
            pk = request.POST.get("pk")
            if pk:
                try:
                    obj = Checklist.objects.get(pk=pk)
                    obj.delete()
                    messages.success(request, "Task deleted.")
                except Checklist.DoesNotExist:
                    messages.warning(request, "The task no longer exists.")
                except Exception as e:
                    messages.error(request, f"Error deleting task: {e}")
            return redirect(return_url or "reports:doer_tasks")

    # ---- filtering (GET) with session persistence ----
    # Clear saved filters only when explicitly requested.
    if request.method == "GET" and request.GET.get("reset") == "1":
        request.session.pop(FILTER_SESSION_KEY, None)
        return redirect("reports:doer_tasks")

    effective_get = None
    if request.method == "GET":
        # If user provided any filter values, save them to session.
        incoming = {k: v for k, v in request.GET.items() if k in FILTER_KEYS and v}
        if incoming:
            request.session[FILTER_SESSION_KEY] = incoming
            effective_get = incoming
        else:
            # No new filters provided; reuse saved ones (if any).
            saved = request.session.get(FILTER_SESSION_KEY)
            if saved:
                effective_get = saved

    form = PCReportFilterForm(effective_get or (request.GET or None), user=request.user)
    items = Checklist.objects.select_related('assign_by', 'assign_to').order_by('planned_date', 'id')

    # Optional status filter (All/Pending/Completed)
    status_source = effective_get if effective_get is not None else request.GET
    status = (status_source.get("status") or "").strip() if status_source else ""
    valid_status = {"Pending", "Completed"}
    if status in valid_status:
        items = items.filter(status=status)

    if form.is_valid():
        d = form.cleaned_data
        if d.get('doer'):
            items = items.filter(assign_to=d['doer'])
        if d.get('department'):
            items = items.filter(assign_to__groups__name=d['department']).distinct()
        if d.get('date_from') and d.get('date_to'):
            s, e = span_bounds(d['date_from'], d['date_to'])
            items = items.filter(planned_date__gte=s, planned_date__lt=e)
        elif d.get('date_from'):
            s, _ = day_bounds(d['date_from'])
            items = items.filter(planned_date__gte=s)
        elif d.get('date_to'):
            _, e = day_bounds(d['date_to'])
            items = items.filter(planned_date__lt=e)

    return render(
        request,
        'reports/list_doer_tasks.html',
        {
            'form': form,
            'items': items,
            'current_status': status,  # so template can keep the selected option
        }
    )


@login_required
def list_fms_tasks(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    items = Delegation.objects.select_related('assign_by', 'assign_to').order_by('planned_date', 'id')
    if form.is_valid():
        d = form.cleaned_data
        if d.get('doer'):
            items = items.filter(assign_to=d['doer'])
        if d.get('department'):
            items = items.filter(assign_by__groups__name__icontains=d['department']).distinct()
        if d.get('date_from') and d.get('date_to'):
            s, e = span_bounds(d['date_from'], d['date_to'])
            items = items.filter(planned_date__gte=s, planned_date__lt=e)
        elif d.get('date_from'):
            s, _ = day_bounds(d['date_from'])
            items = items.filter(planned_date__gte=s)
        elif d.get('date_to'):
            _, e = day_bounds(d['date_to'])
            items = items.filter(planned_date__lt=e)
    return render(request, 'reports/list_fms_tasks.html', {'form': form, 'items': items})


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
    pending_checklist = pending_delegation = 0
    delayed_checklist = delayed_delegation = 0
    time_checklist = "00:00"          # planned/assigned
    time_delegation = "00:00"         # planned/assigned
    actual_time_checklist = "00:00"   # actual
    actual_time_delegation = "00:00"  # actual
    this_week_commitment = None
    last_week_commitment = None

    if form.is_valid() and form.cleaned_data.get('doer'):
        doer = form.cleaned_data['doer']

        # normalized date window
        frm, to = get_week_dates(form.cleaned_data.get('date_from'), form.cleaned_data.get('date_to'))

        week_start = frm
        prev_frm = frm - timedelta(days=7)
        prev_to = frm - timedelta(days=1)
        s_this, e_this = span_bounds(frm, to)
        s_prev, e_prev = span_bounds(prev_frm, prev_to)

        this_week_commitment = WeeklyCommitment.objects.filter(user=doer, week_start=frm).first()
        last_week_commitment = WeeklyCommitment.objects.filter(user=doer, week_start=prev_frm).first()

        # Save commitments
        if request.method == "POST" and 'update_commitment' in request.POST:
            commitment_form = WeeklyMISCommitmentForm(request.POST)
            if commitment_form.is_valid():
                cleaned = commitment_form.cleaned_data
                if not this_week_commitment:
                    this_week_commitment = WeeklyCommitment(user=doer, week_start=frm)
                this_week_commitment.checklist = cleaned.get("checklist") or 0
                this_week_commitment.checklist_desc = cleaned.get("checklist_desc") or ""
                this_week_commitment.checklist_ontime = cleaned.get("checklist_ontime") or 0
                this_week_commitment.checklist_ontime_desc = cleaned.get("checklist_ontime_desc") or ""
                this_week_commitment.delegation = cleaned.get("delegation") or 0
                this_week_commitment.delegation_desc = cleaned.get("delegation_desc") or ""
                this_week_commitment.delegation_ontime = cleaned.get("delegation_ontime") or 0
                this_week_commitment.delegation_ontime_desc = cleaned.get("delegation_ontime_desc") or ""
                this_week_commitment.fms = cleaned.get("fms") or 0
                this_week_commitment.fms_desc = cleaned.get("fms_desc") or ""
                this_week_commitment.audit = cleaned.get("audit") or 0
                this_week_commitment.audit_desc = cleaned.get("audit_desc") or ""
                this_week_commitment.save()
                commitment_message = "Commitment updated successfully."
                return redirect(request.path + "?" + request.META.get('QUERY_STRING', ''))
        else:
            initial = {}
            if this_week_commitment:
                initial = {
                    "checklist": this_week_commitment.checklist,
                    "checklist_desc": this_week_commitment.checklist_desc,
                    "checklist_ontime": this_week_commitment.checklist_ontime,
                    "checklist_ontime_desc": this_week_commitment.checklist_ontime_desc,
                    "delegation": this_week_commitment.delegation,
                    "delegation_desc": this_week_commitment.delegation_desc,
                    "delegation_ontime": this_week_commitment.delegation_ontime,
                    "delegation_ontime_desc": this_week_commitment.delegation_ontime_desc,
                    "fms": this_week_commitment.fms,
                    "fms_desc": this_week_commitment.fms_desc,
                    "audit": this_week_commitment.audit,
                    "audit_desc": this_week_commitment.audit_desc,
                }
            commitment_form = WeeklyMISCommitmentForm(initial=initial)

        # summary rows (this vs last) — negative % for not completed
        for Model, label in [(Checklist, 'Checklist'), (Delegation, 'Delegation')]:
            planned = Model.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this).count()
            planned_last = Model.objects.filter(assign_to=doer, planned_date__gte=s_prev, planned_date__lt=e_prev).count()
            completed = Model.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this, status='Completed').count()
            completed_last = Model.objects.filter(assign_to=doer, planned_date__gte=s_prev, planned_date__lt=e_prev, status='Completed').count()
            rows.append({
                'category': label,
                'last_pct': percent_not_completed(planned_last, completed_last),
                'planned': planned,
                'completed': completed,
                'percent': percent_not_completed(planned, completed),
            })

        checklist_qs = Checklist.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this).select_related('assign_to')
        delegation_qs = Delegation.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this).select_related('assign_to')

        # actual vs assigned times
        actual_checklist_minutes = calculate_checklist_total_time(checklist_qs)
        actual_delegation_minutes = calculate_delegation_total_time(delegation_qs)
        assigned_checklist_minutes = calculate_assigned_total_time(checklist_qs)
        assigned_delegation_minutes = calculate_assigned_total_time(delegation_qs)

        time_checklist = minutes_to_hhmm(assigned_checklist_minutes)
        time_delegation = minutes_to_hhmm(assigned_delegation_minutes)
        actual_time_checklist = minutes_to_hhmm(actual_checklist_minutes)
        actual_time_delegation = minutes_to_hhmm(actual_delegation_minutes)
        total_hours = minutes_to_hhmm(actual_checklist_minutes + actual_delegation_minutes)

        # % scores (negative for "not completed/on-time not met")
        checklist_planned = rows[0]['planned']
        checklist_completed = rows[0]['completed']
        checklist_ontime = Checklist.objects.filter(
            assign_to=doer,
            planned_date__gte=s_this,
            planned_date__lt=e_this,
            status='Completed',
            completed_at__lte=F('planned_date')
        ).count()
        checklist_pct = percent_not_completed(checklist_planned, checklist_completed)
        checklist_ontime_pct = percent_not_completed(checklist_planned, checklist_ontime)

        delegation_planned = rows[1]['planned']
        delegation_completed = rows[1]['completed']
        delegation_ontime = Delegation.objects.filter(
            assign_to=doer,
            planned_date__gte=s_this,
            planned_date__lt=e_this,
            status='Completed',
            completed_at__lte=F('planned_date')
        ).count()
        delegation_pct = percent_not_completed(delegation_planned, delegation_completed)
        delegation_ontime_pct = percent_not_completed(delegation_planned, delegation_ontime)

        avg_scores = {
            'checklist': checklist_pct,
            'delegation': delegation_pct,
            'average': round((checklist_pct + delegation_pct) / 2, 2),
            'checklist_ontime': checklist_ontime_pct,
            'delegation_ontime': delegation_ontime_pct,
            'average_ontime': round((checklist_ontime_pct + delegation_ontime_pct) / 2, 2),
        }

        pending_checklist = Checklist.objects.filter(assign_to=doer, planned_date__lt=s_this, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=s_this, status='Pending').count()
        delayed_checklist = Checklist.objects.filter(assign_to=doer, completed_at__gte=s_this, completed_at__lt=e_this, completed_at__gt=F('planned_date')).count()
        delayed_delegation = Delegation.objects.filter(assign_to=doer, completed_at__gte=s_this, completed_at__lt=e_this, completed_at__gt=F('planned_date')).count()

        full_name = (doer.get_full_name() or doer.username or '').upper()
        try:
            phone = getattr(getattr(doer, 'profile', None), 'phone', '') or ''
        except ObjectDoesNotExist:
            phone = ''
        try:
            dept = getattr(getattr(doer, 'profile', None), 'department', '') or ''
        except ObjectDoesNotExist:
            dept = ''
        header = f"{full_name} ({phone}) – {frm:%d %b, %Y} to {to:%d %b, %Y} [{dept}]"

    return render(request, 'reports/weekly_mis_score.html', {
        'form':                  form,
        'commitment_form':       commitment_form,
        'commitment_message':    commitment_message,
        'rows':                  rows,
        'header':                header,
        'total_hours':           total_hours,
        'week_start':            week_start,
        'pending_checklist':     pending_checklist,
        'pending_delegation':    pending_delegation,
        'delayed_checklist':     delayed_checklist,
        'delayed_delegation':    delayed_delegation,
        'this_week_commitment':  this_week_commitment if form.is_valid() and form.cleaned_data.get('doer') else None,
        'last_week_commitment':  last_week_commitment if form.is_valid() and form.cleaned_data.get('doer') else None,
        'avg_scores':            avg_scores,
        'time_checklist':        time_checklist,         # planned (assigned)
        'time_delegation':       time_delegation,        # planned (assigned)
        'actual_time_checklist': actual_time_checklist,  # actual
        'actual_time_delegation': actual_time_delegation,# actual
    })


@login_required
def performance_score(request):
    form = PCReportFilterForm(request.GET or None, user=request.user)
    header = ''
    checklist_data = []
    delegation_data = []
    summary = {}
    time_checklist = "00:00"
    time_delegation = "00:00"
    total_hours = "00:00"
    week_start = None
    pending_checklist = pending_delegation = delayed_checklist = delayed_delegation = 0

    if form.is_valid() and (form.cleaned_data.get('doer') or not request.user.is_staff):
        doer = form.cleaned_data.get('doer') or request.user

        # normalized date window
        frm, to = get_week_dates(form.cleaned_data.get('date_from'), form.cleaned_data.get('date_to'))
        week_start = frm
        s_this, e_this = span_bounds(frm, to)

        checklist_qs = Checklist.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this).select_related('assign_to')
        delegation_qs = Delegation.objects.filter(assign_to=doer, planned_date__gte=s_this, planned_date__lt=e_this).select_related('assign_to')

        # actual vs assigned minutes
        total_checklist_minutes = calculate_checklist_total_time(checklist_qs)
        total_delegation_minutes = calculate_delegation_total_time(delegation_qs)
        assigned_checklist_minutes = calculate_assigned_total_time(checklist_qs)
        assigned_delegation_minutes = calculate_assigned_total_time(delegation_qs)

        time_checklist = minutes_to_hhmm(assigned_checklist_minutes)   # planned (assigned)
        time_delegation = minutes_to_hhmm(assigned_delegation_minutes) # planned (assigned)
        total_hours = minutes_to_hhmm(total_checklist_minutes + total_delegation_minutes)

        p = checklist_qs.count()
        completed = checklist_qs.filter(status='Completed').count()
        score_not = percent_not_completed(p, completed)
        on_time = checklist_qs.filter(status='Completed', completed_at__lte=F('planned_date')).count()
        score_on = percent_not_completed(p, on_time)
        checklist_data = [
            {
                'task_type': 'All work should be done',
                'planned': p,  'completed': completed,
                'pct': score_not,  # negative
                'assigned_minutes': assigned_checklist_minutes,
                'actual_minutes': total_checklist_minutes
            },
            {
                'task_type': 'All work should be done on time',
                'planned': p,  'completed': on_time,
                'pct': score_on,   # negative
                'assigned_minutes': assigned_checklist_minutes,
                'actual_minutes': total_checklist_minutes
            },
        ]

        p2 = delegation_qs.count()
        completed2 = delegation_qs.filter(status='Completed').count()
        score2_not = percent_not_completed(p2, completed2)
        on_time2 = delegation_qs.filter(status='Completed', completed_at__lte=F('planned_date')).count()
        score2_on = percent_not_completed(p2, on_time2)
        delegation_data = [
            {
                'task_type': 'All work should be done',
                'planned': p2, 'completed': completed2,
                'pct': score2_not,  # negative
                'assigned_minutes': assigned_delegation_minutes,
                'actual_minutes': total_delegation_minutes
            },
            {
                'task_type': 'All work should be done on time',
                'planned': p2, 'completed': on_time2,
                'pct': score2_on,   # negative
                'assigned_minutes': assigned_delegation_minutes,
                'actual_minutes': total_delegation_minutes
            },
        ]

        summary = {
            'checklist_avg':     score_not,
            'checklist_ontime':  score_on,
            'delegation_avg':    score2_not,
            'delegation_ontime': score2_on,
            'overall_avg':       round((score_not + score2_not) / 2, 2),
            'overall_ontime':    round((score_on  + score2_on)  / 2, 2),
        }

        pending_checklist = Checklist.objects.filter(assign_to=doer, planned_date__lt=s_this, status='Pending').count()
        pending_delegation = Delegation.objects.filter(assign_to=doer, planned_date__lt=s_this, status='Pending').count()
        delayed_checklist = Checklist.objects.filter(assign_to=doer, completed_at__gte=s_this, completed_at__lt=e_this, completed_at__gt=F('planned_date')).count()
        delayed_delegation = Delegation.objects.filter(assign_to=doer, completed_at__gte=s_this, completed_at__lt=e_this, completed_at__gt=F('planned_date')).count()

        full_name = (doer.get_full_name() or doer.username or '').upper()
        try:
            phone = getattr(getattr(doer, 'profile', None), 'phone', '') or ''
        except ObjectDoesNotExist:
            phone = ''
        try:
            dept = getattr(getattr(doer, 'profile', None), 'department', '') or ''
        except ObjectDoesNotExist:
            dept = ''
        header = f"{full_name} ({phone}) – {frm:%d %b, %Y} to {to:%d %b, %Y} [{dept}]"

    return render(request, 'reports/performance_score.html', {
        'form':                form,
        'header':              header,
        'checklist_data':      checklist_data,
        'delegation_data':     delegation_data,
        'time_checklist':      time_checklist,   # planned (assigned)
        'time_delegation':     time_delegation,  # planned (assigned)
        'summary':             summary,
        'total_hours':         total_hours,      # sum of actuals
        'week_start':          week_start,
        'pending_checklist':   pending_checklist,
        'pending_delegation':  pending_delegation,
        'delayed_checklist':   delayed_checklist,
        'delayed_delegation':  delayed_delegation,
    })
