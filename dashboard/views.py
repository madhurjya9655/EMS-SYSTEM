from datetime import timedelta
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from apps.tasks.models import Checklist, Delegation, HelpTicket

@login_required
def dashboard_home(request):
    today         = timezone.now().date()
    start_current = today - timedelta(days=today.weekday())
    start_prev    = start_current - timedelta(days=7)
    end_prev      = start_current - timedelta(days=1)

    # ONLY tasks assigned TO the current user
    curr_chk = Checklist.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_current,
        planned_date__date__lte=today,
        status='Completed'
    ).count()
    prev_chk = Checklist.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_prev,
        planned_date__date__lte=end_prev,
        status='Completed'
    ).count()

    curr_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__gte=start_current,
        planned_date__lte=today
    ).count()
    prev_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__gte=start_prev,
        planned_date__lte=end_prev
    ).count()

    curr_help = HelpTicket.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_current,
        planned_date__date__lte=today,
        status='Closed'
    ).count()
    prev_help = HelpTicket.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_prev,
        planned_date__date__lte=end_prev,
        status='Closed'
    ).count()

    week_score = {
        'checklist':   {'previous': prev_chk, 'current': curr_chk},
        'delegation':  {'previous': prev_del, 'current': curr_del},
        'help_ticket': {'previous': prev_help,'current': curr_help},
    }

    pending_tasks = {
        'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
        'delegation':  Delegation.objects.filter(assign_to=request.user).count(),
        'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
    }

    selected   = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1')

    # Task-lists also scoped to request.user
    checklist_qs   = Checklist.objects.filter(assign_to=request.user).order_by('planned_date')
    delegation_qs  = Delegation.objects.filter(assign_to=request.user).order_by('planned_date')
    help_ticket_qs = HelpTicket.objects.filter(assign_to=request.user).order_by('planned_date')

    if today_only:
        checklist_qs   = checklist_qs.filter(planned_date__date=today)
        delegation_qs  = delegation_qs.filter(planned_date=today)
        help_ticket_qs = help_ticket_qs.filter(planned_date__date=today)

    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    def total_minutes(qs, wk_start, wk_end):
        total = 0
        for rec in qs:
            pd = rec.planned_date.date()
            if pd > wk_end: 
                continue
            mins_each = getattr(rec, 'time_per_task_minutes', 0)
            if getattr(rec, 'mode', '') == 'Daily':
                run_start = max(wk_start, pd)
                days = (wk_end - run_start).days + 1
                occ  = (days + rec.frequency - 1) // rec.frequency
            else:
                occ = 1 if wk_start <= pd <= wk_end else 0
            total += mins_each * occ
        return total

    prev_min = total_minutes(checklist_qs, start_prev, end_prev)
    curr_min = total_minutes(checklist_qs, start_current, today)

    def fmt(minutes):
        h, m = divmod(minutes, 60)
        return f"{h:02d}:{m:02d}"

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     fmt(prev_min),
        'curr_time':     fmt(curr_min),
        'today_only':    today_only,
    })
