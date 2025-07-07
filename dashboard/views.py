# dashboard/views.py

from datetime import timedelta, time as dt_time
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from apps.tasks.models import Checklist, Delegation, HelpTicket

def occurs_on_date(check, date):
    """
    Return True if a recurring Checklist/Delegation 'check'
    should occur on the given 'date', based on its mode/frequency.
    """
    pd = check.planned_date.date() if hasattr(check.planned_date, 'date') else check.planned_date
    if date < pd:
        return False
    freq = check.frequency or 1
    mode = check.mode
    delta = (date - pd).days

    if mode == 'Daily':
        return delta % freq == 0

    if mode == 'Weekly':
        if date.weekday() != pd.weekday():
            return False
        return (delta // 7) % freq == 0

    if mode == 'Monthly':
        if date.day != pd.day:
            return False
        months = (date.year - pd.year) * 12 + (date.month - pd.month)
        return months % freq == 0

    if mode == 'Yearly':
        if date.month != pd.month or date.day != pd.day:
            return False
        return (date.year - pd.year) % freq == 0

    return False


@login_required
def dashboard_home(request):
    now_dt = timezone.localtime()
    today = now_dt.date()

    # ---- weekly windows ----
    start_current = today - timedelta(days=today.weekday())
    start_prev    = start_current - timedelta(days=7)
    end_prev      = start_current - timedelta(days=1)

    # ---- week‐score counts ----
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
        'checklist':   {'previous': prev_chk,   'current': curr_chk},
        'delegation':  {'previous': prev_del,   'current': curr_del},
        'help_ticket': {'previous': prev_help, 'current': curr_help},
    }

    # ---- pending counts (non‐closed) ----
    pending_tasks = {
        'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
        'delegation':  Delegation.objects.filter(assign_to=request.user).count(),
        'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
    }

    # ---- UI controls ----
    selected   = request.GET.get('task_type')         # 'checklist', 'delegation', or 'help_ticket'
    today_only = (request.GET.get('today') == '1')    # toggle

    # ---- master querysets (only open help‐tickets) ----
    all_checklist  = Checklist.objects.filter(assign_to=request.user).order_by('planned_date')
    all_delegation = Delegation.objects.filter(assign_to=request.user).order_by('planned_date')
    all_help_ticket = HelpTicket.objects.filter(
        assign_to=request.user
    ).exclude(status='Closed').order_by('planned_date')

    # ---- “Today Only” filter ----
    if today_only:
        checklist_qs   = [c for c in all_checklist  if occurs_on_date(c, today)]
        delegation_qs  = [d for d in all_delegation if occurs_on_date(d, today)]
        help_ticket_qs = [h for h in all_help_ticket if h.planned_date.date() == today]
    else:
        checklist_qs   = list(all_checklist)
        delegation_qs  = list(all_delegation)
        help_ticket_qs = list(all_help_ticket)

    # ---- pick which list to show ----
    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    # ---- SUNDAY or BEFORE 10 AM rule only for checklist/delegation ----
    if selected in ('checklist', 'delegation'):
        if now_dt.weekday() == 6:
            tasks = []
        elif now_dt.time() < dt_time(hour=10, minute=0):
            tasks = [t for t in tasks if t.created_at.date() == today]

    # ---- compute weekly time spent for checklist ----
    def total_minutes(qs, wk_start, wk_end):
        total = 0
        for rec in qs:
            pd = rec.planned_date.date() if hasattr(rec.planned_date, 'date') else rec.planned_date
            if pd > wk_end:
                continue
            mins_each = getattr(rec, 'time_per_task_minutes', 0)
            if getattr(rec, 'mode', '') == 'Daily':
                run_start = max(wk_start, pd)
                days = (wk_end - run_start).days + 1
                occ = (days + rec.frequency - 1) // rec.frequency
            else:
                occ = 1 if wk_start <= pd <= wk_end else 0
            total += mins_each * occ
        return total

    prev_min = total_minutes(all_checklist, start_prev, end_prev)
    curr_min = total_minutes(all_checklist, start_current, today)

    def fmt(minutes):
        h, m = divmod(minutes, 60)
        return f"{h:02d}:{m:02d}"

    # ---- render ----
    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     fmt(prev_min),
        'curr_time':     fmt(curr_min),
        'today_only':    today_only,
    })
