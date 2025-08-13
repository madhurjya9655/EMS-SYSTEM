from datetime import timedelta, datetime, time as dt_time
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.tasks.recurrence import get_next_planned_date  # same helper used by tasks app


RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']


def create_missing_recurring_checklist_tasks(user):
    """
    Ensure each recurring checklist *series* for this user has exactly ONE future
    'Pending' item. This mirrors the logic used in the tasks app.
    """
    now = timezone.now()

    # Identify series by (assignee, task_name, mode, frequency, group_name)
    seeds = (
        Checklist.objects.filter(assign_to=user, mode__in=RECURRING_MODES)
        .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
        .distinct()
    )

    for s in seeds:
        # The latest item in the series (any status)
        last = (
            Checklist.objects
            .filter(**s)
            .order_by('-planned_date', '-id')
            .first()
        )
        if not last:
            continue

        # If there is already a future pending item for this series, skip
        if Checklist.objects.filter(status='Pending', planned_date__gt=now, **s).exists():
            continue

        # Compute the next planned datetime (10:00 IST, skip Sun/holidays)
        next_planned = get_next_planned_date(last.planned_date, last.mode, last.frequency)
        if not next_planned:
            continue

        # De-dupe guard (±1 minute) for this series key
        dupe = Checklist.objects.filter(
            assign_to_id=s['assign_to_id'],
            task_name=s['task_name'],
            mode=s['mode'],
            frequency=s['frequency'],
            group_name=s['group_name'],
            planned_date__gte=next_planned - timedelta(minutes=1),
            planned_date__lt=next_planned + timedelta(minutes=1),
            status='Pending',
        ).exists()
        if dupe:
            continue

        Checklist.objects.create(
            assign_by=last.assign_by,
            task_name=last.task_name,
            message=last.message,
            assign_to=last.assign_to,
            planned_date=next_planned,
            priority=last.priority,
            attachment_mandatory=last.attachment_mandatory,
            mode=last.mode,
            frequency=last.frequency,
            time_per_task_minutes=last.time_per_task_minutes,
            remind_before_days=last.remind_before_days,
            assign_pc=last.assign_pc,
            notify_to=last.notify_to,
            set_reminder=last.set_reminder,
            reminder_mode=last.reminder_mode,
            reminder_frequency=last.reminder_frequency,
            reminder_starting_time=last.reminder_starting_time,
            checklist_auto_close=last.checklist_auto_close,
            checklist_auto_close_days=last.checklist_auto_close_days,
            group_name=getattr(last, 'group_name', None),
            actual_duration_minutes=0,
            status='Pending',
        )


@login_required
def dashboard_home(request):
    # Keep series healthy for this user (one future item per series)
    create_missing_recurring_checklist_tasks(request.user)

    now_dt = timezone.localtime()
    today = now_dt.date()

    # week ranges (Mon..Sun style)
    start_current = today - timedelta(days=today.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    # Checklist counts (by date portion)
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

    # Delegation counts (planned_date is DateTime; compare on date)
    curr_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_current,
        planned_date__date__lte=today
    ).count()
    prev_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__date__gte=start_prev,
        planned_date__date__lte=end_prev
    ).count()

    # Help ticket counts
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
        'help_ticket': {'previous': prev_help,  'current': curr_help},
    }

    pending_tasks = {
        'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
        'delegation':  Delegation.objects.filter(assign_to=request.user, status='Pending').count(),
        'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
    }

    selected   = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1' or request.GET.get('today_only') == '1')

    # Checklist list
    checklist_qs = Checklist.objects.filter(
        assign_to=request.user,
        status='Pending'
    ).order_by('planned_date')
    if today_only:
        checklist_qs = checklist_qs.filter(planned_date__date=today)

    # Delegation + Help tickets
    all_delegation = Delegation.objects.filter(assign_to=request.user, status='Pending').order_by('planned_date')
    if today_only:
        all_delegation = all_delegation.filter(planned_date__date=today)

    all_help_ticket = HelpTicket.objects.filter(
        assign_to=request.user
    ).exclude(status='Closed').order_by('planned_date')
    if today_only:
        all_help_ticket = all_help_ticket.filter(planned_date__date=today)

    if selected == 'delegation':
        tasks = list(all_delegation)
    elif selected == 'help_ticket':
        tasks = list(all_help_ticket)
    else:
        tasks = checklist_qs

    # Checklist gating: hide before 10:00 or on Sunday
    if (selected == 'checklist' or not selected):
        if now_dt.weekday() == 6 or now_dt.time() < dt_time(hour=10, minute=0):
            tasks = []

    def calculate_checklist_assigned_time(qs, date_from, date_to):
        """Sum expected minutes for checklist items in [date_from, date_to] by their recurrence."""
        total_minutes = 0
        for task in qs:
            mode = getattr(task, 'mode', 'Daily')
            freq = getattr(task, 'frequency', 1) or 1
            minutes = task.time_per_task_minutes or 0
            start_date = max(date_from, task.planned_date.date())
            end_date = date_to

            if mode == 'Daily':
                days = (end_date - start_date).days
                if days < 0:
                    continue
                occur = (days // freq) + 1
                total_minutes += minutes * occur

            elif mode == 'Weekly':
                occur = 0
                for i in range((end_date - start_date).days + 1):
                    d = start_date + timedelta(days=i)
                    if d.weekday() == task.planned_date.weekday() and ((d - task.planned_date.date()).days // 7) % freq == 0:
                        occur += 1
                total_minutes += minutes * occur

            elif mode == 'Monthly':
                occur = 0
                d = start_date
                while d <= end_date:
                    if d.day == task.planned_date.day:
                        months = (d.year - task.planned_date.year) * 12 + (d.month - task.planned_date.month)
                        if months % freq == 0:
                            occur += 1
                    # step month (safe)
                    year = d.year + (d.month // 12)
                    month = (d.month % 12) + 1
                    try:
                        d = d.replace(year=year, month=month)
                    except ValueError:
                        d = d.replace(year=year, month=month, day=1)
                total_minutes += minutes * occur

            elif mode == 'Yearly':
                occur = 0
                d = start_date
                while d <= end_date:
                    if d.month == task.planned_date.month and d.day == task.planned_date.day:
                        years = d.year - task.planned_date.year
                        if years % freq == 0:
                            occur += 1
                    try:
                        d = d.replace(year=d.year + 1)
                    except ValueError:
                        d = d.replace(year=d.year + 1, day=1)
                total_minutes += minutes * occur

            else:
                if start_date <= task.planned_date.date() <= end_date:
                    total_minutes += minutes

        return total_minutes

    def calculate_delegation_assigned_time(qs, date_from, date_to):
        """Delegation planned_date is DateTime; compare by date component within [date_from, date_to]."""
        total = 0
        for task in qs:
            pd = task.planned_date.date()
            if date_from <= pd <= date_to:
                total += (task.time_per_task_minutes or 0)
        return total

    def minutes_to_hhmm(minutes):
        h = int(minutes) // 60
        m = int(minutes) % 60
        return f"{h:02d}:{m:02d}"

    prev_min = calculate_checklist_assigned_time(
        Checklist.objects.filter(assign_to=request.user, status='Pending'), start_prev, end_prev
    )
    curr_min = calculate_checklist_assigned_time(
        Checklist.objects.filter(assign_to=request.user, status='Pending'), start_current, today
    )

    prev_min_del = calculate_delegation_assigned_time(
        Delegation.objects.filter(assign_to=request.user), start_prev, end_prev
    )
    curr_min_del = calculate_delegation_assigned_time(
        Delegation.objects.filter(assign_to=request.user), start_current, today
    )

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':     minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':    today_only,
    })
