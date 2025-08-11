from datetime import timedelta, date as dt_date, datetime, time as dt_time
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from apps.tasks.models import Checklist, Delegation, HelpTicket

import pytz


def create_missing_recurring_checklist_tasks(user):
    """
    Ensures any missed recurring checklist occurrences exist up to 'today'.
    (This preserves each series' original time-of-day from its first item.)
    """
    ist = pytz.timezone('Asia/Kolkata')
    today = timezone.localdate()
    now = timezone.localtime(timezone.now(), ist)

    templates = (
        Checklist.objects.filter(assign_to=user, mode__in=['Daily', 'Weekly', 'Monthly', 'Yearly'])
        .values('task_name', 'mode', 'planned_date', 'frequency')
        .distinct()
    )

    for tmpl in templates:
        task_name = tmpl['task_name']
        mode = tmpl['mode']
        base_dt = tmpl['planned_date']
        freq = tmpl['frequency'] or 1

        if isinstance(base_dt, datetime):
            base_date = base_dt.date()
            planned_time = base_dt.time()
        else:
            base_date = base_dt
            planned_time = dt_time(10, 0)

        # Gather existing dates (ignore time) for this series
        existing_dates = set(
            Checklist.objects.filter(
                assign_to=user, task_name=task_name, mode=mode
            ).values_list('planned_date', flat=True)
        )
        existing_date_set = set(d.date() if isinstance(d, datetime) else d for d in existing_dates)

        # Generate occurrences up to 'today'
        gen_date = base_date
        while gen_date <= today:
            if gen_date not in existing_date_set:
                planned_dt = datetime.combine(gen_date, planned_time)
                planned_dt = ist.localize(planned_dt)

                template_qs = Checklist.objects.filter(assign_to=user, task_name=task_name, mode=mode).order_by('planned_date')
                if template_qs.exists():
                    template = template_qs.first()
                    Checklist.objects.create(
                        assign_by=template.assign_by,
                        task_name=task_name,
                        assign_to=user,
                        planned_date=planned_dt,
                        priority=template.priority,
                        attachment_mandatory=template.attachment_mandatory,
                        mode=mode,
                        frequency=freq,
                        time_per_task_minutes=template.time_per_task_minutes,
                        remind_before_days=template.remind_before_days,
                        message=template.message,
                        assign_pc=template.assign_pc,
                        group_name=template.group_name,
                        notify_to=template.notify_to,
                        auditor=template.auditor,
                        set_reminder=template.set_reminder,
                        reminder_mode=template.reminder_mode,
                        reminder_frequency=template.reminder_frequency,
                        reminder_before_days=template.reminder_before_days,
                        reminder_starting_time=template.reminder_starting_time,
                        checklist_auto_close=template.checklist_auto_close,
                        checklist_auto_close_days=template.checklist_auto_close_days,
                        actual_duration_minutes=0,
                    )

            # bump by recurrence
            if mode == 'Daily':
                gen_date += timedelta(days=freq)
            elif mode == 'Weekly':
                gen_date += timedelta(weeks=freq)
            elif mode == 'Monthly':
                # simple month step, clamp day to <= 28 to avoid invalid dates
                year = gen_date.year + ((gen_date.month + freq - 1) // 12)
                month = (gen_date.month + freq - 1) % 12 + 1
                day = min(gen_date.day, 28)
                gen_date = dt_date(year, month, day)
            elif mode == 'Yearly':
                gen_date = dt_date(gen_date.year + freq, gen_date.month, gen_date.day)
            else:
                break


@login_required
def dashboard_home(request):
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

    # Delegation counts (delegation.planned_date is a DateTimeField now; compare on date)
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

    delegation_qs = list(all_delegation)
    help_ticket_qs = list(all_help_ticket)

    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    # Checklist gating: hide before 10:00 or on Sunday
    if (selected == 'checklist' or not selected):
        if now_dt.weekday() == 6 or now_dt.time() < dt_time(hour=10, minute=0):
            tasks = []

    def calculate_checklist_assigned_time(qs, date_from, date_to):
        """
        Sum up expected minutes for checklist items in [date_from, date_to] by recurrence.
        """
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
                    year = d.year + (d.month // 12)
                    month = d.month % 12 + 1
                    try:
                        d = d.replace(year=year, month=month)
                    except Exception:
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
                    except Exception:
                        d = d.replace(year=d.year + 1, day=1)
                total_minutes += minutes * occur

            else:
                if start_date <= task.planned_date.date() <= end_date:
                    total_minutes += minutes

        return total_minutes

    def calculate_delegation_assigned_time(qs, date_from, date_to):
        """
        Delegation planned_date is DateTime; compare by date component to the [date_from, date_to] window.
        """
        total = 0
        for task in qs:
            pd = task.planned_date.date()
            if date_from <= pd <= date_to:
                total += (task.time_per_task_minutes or 0)
        return total

    def minutes_to_hhmm(minutes):
        h = minutes // 60
        m = minutes % 60
        return f"{int(h):02d}:{int(m):02d}"

    prev_min = calculate_checklist_assigned_time(checklist_qs, start_prev, end_prev)
    curr_min = calculate_checklist_assigned_time(checklist_qs, start_current, today)

    prev_min_del = calculate_delegation_assigned_time(
        Delegation.objects.filter(assign_to=request.user), start_prev, end_prev)
    curr_min_del = calculate_delegation_assigned_time(
        Delegation.objects.filter(assign_to=request.user), start_current, today)

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':     minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':    today_only,
    })
