from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from datetime import timedelta
from apps.tasks.models import Checklist, Delegation, HelpTicket

@login_required
def dashboard_home(request):
    today = timezone.now().date()
    start_current = today - timedelta(days=today.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    # Week-over-week on-time counts
    curr_chk = Checklist.objects.filter(
        planned_date__date__gte=start_current,
        planned_date__date__lte=today,
        status='Completed'
    ).count()
    prev_chk = Checklist.objects.filter(
        planned_date__date__gte=start_prev,
        planned_date__date__lte=end_prev,
        status='Completed'
    ).count()

    curr_del = Delegation.objects.filter(
        planned_date__gte=start_current,
        planned_date__lte=today
    ).count()
    prev_del = Delegation.objects.filter(
        planned_date__gte=start_prev,
        planned_date__lte=end_prev
    ).count()

    curr_help = HelpTicket.objects.filter(
        planned_date__date__gte=start_current,
        planned_date__date__lte=today,
        status='Closed'
    ).count()
    prev_help = HelpTicket.objects.filter(
        planned_date__date__gte=start_prev,
        planned_date__date__lte=end_prev,
        status='Closed'
    ).count()

    week_score = {
        'checklist': {'previous': prev_chk, 'current': curr_chk},
        'delegation': {'previous': prev_del, 'current': curr_del},
        'help_ticket': {'previous': prev_help, 'current': curr_help},
    }

    # Pending tasks
    pending_tasks = {
        'checklist': Checklist.objects.filter(status='Pending').count(),
        'delegation': Delegation.objects.count(),
        'help_ticket': HelpTicket.objects.exclude(status='Closed').count(),
    }

    # Upcoming checklist tasks list
    checklist_tasks = Checklist.objects.all().order_by('planned_date')

    return render(request, 'dashboard/dashboard.html', {
        'week_score': week_score,
        'pending_tasks': pending_tasks,
        'checklist_tasks': checklist_tasks,
    })
