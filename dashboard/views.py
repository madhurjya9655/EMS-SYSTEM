from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from apps.tasks.models import Checklist, Delegation, FMS
from apps.sales.models import SalesKPI
from apps.leave.models import LeaveRequest
from apps.petty_cash.models import PettyCashRequest
from apps.reimbursement.models import Reimbursement
from django.utils import timezone
from datetime import timedelta
from django.db.models import Sum

@login_required
def dashboard_home(request):
    user = request.user

    today = timezone.now().date()
    start_of_current_week = today - timedelta(days=today.weekday())
    start_of_previous_week = start_of_current_week - timedelta(days=7)
    end_of_previous_week = start_of_current_week - timedelta(days=1)

    # ─── WEEK SCORE ───
    # Checklist: count completed items in each week (no 'delay' field)
    current_checklist_ontime = Checklist.objects.filter(
        planned_date__gte=start_of_current_week,
        planned_date__lte=today,
        status='Completed'
    ).count()
    previous_checklist_ontime = Checklist.objects.filter(
        planned_date__gte=start_of_previous_week,
        planned_date__lte=end_of_previous_week,
        status='Completed'
    ).count()

    # Delegation: count all delegation entries in each week (no 'delay' or 'status' field)
    current_delegation_count = Delegation.objects.filter(
        planned_date__gte=start_of_current_week,
        planned_date__lte=today
    ).count()
    previous_delegation_count = Delegation.objects.filter(
        planned_date__gte=start_of_previous_week,
        planned_date__lte=end_of_previous_week
    ).count()

    # FMS: count completed & no‐delay items in each week
    current_fms_ontime = FMS.objects.filter(
        planned_date__gte=start_of_current_week,
        planned_date__lte=today,
        status='Completed',
        delay=0
    ).count()
    previous_fms_ontime = FMS.objects.filter(
        planned_date__gte=start_of_previous_week,
        planned_date__lte=end_of_previous_week,
        status='Completed',
        delay=0
    ).count()

    week_score = {
        "checklist": {"previous": previous_checklist_ontime, "current": current_checklist_ontime},
        "delegation": {"previous": previous_delegation_count, "current": current_delegation_count},
        "fms": {"previous": previous_fms_ontime, "current": current_fms_ontime},
    }

    # ─── PENDING TASK COUNTS ───
    pending_checklist = Checklist.objects.filter(status='Pending').count()
    pending_delegation = Delegation.objects.count()
    pending_fms = FMS.objects.filter(status='Pending').count()
    pending_help = 0
    pending_tasks = {
        "checklist": pending_checklist,
        "delegation": pending_delegation,
        "fms": pending_fms,
        "help_ticket": pending_help,
    }

    # ─── SALES “PLAN vs ACTUAL” FOR LAST 2 WEEKS ───
    two_weeks_ago = start_of_previous_week
    last_week = start_of_current_week

    sales_last_two_weeks = SalesKPI.objects.filter(
        metric='sales',
        period_type='weekly',
        period_start__gte=two_weeks_ago,
        period_end__lte=today
    )

    total_plan_wk1 = sales_last_two_weeks.filter(
        period_start__gte=two_weeks_ago,
        period_end__lte=end_of_previous_week
    ).aggregate(sum_plan=Sum("target"))["sum_plan"] or 0
    total_actual_wk1 = sales_last_two_weeks.filter(
        period_start__gte=two_weeks_ago,
        period_end__lte=end_of_previous_week
    ).aggregate(sum_actual=Sum("actual"))["sum_actual"] or 0

    total_plan_wk2 = sales_last_two_weeks.filter(
        period_start__gte=last_week,
        period_end__lte=today
    ).aggregate(sum_plan=Sum("target"))["sum_plan"] or 0
    total_actual_wk2 = sales_last_two_weeks.filter(
        period_start__gte=last_week,
        period_end__lte=today
    ).aggregate(sum_actual=Sum("actual"))["sum_actual"] or 0

    chart_labels = ["Two Weeks Earlier", "Previous Week"]
    chart_plan_values = [total_plan_wk1, total_plan_wk2]
    chart_actual_values = [total_actual_wk1, total_actual_wk2]

    # ─── LIST OF ALL CHECKLIST TASKS ───
    checklist_tasks = Checklist.objects.all().order_by("planned_date")

    context = {
        "week_score": week_score,
        "pending_tasks": pending_tasks,
        "chart_labels": chart_labels,
        "chart_plan_values": chart_plan_values,
        "chart_actual_values": chart_actual_values,
        "checklist_tasks": checklist_tasks,
    }
    return render(request, "dashboard/dashboard.html", context)
