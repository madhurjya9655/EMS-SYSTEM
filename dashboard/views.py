# E:\CLIENT PROJECT\employee management system bos\employee_management_system\dashboard\views.py
from datetime import timedelta, datetime, time as dt_time, date
import logging
import sys

import pytz
from dateutil.relativedelta import relativedelta

from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.shortcuts import render

from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']
IST = pytz.timezone('Asia/Kolkata')
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0


# ----------------------------- logging helper -----------------------------
def _safe_console_text(s: object) -> str:
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    enc = getattr(sys.stderr, "encoding", None) or getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return text.encode("ascii", errors="replace").decode("ascii", errors="replace")


# ----------------------------- small helpers ------------------------------
def is_working_day(dt_: date) -> bool:
    try:
        return dt_.weekday() != 6 and not Holiday.objects.filter(date=dt_).exists()
    except Exception:
        return dt_.weekday() != 6


def next_working_day(dt_: date) -> date:
    while not is_working_day(dt_):
        dt_ += timedelta(days=1)
    return dt_


def day_bounds(d: date):
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(d, dt_time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def span_bounds(d_from: date, d_to_inclusive: date):
    start, _ = day_bounds(d_from)
    _, end = day_bounds(d_to_inclusive)
    return start, end


def _coerce_date_safe(dtor):
    if dtor is None:
        return None
    if isinstance(dtor, date) and not isinstance(dtor, datetime):
        return dtor
    if isinstance(dtor, datetime):
        return (timezone.localtime(dtor) if timezone.is_aware(dtor) else dtor).date()
    if isinstance(dtor, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(dtor, fmt).date()
            except ValueError:
                continue
    try:
        if hasattr(dtor, "date"):
            return dtor.date()
    except Exception:
        pass
    return timezone.localdate()


def _ist_date(dt: datetime) -> date | None:
    """Return the date in IST for an aware/naive datetime."""
    if not dt:
        return None
    if timezone.is_aware(dt):
        return dt.astimezone(IST).date()
    return IST.localize(dt).date()


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime:
    if (mode or '') not in RECURRING_MODES:
        return None

    tz = timezone.get_current_timezone()
    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, tz)

    cur_ist = prev_dt.astimezone(IST)
    step = max(int(frequency or 1), 1)

    if mode == 'Daily':
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == 'Weekly':
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == 'Monthly':
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == 'Yearly':
        cur_ist = cur_ist + relativedelta(years=step)

    cur_ist = cur_ist.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)
    while not is_working_day(cur_ist.date()):
        cur_ist = cur_ist + relativedelta(days=1)
        cur_ist = cur_ist.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)

    return cur_ist.astimezone(tz)


# ---------- Checklist visibility gating (shared with apps.tasks.views) ----------
def _should_show_checklist(task_dt: datetime, now_ist: datetime) -> bool:
    """
    FINAL rule:
      • planned date < today IST  → visible
      • planned date > today IST  → hide
      • planned date == today IST → visible ONLY from 10:00 IST
    """
    if not task_dt:
        return False

    dt_ist = task_dt.astimezone(IST) if timezone.is_aware(task_dt) else IST.localize(task_dt)
    task_date = dt_ist.date()
    today = now_ist.date()

    if task_date < today:
        return True
    if task_date > today:
        return False

    ten_am = dt_time(10, 0, 0)
    return now_ist.timetz().replace(tzinfo=None) >= ten_am


# ------------------------------ aggregations ------------------------------
def calculate_checklist_assigned_time(qs, date_from, date_to):
    total_minutes = 0

    date_from = _coerce_date_safe(date_from)
    date_to = _coerce_date_safe(date_to)

    for task in qs:
        try:
            mode = getattr(task, 'mode', '') or ''
            freq = getattr(task, 'frequency', 1) or 1
            minutes = task.time_per_task_minutes or 0
            task_date = _coerce_date_safe(task.planned_date)

            if task_date > date_to:
                continue

            start_date = max(date_from, task_date)
            end_date = date_to

            if mode == 'Daily':
                days = (end_date - start_date).days
                if days >= 0:
                    occur = (days // freq) + 1
                    total_minutes += minutes * occur

            elif mode == 'Weekly':
                delta_weeks = ((end_date - start_date).days // 7)
                if delta_weeks >= 0:
                    occur = (delta_weeks // freq) + 1
                    total_minutes += minutes * occur

            elif mode == 'Monthly':
                months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                if end_date.day < start_date.day:
                    months -= 1
                if months >= 0:
                    occur = (months // freq) + 1
                    total_minutes += minutes * occur

            elif mode == 'Yearly':
                years = end_date.year - start_date.year
                if (end_date.month, end_date.day) < (start_date.month, start_date.day):
                    years -= 1
                if years >= 0:
                    occur = (years // freq) + 1
                    total_minutes += minutes * occur

            else:
                if start_date <= task_date <= end_date:
                    total_minutes += minutes

        except Exception as e:
            logger.error(_safe_console_text(f"Error calculating checklist time for task {getattr(task, 'id', '?')}: {e}"))
            continue

    return total_minutes


def calculate_delegation_assigned_time_safe(assign_to_user, date_from, date_to):
    total = 0

    try:
        date_from = _coerce_date_safe(date_from)
        date_to = _coerce_date_safe(date_to)

        start_dt, end_dt = span_bounds(date_from, date_to)

        delegations = Delegation.objects.filter(
            assign_to=assign_to_user,
            planned_date__gte=start_dt,
            planned_date__lt=end_dt,
            status='Pending'
        )

        for d in delegations:
            try:
                planned_date = _coerce_date_safe(d.planned_date)
                if date_from <= planned_date <= date_to:
                    total += d.time_per_task_minutes or 0
            except Exception as e:
                logger.error(_safe_console_text(f"Error calculating delegation time for delegation {getattr(d, 'id', '?')}: {e}"))
                continue
    except Exception as e:
        logger.error(_safe_console_text(f"Error in delegation time calculation: {e}"))

    return total


def minutes_to_hhmm(minutes):
    try:
        h = int(minutes) // 60
        m = int(minutes) % 60
        return f"{h:02d}:{m:02d}"
    except (ValueError, TypeError):
        return "00:00"


# --------------------------------- view -----------------------------------
@login_required
def dashboard_home(request):
    """
    Checklist (recurring or one-time):
      • visible if past due
      • visible today only after 10:00 IST
      • hide future
    Delegation & Help Ticket: visible at/after their planned timestamp.
    """
    now_ist = timezone.now().astimezone(IST)
    today_ist = now_ist.date()

    project_tz = timezone.get_current_timezone()
    now_project_tz = now_ist.astimezone(project_tz)

    logger.info(_safe_console_text(f"Dashboard accessed by {request.user.username} at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}"))

    # Week windows (Mon..Sun)
    start_current = today_ist - timedelta(days=today_ist.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    curr_start_dt, curr_end_dt = span_bounds(start_current, today_ist)
    prev_start_dt, prev_end_dt = span_bounds(start_prev, end_prev)

    # Weekly scores
    try:
        curr_chk = Checklist.objects.filter(
            assign_to=request.user, planned_date__gte=curr_start_dt,
            planned_date__lt=curr_end_dt, status='Completed',
        ).count()
        prev_chk = Checklist.objects.filter(
            assign_to=request.user, planned_date__gte=prev_start_dt,
            planned_date__lt=prev_end_dt, status='Completed',
        ).count()

        curr_del = Delegation.objects.filter(
            assign_to=request.user, planned_date__gte=curr_start_dt,
            planned_date__lt=curr_end_dt, status='Completed',
        ).count()
        prev_del = Delegation.objects.filter(
            assign_to=request.user, planned_date__gte=prev_start_dt,
            planned_date__lt=prev_end_dt, status='Completed',
        ).count()

        curr_help = HelpTicket.objects.filter(
            assign_to=request.user, planned_date__gte=curr_start_dt,
            planned_date__lt=curr_end_dt, status='Closed',
        ).count()
        prev_help = HelpTicket.objects.filter(
            assign_to=request.user, planned_date__gte=prev_start_dt,
            planned_date__lt=prev_end_dt, status='Closed',
        ).count()
    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating weekly scores: {e}"))
        curr_chk = prev_chk = curr_del = prev_del = curr_help = prev_help = 0

    week_score = {
        'checklist':   {'previous': prev_chk,   'current': curr_chk},
        'delegation':  {'previous': prev_del,   'current': curr_del},
        'help_ticket': {'previous': prev_help,  'current': curr_help},
    }

    # Pending counts
    try:
        pending_tasks = {
            'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
            'delegation':  Delegation.objects.filter(assign_to=request.user, status='Pending').count(),
            'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
        }
    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating pending counts: {e}"))
        pending_tasks = {'checklist': 0, 'delegation': 0, 'help_ticket': 0}

    selected = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1' or request.GET.get('today_only') == '1')

    # Build IST-aligned bounds once
    start_today_proj = timezone.make_aware(datetime.combine(today_ist, dt_time.min), IST).astimezone(project_tz)
    end_today_proj = timezone.make_aware(datetime.combine(today_ist, dt_time.max), IST).astimezone(project_tz)

    try:
        # -------------------- Checklists --------------------
        # IMPORTANT: always fetch up to EOD, then do the "today" filter in Python by IST date
        base_checklists = (
            Checklist.objects
            .filter(assign_to=request.user, status='Pending', planned_date__lte=end_today_proj)
            .select_related('assign_by', 'assign_to')  # ensure modal fields are warm
            .order_by('planned_date')
        )

        if today_only:
            checklist_qs = [
                c for c in base_checklists
                if (_ist_date(c.planned_date) == today_ist) and _should_show_checklist(c.planned_date, now_ist)
            ]
        else:
            checklist_qs = [
                c for c in base_checklists
                if _should_show_checklist(c.planned_date, now_ist)
            ]

        # ------------------- Delegations / Help Tickets -------------------
        if today_only:
            # show only those whose planned time has arrived
            delegation_qs = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                ).select_related('assign_by', 'assign_to').order_by('planned_date')
            )
            help_ticket_qs = list(
                HelpTicket.objects.filter(
                    assign_to=request.user,
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                ).exclude(status='Closed').select_related('assign_by', 'assign_to').order_by('planned_date')
            )
        else:
            delegation_qs = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__lte=end_today_proj
                ).select_related('assign_by', 'assign_to').order_by('planned_date')
            )
            help_ticket_qs = list(
                HelpTicket.objects.filter(
                    assign_to=request.user, planned_date__lte=end_today_proj
                ).exclude(status='Closed').select_related('assign_by', 'assign_to').order_by('planned_date')
            )

        logger.info(_safe_console_text(
            f"Dashboard filter for {request.user.username} | today_only={today_only} "
            f"| checklist={len(checklist_qs)} delegation={len(delegation_qs)} help={len(help_ticket_qs)}"
        ))
    except Exception as e:
        logger.error(_safe_console_text(f"Error querying task lists: {e}"))
        checklist_qs = []
        delegation_qs = []
        help_ticket_qs = []

    # choose tab
    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    # time aggregates
    try:
        prev_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(assign_to=request.user, status='Pending'),
            start_prev, end_prev
        )
        curr_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(assign_to=request.user, status='Pending'),
            start_current, today_ist
        )
        prev_min_del = calculate_delegation_assigned_time_safe(request.user, start_prev, end_prev)
        curr_min_del = calculate_delegation_assigned_time_safe(request.user, start_current, today_ist)
    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating time aggregations: {e}"))
        prev_min = curr_min = prev_min_del = curr_min_del = 0

    # small sample log
    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            tdt = task.planned_date.astimezone(IST) if task.planned_date else None
            logger.info(_safe_console_text(
                f"  sample {i}: '{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                f"@ {tdt.strftime('%Y-%m-%d %H:%M IST') if tdt else 'No date'}"
            ))

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':     minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':    today_only,
    })
