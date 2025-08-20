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


# -------------------------------------------------------------------
# Emoji-safe console logging helper (avoids UnicodeEncodeError on Windows)
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def is_working_day(dt_: date) -> bool:
    """Check if date is a working day (not Sunday and not holiday)."""
    try:
        return dt_.weekday() != 6 and not Holiday.objects.filter(date=dt_).exists()
    except Exception:
        return dt_.weekday() != 6


def next_working_day(dt_: date) -> date:
    """Find next working day from given date."""
    while not is_working_day(dt_):
        dt_ += timedelta(days=1)
    return dt_


def day_bounds(d: date):
    """Get start and end datetime for a given date in current TZ."""
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(d, dt_time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def span_bounds(d_from: date, d_to_inclusive: date):
    """Get start and end datetime for a date range (inclusive end date)."""
    start, _ = day_bounds(d_from)
    _, end = day_bounds(d_to_inclusive)
    return start, end


def _coerce_date_safe(dtor):
    """Safely convert various date/datetime objects to date."""
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


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime:
    """Get next planned date for recurring tasks, aligned to 10:00 IST and working days."""
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


def create_missing_recurring_checklist_tasks(user):
    """
    Create missing recurring checklist tasks for a user.
    NOTE: If you prefer NOT creating/emailing on dashboard load, remove the call to this function in dashboard_home().
    """
    now = timezone.now()

    seeds = (
        Checklist.objects.filter(assign_to=user, mode__in=RECURRING_MODES, status='Pending')
        .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
        .distinct()
    )

    for s in seeds:
        try:
            last = (
                Checklist.objects
                .filter(status='Pending', **s)
                .order_by('-planned_date', '-id')
                .first()
            )
            if not last:
                continue

            if Checklist.objects.filter(status='Pending', planned_date__gt=now, **s).exists():
                continue

            next_planned = get_next_planned_date(last.planned_date, last.mode, last.frequency)
            if not next_planned:
                continue

            safety = 0
            while next_planned and next_planned <= now and safety < 730:
                next_planned = get_next_planned_date(next_planned, last.mode, last.frequency)
                safety += 1
            if not next_planned:
                continue

            is_dupe = Checklist.objects.filter(
                assign_to_id=s['assign_to_id'],
                task_name=s['task_name'],
                mode=s['mode'],
                frequency=s['frequency'],
                group_name=s['group_name'],
                planned_date__gte=next_planned - timedelta(minutes=1),
                planned_date__lt=next_planned + timedelta(minutes=1),
                status='Pending',
            ).exists()
            if is_dupe:
                continue

            new_task = Checklist.objects.create(
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

            # ALWAYS send emails for recurring tasks
            try:
                from django.urls import reverse
                from apps.tasks.utils import (
                    send_checklist_assignment_to_user,
                    send_checklist_admin_confirmation,
                )
                from django.conf import settings

                site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
                complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_task.id])}"

                send_checklist_assignment_to_user(
                    task=new_task,
                    complete_url=complete_url,
                    subject_prefix="Recurring Checklist Generated",
                )
                send_checklist_admin_confirmation(
                    task=new_task,
                    subject_prefix="Recurring Checklist Generated",
                )
                logger.info(_safe_console_text(
                    f"Created and emailed recurring task: {new_task.task_name} for {new_task.assign_to}"
                ))
            except Exception as e:
                logger.error(_safe_console_text(f"Failed to send recurring task emails: {e}"))

        except Exception as e:
            logger.error(_safe_console_text(f"Failed to create recurring checklist for series {s}: {e}"))
            continue


def calculate_checklist_assigned_time(qs, date_from, date_to):
    """Calculate total assigned time for checklists in date range."""
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
    """Calculate total assigned time for delegations in date range."""
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
    """Convert minutes to HH:MM format."""
    try:
        h = int(minutes) // 60
        m = int(minutes) % 60
        return f"{h:02d}:{m:02d}"
    except (ValueError, TypeError):
        return "00:00"


@login_required
def dashboard_home(request):
    """
    COMPLETELY FIXED: Perfect dashboard filtering per requirements

    Dashboard Requirements:
    - Show all pending tasks with planned date <= today (including past tasks not completed)
    - Show tasks for today until they are completed
    - DO NOT show future tasks (planned for tomorrow or later) until the morning of their planned date at 10:00 AM IST
    """
    try:
        # If you prefer not to create/email recurrences here, comment the next line:
        create_missing_recurring_checklist_tasks(request.user)
    except Exception as e:
        logger.error(_safe_console_text(f"Error creating recurring tasks in dashboard: {e}"))

    # Current IST and today's date
    now_ist = timezone.now().astimezone(IST)
    today_ist = now_ist.date()
    project_tz = timezone.get_current_timezone()
    now_project_tz = now_ist.astimezone(project_tz)

    logger.info(_safe_console_text(f"Dashboard accessed by {request.user.username} at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}"))

    # Week boundaries for stats
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

    # Task lists (perfect cutoff logic)
    try:
        if today_only:
            # Lower bound: start of today IST; Upper bound: NOW (IST) -> converted to project TZ
            today_start = timezone.make_aware(datetime.combine(today_ist, dt_time.min), IST).astimezone(project_tz)

            checklist_qs = list(Checklist.objects.filter(
                assign_to=request.user, status='Pending',
                planned_date__gte=today_start,
                planned_date__lte=now_project_tz,  # ← show only up to NOW
            ).select_related('assign_by').order_by('planned_date'))

            delegation_qs = list(Delegation.objects.filter(
                assign_to=request.user, status='Pending',
                planned_date__gte=today_start,
                planned_date__lte=now_project_tz,
            ).select_related('assign_by').order_by('planned_date'))

            help_ticket_qs = list(HelpTicket.objects.filter(
                assign_to=request.user,
                planned_date__gte=today_start,
                planned_date__lte=now_project_tz,
            ).exclude(status='Closed').select_related('assign_by').order_by('planned_date'))

            cutoff_debug = f"{now_project_tz} (today up to NOW)"
        else:
            # End-of-today IST as cutoff
            end_of_today_ist = timezone.make_aware(datetime.combine(today_ist, dt_time.max), IST).astimezone(project_tz)

            checklist_qs = list(Checklist.objects.filter(
                assign_to=request.user, status='Pending',
                planned_date__lte=end_of_today_ist
            ).select_related('assign_by').order_by('planned_date'))

            delegation_qs = list(Delegation.objects.filter(
                assign_to=request.user, status='Pending',
                planned_date__lte=end_of_today_ist
            ).select_related('assign_by').order_by('planned_date'))

            help_ticket_qs = list(HelpTicket.objects.filter(
                assign_to=request.user, planned_date__lte=end_of_today_ist
            ).exclude(status='Closed').select_related('assign_by').order_by('planned_date'))

            cutoff_debug = str(end_of_today_ist)

        logger.info(_safe_console_text(
            f"Perfect dashboard filtering for {request.user.username}:\n"
            f"  - Today only: {today_only}\n"
            f"  - Found tasks: {len(checklist_qs)} checklists, {len(delegation_qs)} delegations, {len(help_ticket_qs)} help tickets\n"
            f"  - Cutoff: Tasks with planned_date <= {cutoff_debug}"
        ))
    except Exception as e:
        logger.error(_safe_console_text(f"Error querying task lists: {e}"))
        checklist_qs = []
        delegation_qs = []
        help_ticket_qs = []

    # Select which tasks to display
    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    # Time aggregations
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

    # Sample tasks for debug
    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            tdt = task.planned_date.astimezone(IST) if task.planned_date else None
            logger.info(_safe_console_text(
                f"  - Sample task {i}: '{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                f"planned for {tdt.strftime('%Y-%m-%d %H:%M IST') if tdt else 'No date'}"
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
