from datetime import timedelta, datetime, time as dt_time, date
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db.models.functions import Cast
from django.db.models import CharField
import pytz
from dateutil.relativedelta import relativedelta

from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.settings.models import Holiday

RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']
IST = pytz.timezone('Asia/Kolkata')
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0


# -----------------------------
# Working day helpers
# -----------------------------
def is_working_day(dt: date) -> bool:
    """True if the given date is not Sunday and not a Holiday."""
    return dt.weekday() != 6 and not Holiday.objects.filter(date=dt).exists()

def next_working_day(dt: date) -> date:
    """Return the next working date on/after dt."""
    while not is_working_day(dt):
        dt += timedelta(days=1)
    return dt


# -----------------------------
# Time helpers (UDF-safe)
# -----------------------------
def day_bounds(d):
    """
    Given a date `d`, return timezone-aware [start, end) datetimes:
      start = d 00:00:00
      end   = (d + 1 day) 00:00:00
    """
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(d, dt_time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def span_bounds(d_from, d_to_inclusive):
    """
    Convert an inclusive date span [d_from, d_to_inclusive] into
    timezone-aware datetime bounds [start, end) suitable for filtering:
      planned_date__gte=start, planned_date__lt=end
    """
    start, _ = day_bounds(d_from)
    _, end = day_bounds(d_to_inclusive)
    return start, end


# -----------------------------
# Recurrence helper (fixed)
# -----------------------------
def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime:
    """
    FIXED: Get next recurring datetime for dashboard use.
    Step by mode/freq; set time to 10:00 IST; skip Sundays/holidays.
    Return aware in project tz.
    """
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


# -----------------------------
# Recurring checklist guard (fixed)
# -----------------------------
def create_missing_recurring_checklist_tasks(user):
    """
    FIXED: Ensure each recurring checklist *series* for this user has exactly ONE future
    'Pending' item. Uses the same logic as the main tasks views.
    """
    now = timezone.now()

    seeds = (
        Checklist.objects.filter(assign_to=user, mode__in=RECURRING_MODES)
        .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
        .distinct()
    )

    for s in seeds:
        last = (
            Checklist.objects
            .filter(**s)
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

        # Safety: if next is still not in the future, advance until it is
        safety = 0
        while next_planned and next_planned <= now and safety < 730:
            next_planned = get_next_planned_date(next_planned, last.mode, last.frequency)
            safety += 1
        if not next_planned:
            continue

        # De-dupe guard (±1 minute)
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

        try:
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
        except Exception as e:
            # Silently handle any creation errors to prevent dashboard crashes
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to create recurring checklist: {e}")


# -----------------------------
# Safe date helpers for SQLite
# -----------------------------
def _coerce_date_safe(dtor):
    """
    CRITICAL FIX: Safely convert datetime/date to date object.
    Ensures we only pass date objects to SQLite, never datetime objects
    that cause 'fromisoformat: argument must be str' errors.
    """
    if dtor is None:
        return None
    
    # If it's already a date object, return as-is
    if isinstance(dtor, date) and not isinstance(dtor, datetime):
        return dtor
    
    # If it's a datetime, extract the date component
    if isinstance(dtor, datetime):
        # Convert aware datetime to local date to avoid timezone issues
        if timezone.is_aware(dtor):
            local_dt = timezone.localtime(dtor)
            return local_dt.date()
        else:
            return dtor.date()
    
    # If it's a string, try to parse it
    if isinstance(dtor, str):
        try:
            parsed = datetime.strptime(dtor, '%Y-%m-%d').date()
            return parsed
        except ValueError:
            try:
                parsed = datetime.strptime(dtor, '%Y-%m-%d %H:%M:%S').date()
                return parsed
            except ValueError:
                pass
    
    # Fallback: try to extract date from whatever we have
    try:
        if hasattr(dtor, 'date'):
            return dtor.date()
    except Exception:
        pass
    
    # Last resort: return today's date
    return timezone.localdate()


# -----------------------------
# Dashboard (completely fixed)
# -----------------------------
@login_required
def dashboard_home(request):
    # Keep series healthy for this user (one future item per series)
    try:
        create_missing_recurring_checklist_tasks(request.user)
    except Exception as e:
        # Don't let recurring task creation crash the dashboard
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error creating recurring tasks in dashboard: {e}")

    now_dt = timezone.localtime()
    today = now_dt.date()

    # Week ranges (Mon..Sun)
    start_current = today - timedelta(days=today.weekday())  # this Monday
    start_prev = start_current - timedelta(days=7)           # previous Monday
    end_prev = start_current - timedelta(days=1)             # last Sunday

    # Convert inclusive date spans to datetime bounds (UDF-safe)
    curr_start_dt, curr_end_dt = span_bounds(start_current, today)
    prev_start_dt, prev_end_dt = span_bounds(start_prev, end_prev)

    # ---------- Weekly scores (counts) ----------
    # Checklist: Completed this week vs previous week
    curr_chk = Checklist.objects.filter(
        assign_to=request.user,
        planned_date__gte=curr_start_dt,
        planned_date__lt=curr_end_dt,
        status='Completed',
    ).count()
    prev_chk = Checklist.objects.filter(
        assign_to=request.user,
        planned_date__gte=prev_start_dt,
        planned_date__lt=prev_end_dt,
        status='Completed',
    ).count()

    # Delegation: count planned items (status-agnostic, as before)
    curr_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__gte=curr_start_dt,
        planned_date__lt=curr_end_dt,
    ).count()
    prev_del = Delegation.objects.filter(
        assign_to=request.user,
        planned_date__gte=prev_start_dt,
        planned_date__lt=prev_end_dt,
    ).count()

    # Help tickets: Closed this week vs previous week
    curr_help = HelpTicket.objects.filter(
        assign_to=request.user,
        planned_date__gte=curr_start_dt,
        planned_date__lt=curr_end_dt,
        status='Closed',
    ).count()
    prev_help = HelpTicket.objects.filter(
        assign_to=request.user,
        planned_date__gte=prev_start_dt,
        planned_date__lt=prev_end_dt,
        status='Closed',
    ).count()

    week_score = {
        'checklist':   {'previous': prev_chk,   'current': curr_chk},
        'delegation':  {'previous': prev_del,   'current': curr_del},
        'help_ticket': {'previous': prev_help,  'current': curr_help},
    }

    # ---------- Pending counts ----------
    pending_tasks = {
        'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
        'delegation':  Delegation.objects.filter(assign_to=request.user, status='Pending').count(),
        'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
    }

    # ---------- Lists (with optional "today only") ----------
    selected   = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1' or request.GET.get('today_only') == '1')

    if today_only:
        t_start, t_end = day_bounds(today)

    # Checklist list (Pending)
    checklist_qs = Checklist.objects.filter(
        assign_to=request.user,
        status='Pending'
    ).order_by('planned_date')
    if today_only:
        checklist_qs = checklist_qs.filter(planned_date__gte=t_start, planned_date__lt=t_end)

    # Delegation list (Pending)
    all_delegation = Delegation.objects.filter(
        assign_to=request.user, status='Pending'
    ).order_by('planned_date')
    if today_only:
        all_delegation = all_delegation.filter(planned_date__gte=t_start, planned_date__lt=t_end)

    # Help tickets list (not Closed)
    all_help_ticket = HelpTicket.objects.filter(
        assign_to=request.user
    ).exclude(status='Closed').order_by('planned_date')
    if today_only:
        all_help_ticket = all_help_ticket.filter(planned_date__gte=t_start, planned_date__lt=t_end)

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

    # ---------- Time aggregations (FIXED for SQLite) ----------
    def calculate_checklist_assigned_time(qs, date_from, date_to):
        """FIXED: Sum expected minutes for checklist items in [date_from, date_to] by their recurrence."""
        total_minutes = 0
        
        # Convert dates to safe date objects
        date_from = _coerce_date_safe(date_from)
        date_to = _coerce_date_safe(date_to)
        
        for task in qs:
            try:
                mode = getattr(task, 'mode', 'Daily')
                freq = getattr(task, 'frequency', 1) or 1
                minutes = task.time_per_task_minutes or 0
                
                # Safely convert task planned_date to date
                task_date = _coerce_date_safe(task.planned_date)
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
                    # One-time task
                    if start_date <= task_date <= end_date:
                        total_minutes += minutes
                        
            except Exception as e:
                # Skip problematic tasks to prevent dashboard crashes
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error calculating checklist time for task {task.id}: {e}")
                continue

        return total_minutes

    def calculate_delegation_assigned_time_safe(assign_to_user, date_from, date_to):
        """
        FIXED: SQLite-safe delegation time calculation.
        Calculate total assigned minutes for delegations in date range.
        """
        total = 0
        
        # Convert to safe date objects
        date_from = _coerce_date_safe(date_from)
        date_to = _coerce_date_safe(date_to)
        
        # Get delegations in the date range using datetime bounds
        start_dt, end_dt = span_bounds(date_from, date_to)
        
        delegations = Delegation.objects.filter(
            assign_to=assign_to_user,
            planned_date__gte=start_dt,
            planned_date__lt=end_dt
        )
        
        for d in delegations:
            try:
                # Safely convert planned_date to date for comparison
                planned_date = _coerce_date_safe(d.planned_date)
                
                if date_from <= planned_date <= date_to:
                    total += d.time_per_task_minutes or 0
                    
            except Exception as e:
                # Skip problematic delegations
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error calculating delegation time for delegation {d.id}: {e}")
                continue
                
        return total

    # Calculate time aggregations safely
    try:
        prev_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(assign_to=request.user, status='Pending'), 
            start_prev, end_prev
        )
        curr_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(assign_to=request.user, status='Pending'), 
            start_current, today
        )

        # Use the SQLite-safe variant for delegation minutes
        prev_min_del = calculate_delegation_assigned_time_safe(request.user, start_prev, end_prev)
        curr_min_del = calculate_delegation_assigned_time_safe(request.user, start_current, today)
    except Exception as e:
        # Fallback to zero if calculations fail
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error calculating time aggregations: {e}")
        prev_min = curr_min = prev_min_del = curr_min_del = 0

    def minutes_to_hhmm(minutes):
        try:
            h = int(minutes) // 60
            m = int(minutes) % 60
            return f"{h:02d}:{m:02d}"
        except (ValueError, TypeError):
            return "00:00"

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':     minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':    today_only,
    })