from datetime import timedelta, datetime, time as dt_time, date
import logging
import sys

import pytz
from dateutil.relativedelta import relativedelta

from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.shortcuts import render
from django.db.models import Q

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
    """
    Compute the next occurrence preserving the spec:
      • Daily  : every <freq> days
      • Weekly : every <freq> weeks on the SAME weekday
      • Monthly: every <freq> months on the SAME date
      • Yearly : every <freq> years on the SAME month/day
    No shifting to next working day here (strict spec).
    Frequency is clamped to 1..10.
    """
    if (mode or '') not in RECURRING_MODES:
        return None

    # Clamp frequency to 1..10 per spec
    try:
        step = int(frequency or 1)
    except Exception:
        step = 1
    step = max(1, min(step, 10))

    tz = timezone.get_current_timezone()
    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, tz)

    cur_ist = prev_dt.astimezone(IST)

    if mode == 'Daily':
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == 'Weekly':
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == 'Monthly':
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == 'Yearly':
        cur_ist = cur_ist + relativedelta(years=step)

    # Pin assignment time
    cur_ist = cur_ist.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)

    # IMPORTANT: Do NOT move to next working day here; spec requires same weekday/date.
    return cur_ist.astimezone(tz)


# ---------- Checklist visibility gating ----------
def _should_show_checklist(task_dt: datetime, now_ist: datetime) -> bool:
    """
    Visible if:
      • planned date < today (IST)
      • planned date == today (IST) AND current time >= 10:00 IST
    Hidden if:
      • planned date > today (IST)
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


# -------------------------- HANDOVER INTEGRATION ---------------------------
def _normalize_task_type(val) -> str | None:
    """
    Map DB task_type (enum/int/label) to 'checklist' | 'delegation' | 'help_ticket'
    """
    if val is None:
        return None
    # ints (enum)
    try:
        ival = int(val)
        mapping_int = {1: "checklist", 2: "delegation", 3: "help_ticket"}
        if ival in mapping_int:
            return mapping_int[ival]
    except Exception:
        pass
    # strings
    s = str(val).strip().lower()
    if s in ("checklist", "delegation", "help_ticket", "help ticket"):
        return "help_ticket" if s.startswith("help") else s
    if s in ("help_ticket", "helpticket"):
        return "help_ticket"
    if s in ("CHECKLIST", "DELEGATION", "HELP_TICKET"):
        # unlikely here, but just in case
        return s.lower()
    return None


def _get_handover_tasks_for_user(user, today_date: date):
    """
    Returns dict: {'checklist': [ids], 'delegation': [ids], 'help_ticket': [ids]}
    for tasks handed over to the given user and active *today* (IST date).
    Uses real LeaveStatus to avoid string/enum mismatches.
    """
    try:
        from apps.leave.models import LeaveHandover, LeaveStatus  # type: ignore

        active = (
            LeaveHandover.objects
            .filter(
                new_assignee=user,
                is_active=True,
                effective_start_date__lte=today_date,
                effective_end_date__gte=today_date,
                leave_request__status=LeaveStatus.APPROVED,
            )
            .select_related('leave_request')
        )

        out = {'checklist': [], 'delegation': [], 'help_ticket': []}
        for h in active:
            key = _normalize_task_type(getattr(h, "task_type", None))
            if key and key in out:
                out[key].append(h.original_task_id)

        logger.info(_safe_console_text(
            f"handover ids for {user.username} on {today_date}: "
            f"CL={len(out['checklist'])}, DL={len(out['delegation'])}, HT={len(out['help_ticket'])}"
        ))
        return out
    except Exception as e:
        logger.error(_safe_console_text(f"handover lookup failed: {e}"))
        return {'checklist': [], 'delegation': [], 'help_ticket': []}


def _dedupe_by_id(items):
    """De-duplicate a list of model instances by id while preserving order."""
    seen = set()
    out = []
    for x in items:
        _id = getattr(x, 'id', None)
        if _id in seen:
            continue
        seen.add(_id)
        out.append(x)
    return out


# ------------------------------ aggregations ------------------------------
def calculate_checklist_assigned_time(qs, date_from, date_to):
    total_minutes = 0

    date_from = _coerce_date_safe(date_from)
    date_to = _coerce_date_safe(date_to)

    for task in qs:
        try:
            mode = getattr(task, 'mode', '') or ''
            raw_freq = getattr(task, 'frequency', 1) or 1
            try:
                freq = int(raw_freq)
            except Exception:
                freq = 1
            # Clamp per spec
            freq = max(1, min(freq, 10))

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
                planned = d.planned_date
                if planned and timezone.is_naive(planned):
                    planned = timezone.make_aware(planned, timezone.get_current_timezone())
                planned_date = _coerce_date_safe(planned)
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
    Dashboard with “handed-over” section.
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

    # Handover tasks for this user (IST "today")
    handover = _get_handover_tasks_for_user(request.user, today_ist)

    try:
        # -------------------- Checklists --------------------
        base_checklists = list(
            Checklist.objects
            .filter(assign_to=request.user, status='Pending', planned_date__lte=end_today_proj)
            .select_related('assign_by', 'assign_to')
            .order_by('planned_date')
        )

        # Include handed-over checklists
        if handover['checklist']:
            ho_qs = list(
                Checklist.objects
                .filter(id__in=handover['checklist'], status='Pending', planned_date__lte=end_today_proj)
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )
            for t in ho_qs:
                t.is_handover = True
            all_checklists = _dedupe_by_id(base_checklists + ho_qs)
        else:
            all_checklists = base_checklists

        if today_only:
            checklist_qs = [
                c for c in all_checklists
                if (_ist_date(c.planned_date) == today_ist) and
                   (getattr(c, 'is_handover', False) or _should_show_checklist(c.planned_date, now_ist))
            ]
        else:
            checklist_qs = [
                c for c in all_checklists
                if getattr(c, 'is_handover', False) or _should_show_checklist(c.planned_date, now_ist)
            ]

        # ------------------- Delegations -------------------
        if today_only:
            base_delegations = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                ).select_related('assign_by', 'assign_to').order_by('planned_date')
            )
        else:
            base_delegations = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__lte=end_today_proj
                ).select_related('assign_by', 'assign_to').order_by('planned_date')
            )

        if handover['delegation']:
            ho_del = list(
                Delegation.objects.filter(
                    id__in=handover['delegation'], status='Pending',
                    planned_date__lte=end_today_proj
                ).select_related('assign_by', 'assign_to').order_by('planned_date')
            )
            for t in ho_del:
                t.is_handover = True
            delegation_qs = _dedupe_by_id(base_delegations + ho_del)
        else:
            delegation_qs = base_delegations

        # ------------------- Help Tickets -------------------
        if today_only:
            base_help = list(
                HelpTicket.objects.filter(
                    assign_to=request.user,
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                ).exclude(status='Closed').select_related('assign_by', 'assign_to').order_by('planned_date')
            )
        else:
            base_help = list(
                HelpTicket.objects.filter(
                    assign_to=request.user, planned_date__lte=end_today_proj
                ).exclude(status='Closed').select_related('assign_by', 'assign_to').order_by('planned_date')
            )

        if handover['help_ticket']:
            ho_help = list(
                HelpTicket.objects.filter(
                    id__in=handover['help_ticket'],
                    planned_date__lte=end_today_proj
                ).exclude(status='Closed').select_related('assign_by', 'assign_to').order_by('planned_date')
            )
            for t in ho_help:
                t.is_handover = True
            help_ticket_qs = _dedupe_by_id(base_help + ho_help)
        else:
            help_ticket_qs = base_help

        logger.info(_safe_console_text(
            f"Dashboard filter for {request.user.username} | today_only={today_only} "
            f"| checklist={len(checklist_qs)} delegation={len(delegation_qs)} help={len(help_ticket_qs)} "
            f"| handover counts: CL={len(handover['checklist'])} DL={len(handover['delegation'])} HT={len(handover['help_ticket'])}"
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

    # Build “handed-over” section with real objects (so template can link)
    handed_over_full = {'checklist': [], 'delegation': [], 'help_ticket': []}
    if any(handover.values()):
        # Checklist
        if handover['checklist']:
            for t in Checklist.objects.filter(id__in=handover['checklist']).select_related("assign_by", "assign_to"):
                handed_over_full['checklist'].append({
                    'task': t,
                    'handover': None,
                    'original_assignee': t.assign_to,
                    'leave_request': None,
                    'handover_message': '',
                    'task_url': f"/checklist/{t.id}/",
                })
        # Delegation
        if handover['delegation']:
            for t in Delegation.objects.filter(id__in=handover['delegation']).select_related("assign_by", "assign_to"):
                handed_over_full['delegation'].append({
                    'task': t,
                    'handover': None,
                    'original_assignee': t.assign_to,
                    'leave_request': None,
                    'handover_message': '',
                    'task_url': f"/delegation/{t.id}/",
                })
        # Help tickets
        if handover['help_ticket']:
            for t in HelpTicket.objects.filter(id__in=handover['help_ticket']).select_related("assign_by", "assign_to"):
                handed_over_full['help_ticket'].append({
                    'task': t,
                    'handover': None,
                    'original_assignee': t.assign_to,
                    'leave_request': None,
                    'handover_message': '',
                    'task_url': f"/tickets/{t.id}/",
                })

    # sample log
    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            tdt = task.planned_date.astimezone(IST) if task.planned_date else None
            handover_info = " (HANDOVER)" if getattr(task, 'is_handover', False) else ""
            logger.info(_safe_console_text(
                f"  sample {i}: '{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                f"@ {tdt.strftime('%Y-%m-%d %H:%M IST') if tdt else 'No date'}{handover_info}"
            ))

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      selected,
        'prev_time':     minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':     minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':    today_only,
        'handed_over':   handed_over_full,   # << add to context
    })
