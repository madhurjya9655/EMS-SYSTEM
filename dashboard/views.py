# E:\CLIENT PROJECT\employee management system bos\employee_management_system\dashboard\views.py
from __future__ import annotations

from datetime import timedelta, datetime, time as dt_time, date
import logging
import sys

# 🔧 Use pytz everywhere to match the rest of the project (models/forms use pytz)
import pytz

from dateutil.relativedelta import relativedelta

from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.shortcuts import render
from django.core.cache import cache
from django.conf import settings
from django.db.models import Sum  # 🔧 correct aggregate import

# IMPORTANT: Only import TASKS-side models at module import time.
# Do NOT import anything from apps.leave.* at the top of the file.
from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

# Canonical recurring modes
RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']

# Timezone (pytz) — align with models.py to avoid SQLite UDF tz mixups
IST = pytz.timezone('Asia/Kolkata')

# FINAL SPEC:
# - Planned time for recurrences: 19:00 IST (7 PM)
# - Dashboard/email gating: 10:00 IST same day
EVENING_HOUR = 19
EVENING_MINUTE = 0

# Cache TTLs (safe-only caching)
_DASH_TTL = int(getattr(settings, "DASHBOARD_CACHE_TIMEOUT", 300) or 300)
_DASH_FAST_TTL = min(_DASH_TTL, 60)  # volatile counts: keep short


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
    """Working days: Mon–Sat, excluding configured holidays."""
    try:
        return dt_.weekday() != 6 and not Holiday.objects.filter(date=dt_).exists()
    except Exception:
        return dt_.weekday() != 6


def next_working_day(dt_: date) -> date:
    """Move forward to the next working day (Mon–Sat and not a holiday)."""
    while not is_working_day(dt_):
        dt_ += timedelta(days=1)
    return dt_


def _ist_span_to_project_bounds(d_from: date, d_to_inclusive: date):
    """
    Build [start, end) datetimes in PROJECT timezone, aligned to IST date boundaries.
    Uses end-exclusive bounds (better for index usage).
    """
    project_tz = timezone.get_current_timezone()
    start_ist = timezone.make_aware(datetime.combine(d_from, dt_time.min), IST)
    end_ist = timezone.make_aware(datetime.combine(d_to_inclusive + timedelta(days=1), dt_time.min), IST)
    start_proj = timezone.localtime(start_ist, project_tz)
    end_proj = timezone.localtime(end_ist, project_tz)
    return start_proj, end_proj


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
        return timezone.localtime(dt, IST).date()
    aware = timezone.make_aware(dt, IST)
    return aware.date()


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    (Compatibility helper; not used by the dashboard filtering itself.)
    Final recurrence notion for reference here:
      • Daily  : every <freq> days
      • Weekly : every <freq> weeks on the SAME weekday
      • Monthly: every <freq> months on the SAME date
      • Yearly : every <freq> years on the SAME month/day
    TIME IS ALWAYS PINNED TO 19:00 IST (7 PM).  **No shifting off Sun/holidays here.**
    """
    if (mode or '') not in RECURRING_MODES:
        return None

    # Clamp frequency to 1..10 per spec
    try:
        step = int(frequency or 1)
    except Exception:
        step = 1
    step = max(1, min(step, 10))

    project_tz = timezone.get_current_timezone()
    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, project_tz)

    cur_ist = timezone.localtime(prev_dt, IST)

    if mode == 'Daily':
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == 'Weekly':
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == 'Monthly':
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == 'Yearly':
        cur_ist = cur_ist + relativedelta(years=step)

    # Pin to 19:00 IST per final rule
    cur_ist = cur_ist.replace(hour=EVENING_HOUR, minute=EVENING_MINUTE, second=0, microsecond=0)

    return timezone.localtime(cur_ist, project_tz)


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

    if timezone.is_aware(task_dt):
        dt_ist = timezone.localtime(task_dt, IST)
    else:
        dt_ist = timezone.make_aware(task_dt, IST)

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
    try:
        ival = int(val)
        mapping_int = {1: "checklist", 2: "delegation", 3: "help_ticket"}
        if ival in mapping_int:
            return mapping_int[ival]
    except Exception:
        pass
    s = str(val).strip().lower()
    if s in ("checklist", "delegation", "help_ticket", "help ticket"):
        return "help_ticket" if s.startswith("help") else s
    if s in ("help_ticket", "helpticket"):
        return "help_ticket"
    return None


def _get_handover_tasks_for_user(user, today_date: date):
    """
    Returns dict: {'checklist': [ids], 'delegation': [ids], 'help_ticket': [ids]}
    for tasks handed over TO the given user and active *today* (IST date).
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
            .only("task_type", "original_task_id")
        )

        out = {'checklist': [], 'delegation': [], 'help_ticket': []}
        for h in active:
            key = _normalize_task_type(getattr(h, "task_type", None))
            if key and key in out:
                out[key].append(h.original_task_id)

        logger.info(_safe_console_text(
            f"handover ids to {user.username} on {today_date}: "
            f"CL={len(out['checklist'])}, DL={len(out['delegation'])}, HT={len(out['help_ticket'])}"
        ))
        return out
    except Exception as e:
        logger.error(_safe_console_text(f"handover lookup failed: {e}"))
        return {'checklist': [], 'delegation': [], 'help_ticket': []}


def _get_outgoing_handover_ids_for_user(user, today_date: date):
    """
    IDs of tasks the current user handed over AWAY (to hide from their dashboard) during an active leave window.
    Returns: {'checklist': [ids], 'delegation': [ids], 'help_ticket': [ids]}
    """
    try:
        from apps.leave.models import LeaveHandover, LeaveStatus  # type: ignore

        active = (
            LeaveHandover.objects
            .filter(
                original_assignee=user,
                is_active=True,
                effective_start_date__lte=today_date,
                effective_end_date__gte=today_date,
                leave_request__status=LeaveStatus.APPROVED,
            )
            .only("task_type", "original_task_id")
        )
        out = {'checklist': [], 'delegation': [], 'help_ticket': []}
        for h in active:
            key = _normalize_task_type(getattr(h, "task_type", None))
            if key and key in out:
                out[key].append(h.original_task_id)
        return out
    except Exception as e:
        logger.error(_safe_console_text(f"outgoing handover lookup failed: {e}"))
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

    qs = qs.only("planned_date", "mode", "frequency", "time_per_task_minutes")

    for task in qs:
        try:
            mode = getattr(task, 'mode', '') or ''
            raw_freq = getattr(task, 'frequency', 1) or 1
            try:
                freq = int(raw_freq)
            except Exception:
                freq = 1
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
    """
    Safe aggregate for delegation assigned minutes.
    Fix: use django.db.models.Sum (not django.utils.timezone.models), which could raise
    backend errors. Also keep fallback path.
    """
    try:
        date_from = _coerce_date_safe(date_from)
        date_to = _coerce_date_safe(date_to)
        start_dt, end_dt = _ist_span_to_project_bounds(date_from, date_to)

        agg = (
            Delegation.objects.filter(
                assign_to=assign_to_user,
                planned_date__gte=start_dt,
                planned_date__lt=end_dt,
                status='Pending'
            )
            .aggregate(total=Sum("time_per_task_minutes"))
        )
    except Exception:
        agg = None

    if isinstance(agg, dict) and "total" in agg:
        return int(agg["total"] or 0)

    # Fallback (keeps behavior safe if aggregate path fails in odd environments)
    total = 0
    try:
        start_dt, end_dt = _ist_span_to_project_bounds(_coerce_date_safe(date_from), _coerce_date_safe(date_to))
        delegations = Delegation.objects.filter(
            assign_to=assign_to_user,
            planned_date__gte=start_dt,
            planned_date__lt=end_dt,
            status='Pending'
        ).only("planned_date", "time_per_task_minutes")
        for d in delegations:
            total += d.time_per_task_minutes or 0
    except Exception:
        pass
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
    - Shows tasks handed over TO the current user.
    - Hides tasks the current user handed over AWAY during an active leave window.
    - Checklist visibility: only after 10:00 IST of the planned day; past-incomplete remain; future hidden.
    - Help Tickets: appear immediately (no 10:00 gating).
    - Original owner's block: recently “Completed by delegate”.
    - **Non-working day mode:** if today is Sunday OR a configured Holiday, show NO tasks at all.
    """
    now_ist = timezone.localtime(timezone.now(), IST)
    today_ist = now_ist.date()

    project_tz = timezone.get_current_timezone()
    now_project_tz = timezone.localtime(now_ist, project_tz)

    logger.info(_safe_console_text(f"Dashboard accessed by {request.user.username} at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}"))

    # ---- NON-WORKING MODE: short-circuit completely (Sunday OR Holiday) ----
    try:
        is_holiday_today = Holiday.objects.filter(date=today_ist).exists()
    except Exception:
        is_holiday_today = False
    is_sunday_today = today_ist.weekday() == 6

    if is_sunday_today or is_holiday_today:
        logger.info(_safe_console_text(f"Non-working day mode ON for {today_ist}: hiding all dashboard tasks."))
        # We still compute weekly numbers (optionally) but show empty task lists.
        start_current = today_ist - timedelta(days=today_ist.weekday())  # Monday this week
        start_prev = start_current - timedelta(days=7)                    # Monday last week
        end_prev = start_current - timedelta(days=1)                      # Sunday last week

        try:
            curr_start_dt, curr_end_dt = _ist_span_to_project_bounds(start_current, today_ist)
            prev_start_dt, prev_end_dt = _ist_span_to_project_bounds(start_prev, end_prev)

            curr_chk = Checklist.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_chk = Checklist.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
            ).count()

            curr_del = Delegation.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_del = Delegation.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
            ).count()

            curr_help = HelpTicket.objects.filter(
                assign_to=request.user, status='Closed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_help = HelpTicket.objects.filter(
                assign_to=request.user, status='Closed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
            ).count()

            week_score = {
                'checklist':   {'previous': prev_chk,   'current': curr_chk},
                'delegation':  {'previous': prev_del,   'current': curr_del},
                'help_ticket': {'previous': prev_help,  'current': curr_help},
            }
        except Exception as e:
            logger.error(_safe_console_text(f"Error calculating weekly scores (non-working mode): {e}"))
            week_score = {
                'checklist': {'previous': 0, 'current': 0},
                'delegation': {'previous': 0, 'current': 0},
                'help_ticket': {'previous': 0, 'current': 0},
            }

        pending_tasks = {'checklist': 0, 'delegation': 0, 'help_ticket': 0}
        prev_min = curr_min = prev_min_del = curr_min_del = 0

        return render(request, 'dashboard/dashboard.html', {
            'week_score':            week_score,
            'pending_tasks':         pending_tasks,
            'tasks':                 [],        # nothing shown
            'selected':              request.GET.get('task_type'),
            'prev_time':             minutes_to_hhmm(prev_min + prev_min_del),
            'curr_time':             minutes_to_hhmm(curr_min + curr_min_del),
            'today_only':            (request.GET.get('today') == '1' or request.GET.get('today_only') == '1'),
            'handed_over':           {'checklist': [], 'delegation': [], 'help_ticket': []},
            'completed_by_delegate': {'checklist': [], 'delegation': [], 'help_ticket': []},
            'holiday_today':         True,      # template can optionally show a banner
        })

    # Week windows (Mon..Sun) for scorecards (date-only)
    start_current = today_ist - timedelta(days=today_ist.weekday())  # Monday this week
    start_prev = start_current - timedelta(days=7)                    # Monday last week
    end_prev = start_current - timedelta(days=1)                      # Sunday last week

    # ----- Cache: weekly score + pending counts (safe short TTL) -----
    cache_key_week = f"dash:week_score:u{request.user.id}:{today_ist.isoformat()}"
    cache_key_pending = f"dash:pending:u{request.user.id}:{today_ist.isoformat()}"

    week_score = cache.get(cache_key_week)
    pending_tasks = cache.get(cache_key_pending)

    if week_score is None or pending_tasks is None:
        try:
            curr_start_dt, curr_end_dt = _ist_span_to_project_bounds(start_current, today_ist)
            prev_start_dt, prev_end_dt = _ist_span_to_project_bounds(start_prev, end_prev)

            curr_chk = Checklist.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_chk = Checklist.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
            ).count()

            curr_del = Delegation.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_del = Delegation.objects.filter(
                assign_to=request.user, status='Completed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
            ).count()

            curr_help = HelpTicket.objects.filter(
                assign_to=request.user, status='Closed',
                planned_date__gte=curr_start_dt, planned_date__lt=curr_end_dt,
            ).count()
            prev_help = HelpTicket.objects.filter(
                assign_to=request.user, status='Closed',
                planned_date__gte=prev_start_dt, planned_date__lt=prev_end_dt,
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

            cache.set(cache_key_week, week_score, _DASH_FAST_TTL)
            cache.set(cache_key_pending, pending_tasks, _DASH_FAST_TTL)

        except Exception as e:
            # Keep log text stable with earlier deployments
            logger.error(_safe_console_text(f"Error calculating weekly scores: {e}"))
            week_score = {
                'checklist': {'previous': 0, 'current': 0},
                'delegation': {'previous': 0, 'current': 0},
                'help_ticket': {'previous': 0, 'current': 0},
            }
            pending_tasks = {'checklist': 0, 'delegation': 0, 'help_ticket': 0}

    selected = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1' or request.GET.get('today_only') == '1')

    # Build IST-aligned bounds once (project tz)
    start_today_proj = timezone.localtime(
        timezone.make_aware(datetime.combine(today_ist, dt_time.min), IST),
        project_tz
    )
    end_today_proj = timezone.localtime(
        timezone.make_aware(datetime.combine(today_ist, dt_time.max), IST),
        project_tz
    )

    # Handover tasks for this user (IST "today")
    handover_incoming = _get_handover_tasks_for_user(request.user, today_ist)
    handover_outgoing = _get_outgoing_handover_ids_for_user(request.user, today_ist)

    # Gate anchor
    ten_am = dt_time(10, 0, 0)
    after_10 = now_ist.timetz().replace(tzinfo=None) >= ten_am

    try:
        # -------------------- Checklists --------------------
        # IMPORTANT: base list respects 10AM gate using DB filter to avoid loading hidden rows
        if today_only:
            if after_10:
                base_checklists = list(
                    Checklist.objects
                    .filter(
                        assign_to=request.user,
                        status='Pending',
                        planned_date__gte=start_today_proj,
                        planned_date__lte=end_today_proj,
                    )
                    .exclude(id__in=handover_outgoing['checklist'])
                    .select_related('assign_by', 'assign_to')
                    .order_by('planned_date')
                )
            else:
                base_checklists = []
        else:
            if after_10:
                planned_filter = {'planned_date__lte': end_today_proj}
            else:
                planned_filter = {'planned_date__lt': start_today_proj}  # hide today's items before 10AM
            base_checklists = list(
                Checklist.objects
                .filter(assign_to=request.user, status='Pending', **planned_filter)
                .exclude(id__in=handover_outgoing['checklist'])
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )

        # Include handed-over checklists (always visible per existing behavior)
        if handover_incoming['checklist'] and not today_only:
            ho_qs = list(
                Checklist.objects
                .filter(id__in=handover_incoming['checklist'], status='Pending', planned_date__lte=end_today_proj)
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
                )
                .exclude(id__in=handover_outgoing['delegation'])
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )
        else:
            base_delegations = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__lte=end_today_proj
                )
                .exclude(id__in=handover_outgoing['delegation'])
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )

        if handover_incoming['delegation'] and not today_only:
            ho_del = list(
                Delegation.objects.filter(
                    id__in=handover_incoming['delegation'], status='Pending',
                    planned_date__lte=end_today_proj
                )
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
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
                )
                .exclude(status='Closed')
                .exclude(id__in=handover_outgoing['help_ticket'])
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )
        else:
            base_help = list(
                HelpTicket.objects.filter(
                    assign_to=request.user, planned_date__lte=end_today_proj
                )
                .exclude(status='Closed')
                .exclude(id__in=handover_outgoing['help_ticket'])
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )

        if handover_incoming['help_ticket'] and not today_only:
            ho_help = list(
                HelpTicket.objects.filter(
                    id__in=handover_incoming['help_ticket'],
                    planned_date__lte=end_today_proj
                )
                .exclude(status='Closed')
                .select_related('assign_by', 'assign_to')
                .order_by('planned_date')
            )
            for t in ho_help:
                t.is_handover = True
            help_ticket_qs = _dedupe_by_id(base_help + ho_help)
        else:
            help_ticket_qs = base_help

        logger.info(_safe_console_text(
            f"Dashboard filter for {request.user.username} | today_only={today_only} "
            f"| checklist={len(checklist_qs)} delegation={len(delegation_qs)} help={len(help_ticket_qs)} "
            f"| incoming handover: CL={len(handover_incoming['checklist'])} DL={len(handover_incoming['delegation'])} HT={len(handover_incoming['help_ticket'])} "
            f"| outgoing hidden: CL={len(handover_outgoing['checklist'])} DL={len(handover_outgoing['delegation'])} HT={len(handover_outgoing['help_ticket'])}"
        ))
    except Exception as e:
        logger.error(_safe_console_text(f"Error querying task lists: {e}"))
        checklist_qs = []
        delegation_qs = []
        help_ticket_qs = []

    if selected == 'delegation':
        tasks = delegation_qs
    elif selected == 'help_ticket':
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

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

    handed_over_full = {'checklist': [], 'delegation': [], 'help_ticket': []}
    completed_by_delegate = {'checklist': [], 'delegation': [], 'help_ticket': []}
    try:
        from apps.leave.models import LeaveHandover, LeaveStatus

        active_handover = (
            LeaveHandover.objects
            .filter(
                new_assignee=request.user,
                is_active=True,
                effective_start_date__lte=today_ist,
                effective_end_date__gte=today_ist,
                leave_request__status=LeaveStatus.APPROVED,
            )
            .select_related("leave_request", "original_assignee")
            .order_by("id")
        )

        def _row(task, ho, url_prefix: str):
            return {
                'task': task,
                'handover': ho,
                'original_assignee': getattr(ho, "original_assignee", None),
                'leave_request': getattr(ho, "leave_request", None),
                'handover_message': getattr(ho, "message", "") or "",
                'task_url': f"/{url_prefix}/{getattr(task, 'id', '')}/" if getattr(task, 'id', None) else None,
            }

        cl_ids = [h.original_task_id for h in active_handover if _normalize_task_type(h.task_type) == "checklist"]
        dl_ids = [h.original_task_id for h in active_handover if _normalize_task_type(h.task_type) == "delegation"]
        ht_ids = [h.original_task_id for h in active_handover if _normalize_task_type(h.task_type) == "help_ticket"]

        checklist_map = {t.id: t for t in Checklist.objects.filter(id__in=cl_ids).select_related("assign_by", "assign_to")}
        delegation_map = {t.id: t for t in Delegation.objects.filter(id__in=dl_ids).select_related("assign_by", "assign_to")}
        help_map = {t.id: t for t in HelpTicket.objects.filter(id__in=ht_ids).select_related("assign_by", "assign_to")}

        for ho in active_handover:
            key = _normalize_task_type(ho.task_type)
            if key == "checklist":
                t = checklist_map.get(ho.original_task_id)
                if t:
                    handed_over_full['checklist'].append(_row(t, ho, "checklist"))
            elif key == "delegation":
                t = delegation_map.get(ho.original_task_id)
                if t:
                    handed_over_full['delegation'].append(_row(t, ho, "delegation"))
            elif key == "help_ticket":
                t = help_map.get(ho.original_task_id)
                if t:
                    handed_over_full['help_ticket'].append(_row(t, ho, "tickets"))

        lookback_days = 14
        since_dt = timezone.now() - timedelta(days=lookback_days)
        try:
            recent_handover = (
                LeaveHandover.objects
                .filter(original_assignee=request.user)
                .select_related("new_assignee")
                .order_by("-updated_at", "-id")
            )

            cb_cl_ids = [h.original_task_id for h in recent_handover if _normalize_task_type(h.task_type) == "checklist"]
            cb_dl_ids = [h.original_task_id for h in recent_handover if _normalize_task_type(h.task_type) == "delegation"]
            cb_ht_ids = [h.original_task_id for h in recent_handover if _normalize_task_type(h.task_type) == "help_ticket"]

            cb_cls = Checklist.objects.filter(id__in=cb_cl_ids, status="Completed")
            cb_dls = Delegation.objects.filter(id__in=cb_dl_ids, status="Completed")
            cb_hts = HelpTicket.objects.filter(id__in=cb_ht_ids, status="Closed")

            cb_cls = [t for t in cb_cls if getattr(t, "updated_at", since_dt) >= since_dt]
            cb_dls = [t for t in cb_dls if getattr(t, "updated_at", since_dt) >= since_dt]
            cb_hts = [t for t in cb_hts if getattr(t, "updated_at", getattr(t, "resolved_at", since_dt)) >= since_dt]

            ho_map = {}
            for h in recent_handover:
                ho_map[h.original_task_id] = h

            for t in cb_cls:
                ho = ho_map.get(t.id)
                if ho:
                    completed_by_delegate['checklist'].append(_row(t, ho, "checklist"))
            for t in cb_dls:
                ho = ho_map.get(t.id)
                if ho:
                    completed_by_delegate['delegation'].append(_row(t, ho, "delegation"))
            for t in cb_hts:
                ho = ho_map.get(t.id)
                if ho:
                    completed_by_delegate['help_ticket'].append(_row(t, ho, "tickets"))
        except Exception as e:
            logger.error(_safe_console_text(f"Error building completed_by_delegate block: {e}"))
    except Exception as e:
        logger.error(_safe_console_text(f"Error building handed_over section: {e}"))

    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            tdt = timezone.localtime(task.planned_date, IST) if task.planned_date else None
            handover_info = " (HANDOVER)" if getattr(task, 'is_handover', False) else ""
            logger.info(_safe_console_text(
                f"  sample {i}: '{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                f"@ {tdt.strftime('%Y-%m-%d %H:%M IST') if tdt else 'No date'}{handover_info}"
            ))

    return render(request, 'dashboard/dashboard.html', {
        'week_score':          week_score,
        'pending_tasks':       pending_tasks,
        'tasks':               tasks,
        'selected':            selected,
        'prev_time':           minutes_to_hhmm(prev_min + prev_min_del),
        'curr_time':           minutes_to_hhmm(curr_min + curr_min_del),
        'today_only':          today_only,
        'handed_over':         handed_over_full,
        'completed_by_delegate': completed_by_delegate,
        'holiday_today':       False,  # normal day
    })
