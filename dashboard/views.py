# dashboard/views.py
from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time as dt_time, timedelta

import pytz
from dateutil.relativedelta import relativedelta

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.db.models import Sum
from django.db.utils import OperationalError, ProgrammingError
from django.shortcuts import render
from django.utils import timezone

from apps.settings.models import Holiday
from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.tasks.services.holiday_guard import is_holiday_for_user, holiday_skip_reason

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

EVENING_HOUR = 19
EVENING_MINUTE = 0

_DASH_TTL = int(getattr(settings, "DASHBOARD_CACHE_TIMEOUT", 300) or 300)
_DASH_FAST_TTL = min(_DASH_TTL, 60)


# ---------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------
def _safe_console_text(s: object) -> str:
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)

    enc = (
        getattr(sys.stderr, "encoding", None)
        or getattr(sys.stdout, "encoding", None)
        or "utf-8"
    )

    try:
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return text.encode("ascii", errors="replace").decode("ascii", errors="replace")


# ---------------------------------------------------------------------
# Date / timezone helpers
# ---------------------------------------------------------------------
def _coerce_date_safe(value):
    """
    Convert date/datetime/string safely to date.
    """
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        if timezone.is_aware(value):
            return timezone.localtime(value, IST).date()
        return value.date()

    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

    try:
        if hasattr(value, "date"):
            return value.date()
    except Exception:
        pass

    return timezone.localdate()


def _ist_date(value) -> date | None:
    """
    Return IST date from datetime/date.
    """
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        if timezone.is_aware(value):
            return timezone.localtime(value, IST).date()

        aware = timezone.make_aware(value, timezone.get_current_timezone())
        return timezone.localtime(aware, IST).date()

    return _coerce_date_safe(value)


def _ist_span_to_project_bounds(d_from: date, d_to_inclusive: date):
    """
    Build [start, end) datetime bounds in project timezone,
    aligned to IST date boundaries.
    """
    project_tz = timezone.get_current_timezone()

    start_ist = timezone.make_aware(
        datetime.combine(d_from, dt_time.min),
        IST,
    )
    end_ist = timezone.make_aware(
        datetime.combine(d_to_inclusive + timedelta(days=1), dt_time.min),
        IST,
    )

    return (
        timezone.localtime(start_ist, project_tz),
        timezone.localtime(end_ist, project_tz),
    )


def _today_project_bounds(today_ist: date):
    """
    Return start/end of today in project timezone using IST day boundary.
    """
    project_tz = timezone.get_current_timezone()

    start_ist = timezone.make_aware(
        datetime.combine(today_ist, dt_time.min),
        IST,
    )
    end_ist = timezone.make_aware(
        datetime.combine(today_ist, dt_time.max),
        IST,
    )

    return (
        timezone.localtime(start_ist, project_tz),
        timezone.localtime(end_ist, project_tz),
    )


def is_working_day(dt_: date) -> bool:
    """
    Working day means:
    - not Sunday
    - not Holiday Master date
    """
    return not is_holiday_for_user(None, dt_)


def next_working_day(dt_: date) -> date:
    """
    Compatibility helper.

    Dashboard does not use this to shift tasks.
    Recurring engine must skip holidays, not shift.
    """
    while not is_working_day(dt_):
        dt_ += timedelta(days=1)

    return dt_


def get_next_planned_date(prev_dt: datetime, mode: str, frequency: int) -> datetime | None:
    """
    Compatibility helper.

    Time is pinned to 19:00 IST.
    """
    if (mode or "") not in RECURRING_MODES:
        return None

    try:
        step = int(frequency or 1)
    except Exception:
        step = 1

    step = max(1, min(step, 10))

    project_tz = timezone.get_current_timezone()

    if timezone.is_naive(prev_dt):
        prev_dt = timezone.make_aware(prev_dt, project_tz)

    cur_ist = timezone.localtime(prev_dt, IST)

    if mode == "Daily":
        cur_ist = cur_ist + relativedelta(days=step)
    elif mode == "Weekly":
        cur_ist = cur_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        cur_ist = cur_ist + relativedelta(months=step)
    elif mode == "Yearly":
        cur_ist = cur_ist + relativedelta(years=step)

    cur_ist = cur_ist.replace(
        hour=EVENING_HOUR,
        minute=EVENING_MINUTE,
        second=0,
        microsecond=0,
    )

    return timezone.localtime(cur_ist, project_tz)


# ---------------------------------------------------------------------
# Dashboard visibility helpers
# ---------------------------------------------------------------------
def _should_show_checklist(task_dt: datetime, now_ist: datetime) -> bool:
    """
    Checklist dashboard rule:

    - Past planned date: show
    - Future planned date: hide
    - Today planned date: show only after 10:00 IST
    """
    if not task_dt:
        return False

    if timezone.is_aware(task_dt):
        dt_ist = timezone.localtime(task_dt, IST)
    else:
        dt_ist = timezone.make_aware(task_dt, timezone.get_current_timezone())
        dt_ist = timezone.localtime(dt_ist, IST)

    task_date = dt_ist.date()
    today = now_ist.date()

    if task_date < today:
        return True

    if task_date > today:
        return False

    return now_ist.timetz().replace(tzinfo=None) >= dt_time(10, 0, 0)


def _not_holiday_task(obj) -> bool:
    """
    Hide old wrongly-created holiday tasks from dashboard.

    If task planned_date is Sunday/Holiday:
        return False

    If task planned_date is normal day:
        return True
    """
    planned = getattr(obj, "planned_date", None)

    if not planned:
        return True

    return not is_holiday_for_user(
        getattr(obj, "assign_to", None),
        planned,
    )


def _dedupe_by_id(items):
    """
    De-duplicate model instances by id while preserving order.
    """
    seen = set()
    out = []

    for item in items:
        item_id = getattr(item, "id", None)

        if item_id in seen:
            continue

        seen.add(item_id)
        out.append(item)

    return out


# ---------------------------------------------------------------------
# Leave handover helpers
# ---------------------------------------------------------------------
def _normalize_task_type(val) -> str | None:
    """
    Map handover task_type to:
    - checklist
    - delegation
    - help_ticket
    """
    if val is None:
        return None

    try:
        int_val = int(val)
        mapping_int = {
            1: "checklist",
            2: "delegation",
            3: "help_ticket",
        }

        if int_val in mapping_int:
            return mapping_int[int_val]
    except Exception:
        pass

    s = str(val).strip().lower()

    if s in ("checklist",):
        return "checklist"

    if s in ("delegation",):
        return "delegation"

    if s in ("help_ticket", "help ticket", "helpticket"):
        return "help_ticket"

    return None


def _approved_status_values(LeaveStatus) -> list:
    """
    Support enum/string/int approved statuses safely.
    """
    values = set()

    raw = getattr(LeaveStatus, "APPROVED", None)
    candidates = [raw]

    try:
        if raw is not None and hasattr(raw, "value"):
            candidates.append(raw.value)
    except Exception:
        pass

    try:
        if raw is not None and hasattr(raw, "label"):
            candidates.append(raw.label)
    except Exception:
        pass

    for c in candidates:
        if c is None:
            continue

        values.add(c)

        try:
            values.add(str(c))
        except Exception:
            pass

    values.update({"Approved", "APPROVED", "approved"})

    more = set()

    for v in list(values):
        try:
            more.add(int(v))
        except Exception:
            continue

    values |= more

    return list(values)


def _get_handover_tasks_for_user(user, today_date: date):
    """
    Return ids of tasks handed over TO current user and active today.
    """
    empty = {
        "checklist": [],
        "delegation": [],
        "help_ticket": [],
    }

    try:
        from apps.leave.models import LeaveHandover, LeaveStatus

        approved_vals = _approved_status_values(LeaveStatus)

        active = (
            LeaveHandover.objects
            .filter(
                new_assignee=user,
                is_active=True,
                effective_start_date__lte=today_date,
                effective_end_date__gte=today_date,
                leave_request__status__in=approved_vals,
            )
            .only("task_type", "original_task_id")
        )

        out = {
            "checklist": [],
            "delegation": [],
            "help_ticket": [],
        }

        for handover in active:
            key = _normalize_task_type(getattr(handover, "task_type", None))

            if key and key in out:
                out[key].append(handover.original_task_id)

        logger.info(
            _safe_console_text(
                f"Handover ids to {user.username} on {today_date}: "
                f"CL={len(out['checklist'])}, "
                f"DL={len(out['delegation'])}, "
                f"HT={len(out['help_ticket'])}"
            )
        )

        return out

    except (OperationalError, ProgrammingError) as e:
        logger.warning(
            _safe_console_text(
                f"Handover lookup skipped because leave table/schema not ready: {e}"
            )
        )
        return empty

    except Exception as e:
        logger.error(_safe_console_text(f"Handover lookup failed: {e}"))
        return empty


# ---------------------------------------------------------------------
# Time aggregations
# ---------------------------------------------------------------------
def calculate_checklist_assigned_time(qs, date_from, date_to):
    """
    Calculate estimated checklist assigned time in minutes.
    """
    total_minutes = 0

    date_from = _coerce_date_safe(date_from)
    date_to = _coerce_date_safe(date_to)

    if not date_from or not date_to:
        return 0

    try:
        qs = qs.only(
            "planned_date",
            "mode",
            "frequency",
            "time_per_task_minutes",
        )
    except Exception:
        pass

    for task in qs:
        try:
            mode = getattr(task, "mode", "") or ""

            try:
                freq = int(getattr(task, "frequency", 1) or 1)
            except Exception:
                freq = 1

            freq = max(1, min(freq, 10))

            minutes = getattr(task, "time_per_task_minutes", 0) or 0
            task_date = _coerce_date_safe(getattr(task, "planned_date", None))

            if not task_date:
                continue

            if task_date > date_to:
                continue

            start_date = max(date_from, task_date)
            end_date = date_to

            if mode == "Daily":
                days = (end_date - start_date).days

                if days >= 0:
                    occur = (days // freq) + 1
                    total_minutes += minutes * occur

            elif mode == "Weekly":
                weeks = ((end_date - start_date).days // 7)

                if weeks >= 0:
                    occur = (weeks // freq) + 1
                    total_minutes += minutes * occur

            elif mode == "Monthly":
                months = (
                    (end_date.year - start_date.year) * 12
                    + (end_date.month - start_date.month)
                )

                if end_date.day < start_date.day:
                    months -= 1

                if months >= 0:
                    occur = (months // freq) + 1
                    total_minutes += minutes * occur

            elif mode == "Yearly":
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
            logger.error(
                _safe_console_text(
                    f"Error calculating checklist time for task "
                    f"{getattr(task, 'id', '?')}: {e}"
                )
            )
            continue

    return total_minutes


def calculate_delegation_assigned_time_safe(assign_to_user, date_from, date_to):
    """
    Calculate delegation assigned time in minutes.
    """
    try:
        date_from = _coerce_date_safe(date_from)
        date_to = _coerce_date_safe(date_to)

        if not date_from or not date_to:
            return 0

        start_dt, end_dt = _ist_span_to_project_bounds(date_from, date_to)

        agg = (
            Delegation.objects
            .filter(
                assign_to=assign_to_user,
                planned_date__gte=start_dt,
                planned_date__lt=end_dt,
                status="Pending",
                is_skipped_due_to_leave=False,
            )
            .aggregate(total=Sum("time_per_task_minutes"))
        )

        return int(agg.get("total") or 0)

    except Exception:
        total = 0

        try:
            start_dt, end_dt = _ist_span_to_project_bounds(
                _coerce_date_safe(date_from),
                _coerce_date_safe(date_to),
            )

            qs = (
                Delegation.objects
                .filter(
                    assign_to=assign_to_user,
                    planned_date__gte=start_dt,
                    planned_date__lt=end_dt,
                    status="Pending",
                    is_skipped_due_to_leave=False,
                )
                .only("planned_date", "time_per_task_minutes")
            )

            for item in qs:
                total += getattr(item, "time_per_task_minutes", 0) or 0

        except Exception:
            pass

        return total


def minutes_to_hhmm(minutes):
    """
    Convert minutes to HH:MM.
    """
    try:
        h = int(minutes) // 60
        m = int(minutes) % 60
        return f"{h:02d}:{m:02d}"
    except Exception:
        return "00:00"


# ---------------------------------------------------------------------
# Main dashboard view
# ---------------------------------------------------------------------
@login_required
def dashboard_home(request):
    """
    Final BOS Lakshya dashboard rule:

    Sunday/Holiday = complete off day.

    On Sunday/Holiday:
    - no checklist visible
    - no delegation visible
    - no help ticket visible
    - no handover actionable item visible
    - old wrongly-created holiday tasks are suppressed

    Normal day:
    - checklist follows 10:00 IST visibility rule
    - delegation/help ticket visible as per existing date logic
    """
    now_ist = timezone.localtime(timezone.now(), IST)
    today_ist = now_ist.date()

    project_tz = timezone.get_current_timezone()
    now_project_tz = timezone.localtime(now_ist, project_tz)

    selected = request.GET.get("task_type")
    today_only = (
        request.GET.get("today") == "1"
        or request.GET.get("today_only") == "1"
    )

    logger.info(
        _safe_console_text(
            f"Dashboard accessed by {request.user.username} "
            f"at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}"
        )
    )

    # ---------------------------------------------------------------
    # Holiday / Sunday detection
    # ---------------------------------------------------------------
    try:
        is_holiday_today = Holiday.objects.filter(date=today_ist).exists()
    except (OperationalError, ProgrammingError):
        is_holiday_today = False
    except Exception:
        is_holiday_today = False

    is_sunday_today = today_ist.weekday() == 6
    is_off_day_today = bool(is_holiday_today or is_sunday_today)

    # ---------------------------------------------------------------
    # Week ranges
    # ---------------------------------------------------------------
    start_current = today_ist - timedelta(days=today_ist.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    # ---------------------------------------------------------------
    # Weekly score and pending count
    # ---------------------------------------------------------------
    cache_key_week = f"dash:week_score:u{request.user.id}:{today_ist.isoformat()}"
    cache_key_pending = f"dash:pending:u{request.user.id}:{today_ist.isoformat()}"

    week_score = cache.get(cache_key_week)
    pending_tasks = cache.get(cache_key_pending)

    if week_score is None or pending_tasks is None:
        try:
            curr_start_dt, curr_end_dt = _ist_span_to_project_bounds(
                start_current,
                today_ist,
            )
            prev_start_dt, prev_end_dt = _ist_span_to_project_bounds(
                start_prev,
                end_prev,
            )

            curr_chk = Checklist.objects.filter(
                assign_to=request.user,
                status="Completed",
                planned_date__gte=curr_start_dt,
                planned_date__lt=curr_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            prev_chk = Checklist.objects.filter(
                assign_to=request.user,
                status="Completed",
                planned_date__gte=prev_start_dt,
                planned_date__lt=prev_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            curr_del = Delegation.objects.filter(
                assign_to=request.user,
                status="Completed",
                planned_date__gte=curr_start_dt,
                planned_date__lt=curr_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            prev_del = Delegation.objects.filter(
                assign_to=request.user,
                status="Completed",
                planned_date__gte=prev_start_dt,
                planned_date__lt=prev_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            curr_help = HelpTicket.objects.filter(
                assign_to=request.user,
                status="Closed",
                planned_date__gte=curr_start_dt,
                planned_date__lt=curr_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            prev_help = HelpTicket.objects.filter(
                assign_to=request.user,
                status="Closed",
                planned_date__gte=prev_start_dt,
                planned_date__lt=prev_end_dt,
                is_skipped_due_to_leave=False,
            ).count()

            week_score = {
                "checklist": {
                    "previous": prev_chk,
                    "current": curr_chk,
                },
                "delegation": {
                    "previous": prev_del,
                    "current": curr_del,
                },
                "help_ticket": {
                    "previous": prev_help,
                    "current": curr_help,
                },
            }

            if is_off_day_today:
                pending_tasks = {
                    "checklist": 0,
                    "delegation": 0,
                    "help_ticket": 0,
                }
            else:
                pending_tasks = {
                    "checklist": Checklist.objects.filter(
                        assign_to=request.user,
                        status="Pending",
                        is_skipped_due_to_leave=False,
                    ).count(),
                    "delegation": Delegation.objects.filter(
                        assign_to=request.user,
                        status="Pending",
                        is_skipped_due_to_leave=False,
                    ).count(),
                    "help_ticket": HelpTicket.objects.filter(
                        assign_to=request.user,
                        is_skipped_due_to_leave=False,
                    ).exclude(status="Closed").count(),
                }

            cache.set(cache_key_week, week_score, _DASH_FAST_TTL)
            cache.set(cache_key_pending, pending_tasks, _DASH_FAST_TTL)

        except Exception as e:
            logger.error(_safe_console_text(f"Error calculating weekly scores: {e}"))

            week_score = {
                "checklist": {
                    "previous": 0,
                    "current": 0,
                },
                "delegation": {
                    "previous": 0,
                    "current": 0,
                },
                "help_ticket": {
                    "previous": 0,
                    "current": 0,
                },
            }

            pending_tasks = {
                "checklist": 0,
                "delegation": 0,
                "help_ticket": 0,
            }

    # ---------------------------------------------------------------
    # Today boundaries
    # ---------------------------------------------------------------
    start_today_proj, end_today_proj = _today_project_bounds(today_ist)

    # ---------------------------------------------------------------
    # BOS Lakshya hard holiday dashboard stop
    # ---------------------------------------------------------------
    if is_off_day_today:
        reason = holiday_skip_reason(today_ist)

        logger.info(
            _safe_console_text(
                f"Dashboard actionable items suppressed for "
                f"user={request.user.username} "
                f"date={today_ist} reason={reason or 'off_day'}"
            )
        )

        return render(
            request,
            "dashboard/dashboard.html",
            {
                "week_score": week_score,
                "pending_tasks": {
                    "checklist": 0,
                    "delegation": 0,
                    "help_ticket": 0,
                },
                "tasks": [],
                "selected": selected,
                "prev_time": "00:00",
                "curr_time": "00:00",
                "today_only": today_only,
                "handed_over": {
                    "checklist": [],
                    "delegation": [],
                    "help_ticket": [],
                },
                "completed_by_delegate": {
                    "checklist": [],
                    "delegation": [],
                    "help_ticket": [],
                },
                "holiday_today": True,
            },
        )

    # ---------------------------------------------------------------
    # Normal working day flow
    # ---------------------------------------------------------------
    handover_incoming = _get_handover_tasks_for_user(
        request.user,
        today_ist,
    )

    after_10 = now_ist.timetz().replace(tzinfo=None) >= dt_time(10, 0, 0)

    checklist_qs = []
    delegation_qs = []
    help_ticket_qs = []

    try:
        # -----------------------------------------------------------
        # Checklists
        # -----------------------------------------------------------
        if today_only:
            if after_10:
                base_checklists = list(
                    Checklist.objects
                    .filter(
                        assign_to=request.user,
                        status="Pending",
                        planned_date__gte=start_today_proj,
                        planned_date__lte=end_today_proj,
                        is_skipped_due_to_leave=False,
                    )
                    .select_related("assign_by", "assign_to")
                    .order_by("planned_date")
                )
            else:
                base_checklists = []
        else:
            if after_10:
                planned_filter = {
                    "planned_date__lte": end_today_proj,
                }
            else:
                planned_filter = {
                    "planned_date__lt": start_today_proj,
                }

            base_checklists = list(
                Checklist.objects
                .filter(
                    assign_to=request.user,
                    status="Pending",
                    is_skipped_due_to_leave=False,
                    **planned_filter,
                )
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

        ho_checklists = []

        if handover_incoming["checklist"]:
            ho_checklists = list(
                Checklist.objects
                .filter(
                    id__in=handover_incoming["checklist"],
                    status="Pending",
                    is_skipped_due_to_leave=False,
                )
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

            for task in ho_checklists:
                task.is_handover = True

        all_checklists = [
            obj
            for obj in _dedupe_by_id(base_checklists + ho_checklists)
            if _not_holiday_task(obj)
        ]

        if today_only:
            checklist_qs = [
                task
                for task in all_checklists
                if _ist_date(task.planned_date) == today_ist
                and (
                    getattr(task, "is_handover", False)
                    or _should_show_checklist(task.planned_date, now_ist)
                )
            ]
        else:
            checklist_qs = [
                task
                for task in all_checklists
                if getattr(task, "is_handover", False)
                or _should_show_checklist(task.planned_date, now_ist)
            ]

        # -----------------------------------------------------------
        # Delegations
        # -----------------------------------------------------------
        if today_only:
            base_delegations = list(
                Delegation.objects
                .filter(
                    assign_to=request.user,
                    status="Pending",
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                    is_skipped_due_to_leave=False,
                )
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )
        else:
            base_delegations = list(
                Delegation.objects
                .filter(
                    assign_to=request.user,
                    status="Pending",
                    planned_date__lte=end_today_proj,
                    is_skipped_due_to_leave=False,
                )
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

        ho_delegations = []

        if handover_incoming["delegation"]:
            ho_delegations = list(
                Delegation.objects
                .filter(
                    id__in=handover_incoming["delegation"],
                    status="Pending",
                    is_skipped_due_to_leave=False,
                )
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

            for task in ho_delegations:
                task.is_handover = True

        delegation_qs = [
            obj
            for obj in _dedupe_by_id(base_delegations + ho_delegations)
            if _not_holiday_task(obj)
        ]

        # -----------------------------------------------------------
        # Help tickets
        # -----------------------------------------------------------
        if today_only:
            base_help = list(
                HelpTicket.objects
                .filter(
                    assign_to=request.user,
                    planned_date__gte=start_today_proj,
                    planned_date__lte=now_project_tz,
                    is_skipped_due_to_leave=False,
                )
                .exclude(status="Closed")
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )
        else:
            base_help = list(
                HelpTicket.objects
                .filter(
                    assign_to=request.user,
                    planned_date__lte=end_today_proj,
                    is_skipped_due_to_leave=False,
                )
                .exclude(status="Closed")
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

        ho_help = []

        if handover_incoming["help_ticket"]:
            ho_help = list(
                HelpTicket.objects
                .filter(
                    id__in=handover_incoming["help_ticket"],
                    is_skipped_due_to_leave=False,
                )
                .exclude(status="Closed")
                .select_related("assign_by", "assign_to")
                .order_by("planned_date")
            )

            for task in ho_help:
                task.is_handover = True

        help_ticket_qs = [
            obj
            for obj in _dedupe_by_id(base_help + ho_help)
            if _not_holiday_task(obj)
        ]

        logger.info(
            _safe_console_text(
                f"Dashboard filter for {request.user.username} | "
                f"today_only={today_only} | "
                f"holiday_today={is_holiday_today} | "
                f"sunday_today={is_sunday_today} | "
                f"checklist={len(checklist_qs)} | "
                f"delegation={len(delegation_qs)} | "
                f"help={len(help_ticket_qs)} | "
                f"incoming handover: "
                f"CL={len(handover_incoming['checklist'])}, "
                f"DL={len(handover_incoming['delegation'])}, "
                f"HT={len(handover_incoming['help_ticket'])}"
            )
        )

    except Exception as e:
        logger.error(_safe_console_text(f"Error querying dashboard task lists: {e}"))

        checklist_qs = []
        delegation_qs = []
        help_ticket_qs = []

    # ---------------------------------------------------------------
    # Selected tab task list
    # ---------------------------------------------------------------
    if selected == "delegation":
        tasks = delegation_qs
    elif selected == "help_ticket":
        tasks = help_ticket_qs
    else:
        tasks = checklist_qs

    # ---------------------------------------------------------------
    # Time cards
    # ---------------------------------------------------------------
    try:
        prev_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(
                assign_to=request.user,
                status="Pending",
                is_skipped_due_to_leave=False,
            ),
            start_prev,
            end_prev,
        )

        curr_min = calculate_checklist_assigned_time(
            Checklist.objects.filter(
                assign_to=request.user,
                status="Pending",
                is_skipped_due_to_leave=False,
            ),
            start_current,
            today_ist,
        )

        prev_min_del = calculate_delegation_assigned_time_safe(
            request.user,
            start_prev,
            end_prev,
        )

        curr_min_del = calculate_delegation_assigned_time_safe(
            request.user,
            start_current,
            today_ist,
        )

    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating time aggregations: {e}"))

        prev_min = 0
        curr_min = 0
        prev_min_del = 0
        curr_min_del = 0

    # ---------------------------------------------------------------
    # Handover display blocks
    # ---------------------------------------------------------------
    handed_over_full = {
        "checklist": [],
        "delegation": [],
        "help_ticket": [],
    }

    completed_by_delegate = {
        "checklist": [],
        "delegation": [],
        "help_ticket": [],
    }

    try:
        from apps.leave.models import LeaveHandover, LeaveStatus

        approved_vals = _approved_status_values(LeaveStatus)

        active_handover = (
            LeaveHandover.objects
            .filter(
                new_assignee=request.user,
                is_active=True,
                effective_start_date__lte=today_ist,
                effective_end_date__gte=today_ist,
                leave_request__status__in=approved_vals,
            )
            .select_related("leave_request", "original_assignee")
            .order_by("id")
        )

        def _row(task, handover, url_prefix: str):
            return {
                "task": task,
                "handover": handover,
                "original_assignee": getattr(handover, "original_assignee", None),
                "leave_request": getattr(handover, "leave_request", None),
                "handover_message": getattr(handover, "message", "") or "",
                "task_url": (
                    f"/{url_prefix}/{getattr(task, 'id', '')}/"
                    if getattr(task, "id", None)
                    else None
                ),
            }

        cl_ids = [
            h.original_task_id
            for h in active_handover
            if _normalize_task_type(h.task_type) == "checklist"
        ]

        dl_ids = [
            h.original_task_id
            for h in active_handover
            if _normalize_task_type(h.task_type) == "delegation"
        ]

        ht_ids = [
            h.original_task_id
            for h in active_handover
            if _normalize_task_type(h.task_type) == "help_ticket"
        ]

        checklist_map = {
            task.id: task
            for task in Checklist.objects
            .filter(
                id__in=cl_ids,
                is_skipped_due_to_leave=False,
            )
            .select_related("assign_by", "assign_to")
            if _not_holiday_task(task)
        }

        delegation_map = {
            task.id: task
            for task in Delegation.objects
            .filter(
                id__in=dl_ids,
                is_skipped_due_to_leave=False,
            )
            .select_related("assign_by", "assign_to")
            if _not_holiday_task(task)
        }

        help_map = {
            task.id: task
            for task in HelpTicket.objects
            .filter(
                id__in=ht_ids,
                is_skipped_due_to_leave=False,
            )
            .select_related("assign_by", "assign_to")
            if _not_holiday_task(task)
        }

        for handover in active_handover:
            key = _normalize_task_type(handover.task_type)

            if key == "checklist":
                task = checklist_map.get(handover.original_task_id)

                if task:
                    handed_over_full["checklist"].append(
                        _row(task, handover, "checklist")
                    )

            elif key == "delegation":
                task = delegation_map.get(handover.original_task_id)

                if task:
                    handed_over_full["delegation"].append(
                        _row(task, handover, "delegation")
                    )

            elif key == "help_ticket":
                task = help_map.get(handover.original_task_id)

                if task:
                    handed_over_full["help_ticket"].append(
                        _row(task, handover, "tickets")
                    )

        # Recently completed by delegate
        lookback_days = 14
        since_dt = timezone.now() - timedelta(days=lookback_days)

        try:
            recent_handover = (
                LeaveHandover.objects
                .filter(original_assignee=request.user)
                .select_related("new_assignee")
                .order_by("-updated_at", "-id")
            )

            cb_cl_ids = [
                h.original_task_id
                for h in recent_handover
                if _normalize_task_type(h.task_type) == "checklist"
            ]

            cb_dl_ids = [
                h.original_task_id
                for h in recent_handover
                if _normalize_task_type(h.task_type) == "delegation"
            ]

            cb_ht_ids = [
                h.original_task_id
                for h in recent_handover
                if _normalize_task_type(h.task_type) == "help_ticket"
            ]

            cb_cls = Checklist.objects.filter(
                id__in=cb_cl_ids,
                status="Completed",
                is_skipped_due_to_leave=False,
            )

            cb_dls = Delegation.objects.filter(
                id__in=cb_dl_ids,
                status="Completed",
                is_skipped_due_to_leave=False,
            )

            cb_hts = HelpTicket.objects.filter(
                id__in=cb_ht_ids,
                status="Closed",
                is_skipped_due_to_leave=False,
            )

            cb_cls = [
                task
                for task in cb_cls
                if getattr(task, "updated_at", since_dt) >= since_dt
                and _not_holiday_task(task)
            ]

            cb_dls = [
                task
                for task in cb_dls
                if getattr(task, "updated_at", since_dt) >= since_dt
                and _not_holiday_task(task)
            ]

            cb_hts = [
                task
                for task in cb_hts
                if getattr(
                    task,
                    "updated_at",
                    getattr(task, "resolved_at", since_dt),
                ) >= since_dt
                and _not_holiday_task(task)
            ]

            handover_map = {}

            for handover in recent_handover:
                handover_map[handover.original_task_id] = handover

            for task in cb_cls:
                handover = handover_map.get(task.id)

                if handover:
                    completed_by_delegate["checklist"].append(
                        _row(task, handover, "checklist")
                    )

            for task in cb_dls:
                handover = handover_map.get(task.id)

                if handover:
                    completed_by_delegate["delegation"].append(
                        _row(task, handover, "delegation")
                    )

            for task in cb_hts:
                handover = handover_map.get(task.id)

                if handover:
                    completed_by_delegate["help_ticket"].append(
                        _row(task, handover, "tickets")
                    )

        except Exception as e:
            logger.error(
                _safe_console_text(
                    f"Error building completed_by_delegate block: {e}"
                )
            )

    except (OperationalError, ProgrammingError) as e:
        logger.warning(
            _safe_console_text(
                f"Leave tables/schema not ready; skipping handed_over section: {e}"
            )
        )

    except Exception as e:
        logger.error(_safe_console_text(f"Error building handed_over section: {e}"))

    # ---------------------------------------------------------------
    # Debug samples
    # ---------------------------------------------------------------
    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            try:
                planned = getattr(task, "planned_date", None)
                planned_ist = timezone.localtime(planned, IST) if planned else None

                logger.info(
                    _safe_console_text(
                        f"sample {i}: "
                        f"'{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                        f"@ "
                        f"{planned_ist.strftime('%Y-%m-%d %H:%M IST') if planned_ist else 'No date'}"
                        f"{' (HANDOVER)' if getattr(task, 'is_handover', False) else ''}"
                    )
                )
            except Exception:
                continue

    # ---------------------------------------------------------------
    # Final render
    # ---------------------------------------------------------------
    return render(
        request,
        "dashboard/dashboard.html",
        {
            "week_score": week_score,
            "pending_tasks": pending_tasks,
            "tasks": tasks,
            "selected": selected,
            "prev_time": minutes_to_hhmm(prev_min + prev_min_del),
            "curr_time": minutes_to_hhmm(curr_min + curr_min_del),
            "today_only": today_only,
            "handed_over": handed_over_full,
            "completed_by_delegate": completed_by_delegate,
            "holiday_today": bool(is_holiday_today or is_sunday_today),
        },
    )