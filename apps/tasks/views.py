# apps/tasks/views.py
import csv
import io
import math
import re
import pytz
from datetime import datetime, timedelta, time, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from django.db import transaction

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.db.models import Q, F, Subquery, OuterRef
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_time

from django.conf import settings

from apps.users.permissions import has_permission
from .forms import (
    BulkUploadForm,
    ChecklistForm, CompleteChecklistForm,
    DelegationForm, CompleteDelegationForm,
    HelpTicketForm
)
from .models import BulkUpload, Checklist, Delegation, FMS, HelpTicket
from apps.settings.models import Holiday

# Centralized email helpers
from .email_utils import (
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
    send_checklist_unassigned_notice,
    send_delegation_assignment_to_user,
    send_help_ticket_assignment_to_user,
    send_help_ticket_admin_confirmation,
    send_help_ticket_unassigned_notice,
    send_admin_bulk_summary,
)

User = get_user_model()

# Who can create special items
can_create = lambda u: u.is_superuser or u.groups.filter(
    name__in=['Admin', 'Manager', 'EA', 'CEO']
).exists()

# Base URL (prefer settings.SITE_URL if present)
site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# IST and default assignment time
IST = pytz.timezone('Asia/Kolkata')
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

# Feature flags (avoid email-induced request blocking in production)
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", False)
SEND_EMAILS_ON_BULK = getattr(settings, "SEND_EMAILS_ON_BULK", False)

# ---- Recurrence / Working-day helpers ---------------------------------------

RECURRING_MODES = ['Daily', 'Weekly', 'Monthly', 'Yearly']

def is_working_day(dt: date) -> bool:
    """True if the given date is not Sunday and not a Holiday."""
    return dt.weekday() != 6 and not Holiday.objects.filter(date=dt).exists()

def next_working_day(dt: date) -> date:
    """Return the next working date on/after dt."""
    while not is_working_day(dt):
        dt += timedelta(days=1)
    return dt

def normalize_planned_dt(dt: datetime) -> datetime:
    """
    Given a datetime (naive or aware):
      - snap to 10:00 IST
      - if date is non-working (Sun/Holiday), move to next working day (still 10:00 IST)
      - return as aware datetime in project's current timezone
    """
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, tz)

    dt_ist = dt.astimezone(IST).replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)

    d = dt_ist.date()
    if not is_working_day(d):
        d = next_working_day(d)
        dt_ist = IST.localize(datetime(d.year, d.month, d.day, ASSIGN_HOUR, ASSIGN_MINUTE))

    return dt_ist.astimezone(tz)

# ---- Time-zone safe helpers (preserve user-chosen time) ---------------------

def make_aware_assuming_ist(dt: datetime) -> datetime:
    """
    If naive -> interpret as IST; return aware in project tz.
    """
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    return dt.astimezone(tz)

def normalize_planned_dt_preserve_time(dt: datetime) -> datetime:
    """
    Keep original time; if non-working date, roll DATE forward (time unchanged).
    """
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = IST.localize(dt)

    dt_ist = dt.astimezone(IST)
    d = dt_ist.date()
    hh, mm, ss, us = dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond

    if not is_working_day(d):
        d = next_working_day(d)
        dt_ist = IST.localize(datetime(d.year, d.month, d.day, hh, mm, ss, us))

    return dt_ist.astimezone(tz)

# ---- First-occurrence keeper -------------------------------------------------

def keep_first_occurrence(dt: datetime) -> datetime:
    """
    First occurrence keeps EXACT date+time; naive interpreted as IST.
    """
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    return dt.astimezone(tz)

def next_recurring_datetime(prev_dt: datetime, mode: str, frequency: int | None) -> datetime | None:
    """
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

def _series_filter_kwargs(task: Checklist) -> dict:
    """Identify a 'series' of a recurring checklist task."""
    return dict(
        assign_to_id=task.assign_to_id,
        task_name=task.task_name,
        mode=task.mode,
        frequency=task.frequency,
        group_name=task.group_name,
    )

def create_next_if_recurring(task: Checklist) -> None:
    """
    Create next pending checklist (idempotent).
    Sends emails only when SEND_EMAILS_FOR_AUTO_RECUR is True.
    """
    if (task.mode or '') not in RECURRING_MODES:
        return

    nxt_dt = next_recurring_datetime(task.planned_date, task.mode, task.frequency)
    if not nxt_dt:
        return

    if Checklist.objects.filter(
        status='Pending',
        planned_date__gte=nxt_dt - timedelta(minutes=1),
        **_series_filter_kwargs(task)
    ).exists():
        return

    new_obj = Checklist.objects.create(
        assign_by=task.assign_by,
        task_name=task.task_name,
        message=task.message,
        assign_to=task.assign_to,
        planned_date=nxt_dt,
        priority=task.priority,
        attachment_mandatory=task.attachment_mandatory,
        mode=task.mode,
        frequency=task.frequency,
        time_per_task_minutes=task.time_per_task_minutes,
        remind_before_days=task.remind_before_days,
        assign_pc=task.assign_pc,
        notify_to=task.notify_to,
        set_reminder=task.set_reminder,
        reminder_mode=task.reminder_mode,
        reminder_frequency=task.reminder_frequency,
        reminder_starting_time=task.reminder_starting_time,
        checklist_auto_close=task.checklist_auto_close,
        checklist_auto_close_days=task.checklist_auto_close_days,
        group_name=task.group_name,
        actual_duration_minutes=0,
        status='Pending',
    )

    # Avoid SMTP blocking on the request path unless explicitly enabled.
    if SEND_EMAILS_FOR_AUTO_RECUR:
        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
        try:
            send_checklist_assignment_to_user(
                task=new_obj,
                complete_url=complete_url,
                subject_prefix="Recurring Checklist Generated"
            )
            send_checklist_admin_confirmation(
                task=new_obj,
                subject_prefix="Recurring Checklist Generated"
            )
        except Exception:
            # Swallow email errors so web requests never 500 here
            pass

def ensure_next_for_all_recurring() -> None:
    """
    If latest item in a series is not future-pending, create the next one.
    """
    now = timezone.now()
    seeds = (Checklist.objects
             .filter(mode__in=RECURRING_MODES)
             .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
             .distinct())
    for s in seeds:
        last = (Checklist.objects
                .filter(**s)
                .order_by('-planned_date', '-id')
                .first())
        if not last:
            continue
        if Checklist.objects.filter(status='Pending', planned_date__gt=now, **s).exists():
            continue
        if last.planned_date <= now:
            create_next_if_recurring(last)

# ---- Parsing helpers (default MDY for slash dates like Excel) ----------------

def _try_dt(s, fmt):
    try:
        return datetime.strptime(s, fmt)
    except Exception:
        return None

_SLASH_DT_WITH_TIME = re.compile(
    r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::\d{2})?\s*([APMapm]{2})?\s*$"
)

def parse_planned_datetime_str(s: str):
    s = re.sub(r"\s+", " ", (s or "").strip())
    if not s:
        return None

    fmts = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %I:%M:%S %p",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %I:%M %p",
        "%Y/%m/%d %I:%M:%S %p",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %I:%M %p",
        "%d-%m-%Y %I:%M:%S %p",
        "%m-%d-%Y %H:%M",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y %I:%M %p",
        "%m-%d-%Y %I:%M:%S %p",
    ]
    for fmt in fmts:
        dt = _try_dt(s, fmt)
        if dt:
            return dt

    m = _SLASH_DT_WITH_TIME.match(s)
    if m:
        mdy = (_try_dt(s, "%m/%d/%Y %H:%M") or _try_dt(s, "%m/%d/%Y %I:%M %p") or _try_dt(s, "%m/%d/%Y %H:%M:%S"))
        if mdy:
            return mdy
        dmy = (_try_dt(s, "%d/%m/%Y %H:%M") or _try_dt(s, "%d/%m/%Y %I:%M %p") or _try_dt(s, "%d/%m/%Y %H:%M:%S"))
        return dmy

    mdy = (_try_dt(s, "%m/%d/%Y %H:%M") or _try_dt(s, "%m/%d/%Y %I:%M %p") or _try_dt(s, "%m/%d/%Y %H:%M:%S"))
    if mdy:
        return mdy
    dmy = (_try_dt(s, "%d/%m/%Y %H:%M") or _try_dt(s, "%d/%m/%Y %I:%M %p") or _try_dt(s, "%d/%m/%Y %H:%M:%S"))
    return dmy

def parse_date_only_str(s: str):
    s = re.sub(r"\s+", " ", (s or "").strip())
    if not s:
        return None

    fmts = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y"]
    for fmt in fmts:
        dt = _try_dt(s, fmt)
        if dt:
            return dt.date()

    mdy = _try_dt(s, "%m/%d/%Y")
    if mdy:
        return mdy.date()
    dmy = _try_dt(s, "%d/%m/%Y")
    return dmy.date() if dmy else None

def parse_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, float):
        return default if math.isnan(val) else int(val)
    s = str(val).strip()
    return int(s) if s.isdigit() else default

def parse_int_loose(val, default=None):
    """
    Accepts floats like 1.0; pulls digits out of strings; None/NaN -> default.
    """
    if val is None:
        return default
    if isinstance(val, float):
        if math.isnan(val):
            return default
        return int(val)
    s = str(val).strip()
    if s == "":
        return default
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else default

def parse_time_val(val):
    if val is None:
        return None
    if hasattr(val, 'hour') and hasattr(val, 'minute'):
        return val
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime().time()
    s = str(val).strip()
    return parse_time(s) if s else None

def get_default_time():
    return time(ASSIGN_HOUR, ASSIGN_MINUTE)

# ---- Row cleaning for bulk upload -------------------------------------------

def _clean_row_keys(row: dict) -> dict:
    if not row:
        return {}
    out = {}
    for k, v in row.items():
        key = k.strip() if isinstance(k, str) else k
        out[key] = v
    return out

def _is_blank_cell(v) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    if isinstance(v, str) and v.strip() == "":
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return False

def _is_empty_row(row: dict) -> bool:
    if not row:
        return True
    return all(_is_blank_cell(v) for v in row.values())

# ---- Delegation time aggregation --------------------------------------------

def _coerce_date(dtor):
    if isinstance(dtor, datetime):
        return dtor.date()
    return dtor

def calculate_delegation_assigned_time(qs, up_to=None):
    if up_to is None:
        up_to = timezone.localdate()
    total = 0
    for d in qs:
        freq = d.frequency or 1
        minutes = d.time_per_task_minutes or 0
        mode = getattr(d, 'mode', 'Daily')
        start_date = _coerce_date(d.planned_date)
        if start_date > up_to:
            continue
        occur = 0
        if mode == 'Daily':
            days = (up_to - start_date).days
            if days >= 0:
                occur = (days // freq) + 1
        elif mode == 'Weekly':
            delta_weeks = ((up_to - start_date).days // 7)
            if delta_weeks >= 0:
                occur = (delta_weeks // freq) + 1
        elif mode == 'Monthly':
            months = (up_to.year - start_date.year) * 12 + (up_to.month - start_date.month)
            if up_to.day < start_date.day:
                months -= 1
            if months >= 0:
                occur = (months // freq) + 1
        elif mode == 'Yearly':
            years = up_to.year - start_date.year
            if (up_to.month, up_to.day) < (start_date.month, start_date.day):
                years -= 1
            if years >= 0:
                occur = (years // freq) + 1
        else:
            occur = 1 if start_date <= up_to else 0
        total += occur * minutes
    return total

def calculate_delegation_actual_time(qs, up_to=None):
    if up_to is None:
        up_to = timezone.localdate()
    total = 0
    for d in qs.filter(status='Completed'):
        if d.completed_at:
            comp_date = d.completed_at.date() if hasattr(d.completed_at, 'date') else d.completed_at
            if comp_date <= up_to:
                total += d.actual_duration_minutes or 0
    return total

# ---- Checklist pages ---------------------------------------------------------

def _delete_series_for(instance: Checklist) -> int:
    """
    Delete ALL pending items in the same recurring series as `instance`.
    Returns number of rows deleted.
    """
    if not instance:
        return 0
    filters = _series_filter_kwargs(instance)
    deleted, _ = Checklist.objects.filter(status='Pending', **filters).delete()
    return deleted

@has_permission('list_checklist')
def list_checklist(request):
    # Only check/generate next occurrences on GET,
    # and *skip once* immediately after a delete so the list doesn't repopulate.
    if request.method == 'GET':
        if not request.session.pop('suppress_auto_recur', False):
            ensure_next_for_all_recurring()

    if request.method == 'POST':
        # 1) Per-row "Delete series" button (hidden mini-form)
        if request.POST.get('action') == 'delete_series' and request.POST.get('pk'):
            try:
                obj = Checklist.objects.get(pk=int(request.POST['pk']))
            except (Checklist.DoesNotExist, ValueError, TypeError):
                messages.warning(request, "The selected series no longer exists.")
                return redirect('tasks:list_checklist')
            deleted = _delete_series_for(obj)
            if deleted:
                messages.success(request, f"Deleted {deleted} occurrence(s) from the series “{obj.task_name}”.")
            else:
                messages.info(request, "No pending occurrences found to delete for that series.")
            request.session['suppress_auto_recur'] = True
            return redirect('tasks:list_checklist')

        # 2) Bulk delete (optionally expand to whole series)
        ids = request.POST.getlist('sel')
        with_series = bool(request.POST.get('with_series'))
        total_deleted = 0

        if ids:
            if with_series:
                # Expand each selected row to its series and delete all pending in each
                series_seen = set()
                for sid in ids:
                    try:
                        obj = Checklist.objects.get(pk=int(sid))
                    except (Checklist.DoesNotExist, ValueError, TypeError):
                        continue
                    key = tuple(sorted(_series_filter_kwargs(obj).items()))
                    if key in series_seen:
                        continue
                    series_seen.add(key)
                    total_deleted += _delete_series_for(obj)
                if total_deleted:
                    messages.success(request, f"Deleted {total_deleted} pending occurrence(s) across selected series.")
                else:
                    messages.info(request, "Nothing to delete – no pending occurrences in selected series.")
            else:
                # Only delete the selected occurrences
                deleted, _ = Checklist.objects.filter(pk__in=ids).delete()
                total_deleted += deleted
                if deleted:
                    messages.success(request, f"Deleted {deleted} selected task(s).")
                else:
                    messages.info(request, "Nothing was deleted. The selected tasks may have already been removed.")

            # prevent auto-recreate on redirect
            request.session['suppress_auto_recur'] = True

        return redirect('tasks:list_checklist')

    # Show 1st of each recurring series + all one-time
    one_time_qs = Checklist.objects.exclude(mode__in=RECURRING_MODES).filter(status='Pending')
    base_rec = Checklist.objects.filter(status='Pending', mode__in=RECURRING_MODES)

    first_recurring_pk = Subquery(
        Checklist.objects.filter(
            status='Pending',
            assign_to=OuterRef('assign_to'),
            task_name=OuterRef('task_name'),
            mode=OuterRef('mode'),
            frequency=OuterRef('frequency'),
            group_name=OuterRef('group_name'),
        ).order_by('planned_date', 'id').values('pk')[:1]
    )

    recurring_first_qs = (
        base_rec.annotate(first_pk=first_recurring_pk).filter(pk=F('first_pk')).values('pk')
    )

    qs = Checklist.objects.filter(Q(pk__in=recurring_first_qs) | Q(pk__in=one_time_qs.values('pk')))

    # Filters
    if (kw := request.GET.get('keyword', '').strip()):
        qs = qs.filter(Q(task_name__icontains=kw) | Q(message__icontains=kw))
    for param, lookup in [
        ('assign_to', 'assign_to_id'),
        ('priority', 'priority'),
        ('group_name', 'group_name__icontains'),
        ('start_date', 'planned_date__date__gte'),
        ('end_date', 'planned_date__date__lte'),
    ]:
        if (v := request.GET.get(param, '').strip()):
            qs = qs.filter(**{lookup: v})
    if request.GET.get('today_only'):
        today = timezone.localdate()
        qs = qs.filter(planned_date__date=today)

    items = qs.order_by('-planned_date', '-id')

    if request.GET.get('download'):
        resp = HttpResponse(content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename="checklist.csv"'
        w = csv.writer(resp)
        w.writerow(['Task Name', 'Assign To', 'Planned Date', 'Priority', 'Group Name', 'Status'])
        for itm in items:
            w.writerow([
                itm.task_name,
                itm.assign_to.get_full_name() or itm.assign_to.username,
                itm.planned_date.strftime('%Y-%m-%d %H:%M'),
                itm.priority,
                itm.group_name,
                itm.status,
            ])
        return resp

    ctx = {
        'items': items,
        'users': User.objects.order_by('username'),
        'priority_choices': Checklist._meta.get_field('priority').choices,
        'group_names': Checklist.objects.order_by('group_name').values_list('group_name', flat=True).distinct(),
        'current_tab': 'checklist',
    }
    if request.GET.get('partial'):
        return render(request, 'tasks/partial_list_checklist.html', ctx)
    return render(request, 'tasks/list_checklist.html', ctx)


@has_permission('add_checklist')
def add_checklist(request):
    if request.method == 'POST':
        form = ChecklistForm(request.POST, request.FILES)
        if form.is_valid():
            planned_date = form.cleaned_data.get('planned_date')
            if planned_date:
                planned_date = keep_first_occurrence(planned_date)
            obj = form.save(commit=False)
            obj.planned_date = planned_date
            obj.save()
            form.save_m2m()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
            try:
                send_checklist_assignment_to_user(task=obj, complete_url=complete_url, subject_prefix="New Checklist Task Assigned")
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Assignment")
            except Exception:
                pass

            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(initial={'assign_by': request.user})
    return render(request, 'tasks/add_checklist.html', {'form': form})

@has_permission('add_checklist')
def edit_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    old_assignee = obj.assign_to
    if request.method == 'POST':
        form = ChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = form.cleaned_data.get('planned_date')
            if planned_date:
                planned_date = keep_first_occurrence(planned_date)
            obj2 = form.save(commit=False)
            obj2.planned_date = planned_date
            obj2.save()
            form.save_m2m()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj2.id])}"
            try:
                if old_assignee and obj2.assign_to_id != old_assignee.id:
                    send_checklist_unassigned_notice(task=obj2, old_user=old_assignee)
                    send_checklist_assignment_to_user(task=obj2, complete_url=complete_url, subject_prefix="Checklist Task Reassigned")
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Reassigned")
                else:
                    send_checklist_assignment_to_user(task=obj2, complete_url=complete_url, subject_prefix="Checklist Task Updated")
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Updated")
            except Exception:
                pass

            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(instance=obj)
    return render(request, 'tasks/add_checklist.html', {'form': form})

@has_permission('list_checklist')
def delete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        obj.delete()
        # prevent immediate auto-regeneration on redirect
        request.session['suppress_auto_recur'] = True
        messages.success(request, f"Deleted checklist task “{obj.task_name}”.")
        return redirect('tasks:list_checklist')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Checklist'})

@has_permission('list_checklist')
def reassign_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        old_assignee = obj.assign_to
        if uid := request.POST.get('assign_to'):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
            try:
                send_checklist_assignment_to_user(task=obj, complete_url=complete_url, subject_prefix="Checklist Task Reassigned")
                if old_assignee and old_assignee.id != obj.assign_to_id:
                    send_checklist_unassigned_notice(task=obj, old_user=old_assignee)
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Reassigned")
            except Exception:
                pass

            return redirect('tasks:list_checklist')
    return render(request, 'tasks/reassign_checklist.html', {
        'object': obj,
        'all_users': User.objects.order_by('username')
    })

@login_required
def complete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk, assign_to=request.user)
    if request.method == 'POST':
        form = CompleteChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            form.save()
            now = timezone.now()
            obj.status = 'Completed'
            obj.completed_at = now
            mins = int((now - obj.planned_date).total_seconds() // 60)
            obj.actual_duration_minutes = max(mins, 0)
            obj.save()
            create_next_if_recurring(obj)
            return redirect(request.GET.get('next', 'dashboard:home'))
    else:
        form = CompleteChecklistForm(instance=obj)
    return render(request, 'tasks/complete_checklist.html', {'form': form, 'object': obj})

# ---- Delegation pages --------------------------------------------------------

@has_permission('list_delegation')
def list_delegation(request):
    if request.method == 'POST':
        if ids := request.POST.getlist('sel'):
            Delegation.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_delegation')
    items = Delegation.objects.all().order_by('-planned_date')

    if request.GET.get('today_only'):
        today = timezone.localdate()
        items = items.filter(planned_date__date=today)

    up_to = timezone.localdate()
    assign_time = calculate_delegation_assigned_time(items, up_to)
    actual_time = calculate_delegation_actual_time(items, up_to)

    ctx = {
        'items': items,
        'current_tab': 'delegation',
        'assign_time': assign_time,
        'actual_time': actual_time,
    }
    if request.GET.get('partial'):
        return render(request, 'tasks/partial_list_delegation.html', ctx)
    return render(request, 'tasks/list_delegation.html', ctx)

@has_permission('add_delegation')
def add_delegation(request):
    if request.method == 'POST':
        form = DelegationForm(request.POST, request.FILES)
        if form.is_valid():
            planned_dt = form.cleaned_data.get('planned_date')
            if planned_dt:
                planned_dt = keep_first_occurrence(planned_dt)
            obj = form.save(commit=False)
            obj.planned_date = planned_dt
            obj.save()

            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
            try:
                send_delegation_assignment_to_user(delegation=obj, complete_url=complete_url, subject_prefix="New Delegation Task Assigned")
            except Exception:
                pass

            return redirect('tasks:list_delegation')
    else:
        form = DelegationForm(initial={'assign_by': request.user})
    return render(request, 'tasks/add_delegation.html', {'form': form})

@has_permission('add_delegation')
def edit_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        form = DelegationForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_dt = form.cleaned_data.get('planned_date')
            if planned_dt:
                planned_dt = keep_first_occurrence(planned_dt)
            obj2 = form.save(commit=False)
            obj2.planned_date = planned_dt
            obj2.save()

            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj2.id])}"
            try:
                send_delegation_assignment_to_user(delegation=obj2, complete_url=complete_url, subject_prefix="Delegation Task Updated")
            except Exception:
                pass

            return redirect('tasks:list_delegation')
    else:
        form = DelegationForm(instance=obj)
    return render(request, 'tasks/add_delegation.html', {'form': form})

@has_permission('list_delegation')
def delete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_delegation')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Delegation'})

@has_permission('list_delegation')
def reassign_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        if uid := request.POST.get('assign_to'):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()
            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
            try:
                send_delegation_assignment_to_user(delegation=obj, complete_url=complete_url, subject_prefix="Delegation Task Reassigned")
            except Exception:
                pass
            return redirect('tasks:list_delegation')
    return render(request, 'tasks/reassign_delegation.html', {
        'object': obj,
        'all_users': User.objects.order_by('username')
    })

@login_required
def complete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk, assign_to=request.user)
    if request.method == 'POST':
        form = CompleteDelegationForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            form.save()
            now = timezone.now()
            obj.status = 'Completed'
            obj.completed_at = now
            mins = int((now - obj.planned_date).total_seconds() // 60)
            obj.actual_duration_minutes = max(mins, 0)
            obj.save()
            return redirect(request.GET.get('next', 'dashboard:home') + '?task_type=delegation')
    else:
        form = CompleteDelegationForm(instance=obj)
    return render(request, 'tasks/complete_delegation.html', {'form': form, 'object': obj})

# ---- Bulk upload (emails & admin summary) -----------------------------------

@has_permission('bulk_upload')
def bulk_upload(request):
    if request.method != 'POST':
        form = BulkUploadForm()
        return render(request, 'tasks/bulk_upload.html', {'form': form})

    form = BulkUploadForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, 'tasks/bulk_upload.html', {'form': form})

    upload = form.save(commit=False)
    f = request.FILES.get('csv_file')
    form_type = form.cleaned_data['form_type']

    if not f:
        messages.error(request, "Please choose a file to upload.")
        return render(request, 'tasks/bulk_upload.html', {'form': form})

    ext = f.name.rsplit('.', 1)[-1].lower()
    rows = []
    try:
        if ext in ('xls', 'xlsx'):
            xl = pd.read_excel(f, sheet_name=None)
            sheet_name = 'Checklist Upload' if form_type == 'checklist' else 'Delegation Upload'
            sheet = xl.get(sheet_name, next(iter(xl.values())))
            rows = sheet.to_dict('records')
        else:
            raw = f.read()
            text = None
            for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
                try:
                    text = raw.decode(enc); break
                except Exception:
                    continue
            if text is None:
                messages.error(request, "Could not decode file. Try UTF-8 CSV or Excel.")
                return render(request, 'tasks/bulk_upload.html', {'form': form})
            rows = list(csv.DictReader(io.StringIO(text)))
    except Exception:
        messages.error(request, "Could not read the file. Ensure it is a valid CSV or Excel.")
        return render(request, 'tasks/bulk_upload.html', {'form': form})

    errors = []
    to_create = []

    # ---------- CHECKLIST BULK ----------
    if form_type == 'checklist':
        required_headers = ['Task Name', 'Assign To', 'Planned Date']
        if rows:
            first_keys = [(k.strip() if isinstance(k, str) else k) for k in rows[0].keys()]
            missing = [h for h in required_headers if h not in first_keys]
        else:
            missing = required_headers
        if missing:
            messages.error(request, f"Missing required columns for Checklist: {', '.join(missing)}")
            return render(request, 'tasks/bulk_upload.html', {'form': form})

        for idx, raw_row in enumerate(rows, start=2):  # header = row 1
            row = _clean_row_keys(raw_row)
            if _is_empty_row(row):
                continue

            task_name = str(row.get('Task Name', '')).strip()
            uname = str(row.get('Assign To', '')).strip()
            planned_raw = row.get('Planned Date', '')
            priority = str(row.get('Priority', 'Low')).strip() or 'Low'
            mode_val = str(row.get('Mode', '')).strip()
            freq_val = row.get('Frequency', '') if row.get('Frequency', '') is not None else ''
            time_per = row.get('Time per Task (minutes)', 0)
            remind_before_days = row.get('Reminder Before Days', 0)
            assign_pc_u = str(row.get('Assign PC', '')).strip()
            notify_to_u = str(row.get('Notify To', '')).strip()
            set_reminder = str(row.get('Set Reminder', '')).strip().lower() in ['yes', 'true', '1']
            reminder_mode = str(row.get('Reminder Mode', '')).strip()
            reminder_frequency = row.get('Reminder Frequency', '')
            reminder_starting_time_raw = row.get('Reminder Starting Time', '')
            checklist_auto_close = str(row.get('Checklist Auto Close', '')).strip().lower() in ['yes', 'true', '1']
            checklist_auto_close_days = row.get('Checklist Auto Close Days', 0)
            message_txt = str(row.get('Message', '')).strip()
            attach_mand = str(row.get('Make Attachment Mandatory', '')).strip().lower() in ['yes', 'true', '1']
            group_name = str(row.get('Group Name', '')).strip()

            if not task_name:
                errors.append(f"Row {idx}: Task Name is required."); continue
            if not uname:
                errors.append(f"Row {idx}: Assign To (username) is required."); continue
            atou = User.objects.filter(username=uname).first()
            if not atou:
                errors.append(f"Row {idx}: Assign To username '{uname}' not found."); continue

            planned_dt = None
            try:
                if isinstance(planned_raw, pd.Timestamp):
                    planned_dt = planned_raw.to_pydatetime()
                else:
                    s = str(planned_raw).strip()
                    if s:
                        parsed = parse_planned_datetime_str(s)
                        if not parsed:
                            raise ValueError("bad dt")
                        planned_dt = parsed
            except Exception:
                planned_dt = None
            if not planned_dt:
                errors.append(f"Row {idx}: Planned Date is invalid or missing. Use 'YYYY-MM-DD HH:MM' or 'M/D/YYYY HH:MM'."); continue

            planned_dt = keep_first_occurrence(planned_dt)

            if priority not in dict(Checklist._meta.get_field('priority').choices).keys():
                errors.append(f"Row {idx}: Priority '{priority}' is invalid. Use Low/Medium/High."); continue

            try:
                freq = parse_int_loose(freq_val, default=0)
                if freq is None: freq = 0
            except Exception:
                errors.append(f"Row {idx}: Frequency must be an integer."); continue

            try:
                time_per_minutes = parse_int_loose(time_per, default=0)
                if time_per_minutes is None: time_per_minutes = 0
            except Exception:
                errors.append(f"Row {idx}: Time per Task (minutes) must be an integer."); continue

            try:
                rbd = parse_int_loose(remind_before_days, default=0)
                if rbd is None: rbd = 0
            except Exception:
                errors.append(f"Row {idx}: Reminder Before Days must be an integer."); continue

            rfreq_default = 1 if set_reminder else 0
            rfreq = parse_int_loose(reminder_frequency, default=rfreq_default)

            rst_time = None
            if reminder_starting_time_raw:
                try:
                    if isinstance(reminder_starting_time_raw, pd.Timestamp):
                        rst_time = reminder_starting_time_raw.to_pydatetime().time()
                    else:
                        rst_time = parse_time(str(reminder_starting_time_raw).strip())
                except Exception:
                    rst_time = None

            try:
                cac_days = parse_int_loose(checklist_auto_close_days, default=0)
                if cac_days is None: cac_days = 0
            except Exception:
                errors.append(f"Row {idx}: Checklist Auto Close Days must be an integer."); continue

            assign_pc = User.objects.filter(username=assign_pc_u).first() if assign_pc_u else None
            notify_to = User.objects.filter(username=notify_to_u).first() if notify_to_u else None

            to_create.append(Checklist(
                assign_by=request.user,
                task_name=task_name,
                message=message_txt,
                assign_to=atou,
                planned_date=planned_dt,
                priority=priority,
                attachment_mandatory=attach_mand,
                mode=mode_val,
                frequency=freq,
                time_per_task_minutes=time_per_minutes,
                remind_before_days=rbd,
                assign_pc=assign_pc,
                notify_to=notify_to,
                set_reminder=set_reminder,
                reminder_mode=reminder_mode or None,
                reminder_frequency=rfreq,
                reminder_starting_time=rst_time,
                checklist_auto_close=checklist_auto_close,
                checklist_auto_close_days=cac_days,
                group_name=group_name,
                actual_duration_minutes=0
            ))

        if errors:
            messages.error(request, "<ul class='mb-0'>" + "".join([f"<li>{e}</li>" for e in errors]) + "</ul>")
            return render(request, 'tasks/bulk_upload.html', {'form': BulkUploadForm()})

        created_objs = []
        with transaction.atomic():
            for obj in to_create:
                obj.save()
                created_objs.append(obj)
                if SEND_EMAILS_ON_BULK:
                    complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
                    try:
                        send_checklist_assignment_to_user(task=obj, complete_url=complete_url, subject_prefix="New Checklist Task Assigned (Bulk)")
                    except Exception:
                        pass
        # Always send admin summary (lightweight single email)
        try:
            send_admin_bulk_summary(
                title=f"{len(created_objs)} Checklist task(s) imported via Bulk Upload",
                rows=[{
                    "Task": o.task_name,
                    "Assignee": o.assign_to.get_full_name() or o.assign_to.username,
                    "Planned Date": o.planned_date,
                    "Priority": o.priority,
                } for o in created_objs]
            )
        except Exception:
            pass
        messages.success(request, f"{len(created_objs)} checklist row(s) imported successfully.")
        return redirect('tasks:bulk_upload')

    # ---------- DELEGATION BULK ----------
    else:
        required_headers = ['Task Name', 'Assign To', 'Planned Date']
        if rows:
            first_keys = [(k.strip() if isinstance(k, str) else k) for k in rows[0].keys()]
            missing = [h for h in required_headers if h not in first_keys]
        else:
            missing = required_headers
        if missing:
            messages.error(request, f"Missing required columns for Delegation: {', '.join(missing)}")
            return render(request, 'tasks/bulk_upload.html', {'form': form})

        for idx, raw_row in enumerate(rows, start=2):
            row = _clean_row_keys(raw_row)
            if _is_empty_row(row):
                continue

            task_name = str(row.get('Task Name', '')).strip()
            uname = str(row.get('Assign To', '')).strip()
            pd_val = row.get('Planned Date', '')
            priority = str(row.get('Priority', 'Low')).strip() or 'Low'
            attach_mand = str(row.get('Make Attachment Mandatory', '')).strip().lower() in ['yes', 'true', '1']
            time_per = row.get('Time per Task (minutes)', 0)
            mode_val = str(row.get('Mode', '')).strip()
            freq_val = row.get('Frequency', '') if row.get('Frequency', '') is not None else ''

            if not task_name:
                errors.append(f"Row {idx}: Task Name is required."); continue
            if not uname:
                errors.append(f"Row {idx}: Assign To (username) is required."); continue
            atou = User.objects.filter(username=uname).first()
            if not atou:
                errors.append(f"Row {idx}: Assign To username '{uname}' not found."); continue

            planned_dt = None
            try:
                if isinstance(pd_val, pd.Timestamp):
                    planned_dt = pd_val.to_pydatetime()
                else:
                    s = str(pd_val).strip()
                    if s:
                        parsed = parse_planned_datetime_str(s)
                        if not parsed:
                            raise ValueError("bad date-time")
                        planned_dt = parsed
            except Exception:
                planned_dt = None
            if not planned_dt:
                errors.append(f"Row {idx}: Planned Date is invalid or missing. Use 'YYYY-MM-DD HH:MM' or 'M/D/YYYY HH:MM'."); continue

            planned_dt = keep_first_occurrence(planned_dt)

            if priority not in dict(Delegation._meta.get_field('priority').choices).keys():
                errors.append(f"Row {idx}: Priority '{priority}' is invalid. Use Low/Medium/High."); continue

            try:
                time_per_minutes = parse_int_loose(time_per, default=0)
                if time_per_minutes is None: time_per_minutes = 0
            except Exception:
                errors.append(f"Row {idx}: Time per Task (minutes) must be an integer."); continue

            try:
                freq = parse_int_loose(freq_val, default=0)
                if freq is None: freq = 0
            except Exception:
                errors.append(f"Row {idx}: Frequency must be an integer."); continue

            to_create.append(Delegation(
                assign_by=request.user,
                task_name=task_name,
                assign_to=atou,
                planned_date=planned_dt,
                priority=priority,
                attachment_mandatory=attach_mand,
                time_per_task_minutes=time_per_minutes,
                mode=mode_val,
                frequency=freq,
                actual_duration_minutes=0
            ))

        if errors:
            messages.error(request, "<ul class='mb-0'>" + "".join([f"<li>{e}</li>" for e in errors]) + "</ul>")
            return render(request, 'tasks/bulk_upload.html', {'form': BulkUploadForm()})

        created_objs = []
        with transaction.atomic():
            for obj in to_create:
                obj.save()
                created_objs.append(obj)
                if SEND_EMAILS_ON_BULK:
                    complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
                    try:
                        send_delegation_assignment_to_user(delegation=obj, complete_url=complete_url, subject_prefix="New Delegation Task Assigned (Bulk)")
                    except Exception:
                        pass

        try:
            send_admin_bulk_summary(
                title=f"{len(created_objs)} Delegation task(s) imported via Bulk Upload",
                rows=[{
                    "Task": o.task_name,
                    "Assignee": o.assign_to.get_full_name() or o.assign_to.username,
                    "Planned Date": o.planned_date,
                    "Priority": o.priority,
                } for o in created_objs]
            )
        except Exception:
            pass

        messages.success(request, f"{len(created_objs)} delegation row(s) imported successfully.")
        return redirect('tasks:bulk_upload')

# ---- Downloads, tickets, FMS -------------------------------------------------

@has_permission('bulk_upload')
def download_checklist_template(request):
    path = finders.find('bulk_upload_templates/checklist_template.csv')
    if not path:
        raise Http404
    return FileResponse(open(path, 'rb'), as_attachment=True, filename='checklist_template.csv')

@has_permission('bulk_upload')
def download_delegation_template(request):
    path = finders.find('bulk_upload_templates/delegation_template.csv')
    if not path:
        raise Http404
    return FileResponse(open(path, 'rb'), as_attachment=True, filename='delegation_template.csv')

@login_required
def list_fms(request):
    items = FMS.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_fms.html', {'items': items})

@login_required
def list_help_ticket(request):
    qs = HelpTicket.objects.select_related('assign_by', 'assign_to')
    if not can_create(request.user):
        qs = qs.filter(assign_to=request.user)
    for param, lookup in [
        ('from_date', 'planned_date__date__gte'),
        ('to_date',   'planned_date__date__lte'),
    ]:
        if v := request.GET.get(param, '').strip():
            qs = qs.filter(**{lookup: v})
    for param, lookup in [
        ('assign_by', 'assign_by_id'),
        ('assign_to', 'assign_to_id'),
        ('status',    'status'),
    ]:
        v = request.GET.get(param, 'all')
        if v != 'all':
            qs = qs.filter(**{lookup: v})
    items = qs.order_by('-planned_date')
    return render(request, 'tasks/list_help_ticket.html', {
        'items': items,
        'current_tab': 'all',
        'can_create': can_create(request.user),
        'users': User.objects.order_by('username'),
        'status_choices': HelpTicket.STATUS_CHOICES,
    })

@login_required
def assigned_to_me(request):
    items = HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').order_by('-planned_date')
    return render(request, 'tasks/list_help_ticket_assigned_to.html', {'items': items, 'current_tab': 'assigned_to'})

@login_required
def assigned_by_me(request):
    items = HelpTicket.objects.filter(assign_by=request.user).order_by('-planned_date')
    return render(request, 'tasks/list_help_ticket_assigned_by.html', {'items': items, 'current_tab': 'assigned_by'})

@login_required
def add_help_ticket(request):
    if request.method == 'POST':
        form = HelpTicketForm(request.POST, request.FILES)
        if form.is_valid():
            planned_date = form.cleaned_data.get('planned_date')
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is holiday date, you can not add on this day.")
                return render(request, 'tasks/add_help_ticket.html', {
                    'form': form, 'current_tab': 'add', 'can_create': can_create(request.user)
                })
            ticket = form.save(commit=False)
            ticket.assign_by = request.user
            ticket.save()

            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                send_help_ticket_assignment_to_user(ticket=ticket, complete_url=complete_url, subject_prefix="New Help Ticket Assigned")
                send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Assignment")
            except Exception:
                pass

            return redirect('tasks:list_help_ticket')
    else:
        form = HelpTicketForm()
    return render(request, 'tasks/add_help_ticket.html', {
        'form': form, 'current_tab': 'add', 'can_create': can_create(request.user)
    })

@login_required
def edit_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    old_assignee = obj.assign_to
    if request.method == 'POST':
        form = HelpTicketForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = form.cleaned_data.get('planned_date')
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is holiday date, you can not add on this day.")
                return render(request, 'tasks/add_help_ticket.html', {
                    'form': form, 'current_tab': 'edit', 'can_create': can_create(request.user)
                })
            ticket = form.save()

            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                if old_assignee and ticket.assign_to_id != old_assignee.id:
                    send_help_ticket_unassigned_notice(ticket=ticket, old_user=old_assignee)
                    send_help_ticket_assignment_to_user(ticket=ticket, complete_url=complete_url, subject_prefix="Help Ticket Reassigned")
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Reassigned")
                else:
                    send_help_ticket_assignment_to_user(ticket=ticket, complete_url=complete_url, subject_prefix="Help Ticket Updated")
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Updated")
            except Exception:
                pass

            return redirect('tasks:list_help_ticket')
    else:
        form = HelpTicketForm(instance=obj)
    return render(request, 'tasks/add_help_ticket.html', {
        'form': form, 'current_tab': 'edit', 'can_create': can_create(request.user)
    })

@login_required
def complete_help_ticket(request, pk):
    return redirect('tasks:note_help_ticket', pk=pk)

@login_required
def note_help_ticket(request, pk):
    ticket = get_object_or_404(HelpTicket, pk=pk, assign_to=request.user)
    if request.method == 'POST':
        notes = request.POST.get('resolved_notes', '').strip()
        ticket.resolved_notes = notes
        if 'media_upload' in request.FILES:
            ticket.media_upload = request.FILES['media_upload']
        if ticket.status != 'Closed':
            ticket.status = 'Closed'
            ticket.resolved_at = timezone.now()
            ticket.resolved_by = request.user
            if ticket.resolved_at and ticket.planned_date:
                mins = int((ticket.resolved_at - ticket.planned_date).total_seconds() // 60)
                ticket.actual_duration_minutes = max(mins, 0)
        ticket.save()
        if ticket.status == 'Closed':
            recipients = []
            if ticket.assign_to.email:
                recipients.append(ticket.assign_to.email)
            if ticket.assign_by.email and ticket.assign_by.email not in recipients:
                recipients.append(ticket.assign_by.email)
            if recipients:
                from django.core.mail import EmailMultiAlternatives
                from django.template.loader import render_to_string
                subject = f"Help Ticket Closed: {ticket.title}"
                html_message = render_to_string('email/help_ticket_closed.html', {
                    'ticket': ticket, 'assign_by': ticket.assign_by, 'assign_to': ticket.assign_to,
                })
                try:
                    msg = EmailMultiAlternatives(subject, html_message, getattr(settings, "DEFAULT_FROM_EMAIL", None), recipients)
                    msg.attach_alternative(html_message, "text/html")
                    msg.send(fail_silently=True)  # never block user flow on close-notification
                except Exception:
                    pass
        messages.success(request, f"Note saved for HT-{ticket.id}.")
        return redirect(request.GET.get('next', reverse('tasks:assigned_to_me')))
    return render(request, 'tasks/note_help_ticket.html', {
        'ticket': ticket, 'next': request.GET.get('next', reverse('tasks:assigned_to_me'))
    })

@login_required
def delete_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_help_ticket')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'HelpTicket'})
