import csv
import io
import math
import pytz
from datetime import datetime, timedelta, time
import pandas as pd
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.db.models import Q
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime, parse_time
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string

from apps.users.permissions import has_permission
from .forms import (
    BulkUploadForm,
    ChecklistForm, CompleteChecklistForm,
    DelegationForm, CompleteDelegationForm,
    HelpTicketForm
)
from .models import BulkUpload, Checklist, Delegation, FMS, HelpTicket
from apps.settings.models import Holiday

User = get_user_model()
can_create = lambda u: u.is_superuser or u.groups.filter(
    name__in=['Admin', 'Manager', 'EA', 'CEO']
).exists()

site_url = "https://ems-system-d26q.onrender.com"

IST = pytz.timezone('Asia/Kolkata')
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

def is_working_day(dt):
    return dt.weekday() != 6 and not Holiday.objects.filter(date=dt).exists()

def next_working_day(dt):
    while not is_working_day(dt):
        dt += timedelta(days=1)
    return dt

def get_next_planned_datetime(prev_dt, mode, freq, orig_weekday=None, orig_day=None):
    date_part = prev_dt.date()
    if mode == "Daily":
        n = 0
        d = date_part
        while n < freq:
            d += timedelta(days=1)
            if is_working_day(d):
                n += 1
        return IST.localize(datetime.combine(d, time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    elif mode == "Weekly":
        weeks = freq
        base = date_part + timedelta(weeks=weeks)
        for i in range(7):
            d = base + timedelta(days=i)
            if d.weekday() == orig_weekday and is_working_day(d):
                return IST.localize(datetime.combine(d, time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    elif mode == "Monthly":
        y, m = date_part.year, date_part.month
        total_month = m - 1 + freq
        y += total_month // 12
        m = (total_month % 12) + 1
        d = None
        for day_try in [orig_day] + list(range(31, 27, -1)):
            try:
                d = datetime(y, m, day_try)
                break
            except ValueError:
                continue
        date_obj = d.date()
        date_obj = next_working_day(date_obj)
        return IST.localize(datetime.combine(date_obj, time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    elif mode == "Yearly":
        y = date_part.year + freq
        m = date_part.month
        d = None
        for day_try in [orig_day] + list(range(31, 27, -1)):
            try:
                d = datetime(y, m, day_try)
                break
            except ValueError:
                continue
        date_obj = d.date()
        date_obj = next_working_day(date_obj)
        return IST.localize(datetime.combine(date_obj, time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    date_obj = next_working_day(date_part)
    return IST.localize(datetime.combine(date_obj, time(ASSIGN_HOUR, ASSIGN_MINUTE)))

def create_missing_recurring_checklist_tasks():
    today = timezone.localtime(timezone.now(), IST).date()
    qs = Checklist.objects.filter(mode__in=['Daily', 'Weekly', 'Monthly', 'Yearly'])
    for checklist in qs:
        freq = checklist.frequency
        if freq < 1:
            continue
        mode = checklist.mode
        assign_to = checklist.assign_to
        task_name = checklist.task_name

        orig_weekday = checklist.planned_date.weekday() if mode == 'Weekly' else None
        orig_day = checklist.planned_date.day if mode in ['Monthly', 'Yearly'] else None

        last_task = Checklist.objects.filter(
            assign_to=assign_to,
            task_name=task_name,
            mode=mode,
        ).order_by('-planned_date').first()
        prev_dt = timezone.localtime(last_task.planned_date, IST) if last_task else timezone.localtime(checklist.planned_date, IST)
        if prev_dt.date() < today or (last_task and last_task.status == 'Completed'):
            next_dt = get_next_planned_datetime(prev_dt, mode, freq, orig_weekday, orig_day)
            if not Checklist.objects.filter(
                assign_to=assign_to,
                task_name=task_name,
                planned_date=next_dt
            ).exists():
                new_obj = Checklist.objects.create(
                    assign_by=checklist.assign_by,
                    task_name=checklist.task_name,
                    assign_to=assign_to,
                    planned_date=next_dt,
                    priority=checklist.priority,
                    attachment_mandatory=checklist.attachment_mandatory,
                    mode=mode,
                    frequency=freq,
                    time_per_task_minutes=checklist.time_per_task_minutes,
                    remind_before_days=checklist.remind_before_days,
                    message=checklist.message,
                    assign_pc=checklist.assign_pc,
                    group_name=checklist.group_name,
                    notify_to=checklist.notify_to,
                    auditor=checklist.auditor,
                    set_reminder=checklist.set_reminder,
                    reminder_mode=checklist.reminder_mode,
                    reminder_frequency=checklist.reminder_frequency,
                    reminder_before_days=checklist.reminder_before_days,
                    reminder_starting_time=checklist.reminder_starting_time,
                    checklist_auto_close=checklist.checklist_auto_close,
                    checklist_auto_close_days=checklist.checklist_auto_close_days,
                    actual_duration_minutes=0
                )
                if new_obj.assign_to.email:
                    complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
                    subject = f"New Checklist Task Assigned: {new_obj.task_name}"
                    html_message = render_to_string(
                        'email/checklist_assigned.html',
                        {
                            'task': new_obj,
                            'assign_by': new_obj.assign_by,
                            'assign_to': new_obj.assign_to,
                            'complete_url': complete_url,
                        }
                    )
                    msg = EmailMultiAlternatives(
                        subject,
                        html_message,
                        None,
                        [new_obj.assign_to.email]
                    )
                    msg.attach_alternative(html_message, "text/html")
                    msg.send(fail_silently=False)

def parse_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, float):
        return default if math.isnan(val) else int(val)
    s = str(val).strip()
    return int(s) if s.isdigit() else default

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

def calculate_delegation_assigned_time(qs, up_to=None):
    if up_to is None:
        up_to = timezone.localdate()
    total = 0
    for d in qs:
        freq = d.frequency or 1
        minutes = d.time_per_task_minutes or 0
        mode = getattr(d, 'mode', 'Daily')
        planned_date = d.planned_date
        start = planned_date
        if start > up_to:
            continue
        occur = 0
        if mode == 'Daily':
            days = (up_to - start).days
            if days >= 0:
                occur = (days // freq) + 1
        elif mode == 'Weekly':
            delta_weeks = ((up_to - start).days // 7)
            if delta_weeks >= 0:
                occur = (delta_weeks // freq) + 1
        elif mode == 'Monthly':
            months = (up_to.year - start.year) * 12 + (up_to.month - start.month)
            if up_to.day >= start.day:
                months += 0
            else:
                months -= 1
            if months >= 0:
                occur = (months // freq) + 1
        elif mode == 'Yearly':
            years = up_to.year - start.year
            if (up_to.month, up_to.day) < (start.month, start.day):
                years -= 1
            if years >= 0:
                occur = (years // freq) + 1
        else:
            occur = 1 if start <= up_to else 0
        total += occur * minutes
    return total

def calculate_delegation_actual_time(qs, up_to=None):
    if up_to is None:
        up_to = timezone.localdate()
    total = 0
    for d in qs.filter(status='Completed'):
        if d.completed_at:
            if hasattr(d.completed_at, 'date'):
                comp_date = d.completed_at.date()
            else:
                comp_date = d.completed_at
            if comp_date <= up_to:
                total += d.actual_duration_minutes or 0
    return total

@has_permission('list_checklist')
def list_checklist(request):
    create_missing_recurring_checklist_tasks()
    if request.method == 'POST':
        ids = request.POST.getlist('sel')
        if ids:
            Checklist.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_checklist')

    qs = Checklist.objects.all()
    if kw := request.GET.get('keyword', '').strip():
        qs = qs.filter(Q(task_name__icontains=kw) | Q(message__icontains=kw))
    for param, lookup in [
        ('assign_to', 'assign_to_id'),
        ('priority', 'priority'),
        ('group_name', 'group_name__icontains'),
        ('start_date', 'planned_date__date__gte'),
        ('end_date', 'planned_date__date__lte'),
    ]:
        if v := request.GET.get(param, '').strip():
            qs = qs.filter(**{lookup: v})

    if request.GET.get('today_only'):
        today = timezone.localdate()
        qs = qs.filter(planned_date__date=today)

    items = qs.order_by('-planned_date')

    if request.GET.get('download'):
        resp = HttpResponse(content_type='text/csv')
        resp['Content-Disposition'] = 'attachment; filename="checklist.csv"'
        w = csv.writer(resp)
        w.writerow([
            'Task Name', 'Assign To', 'Planned Date',
            'Priority', 'Group Name', 'Status'
        ])
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
        'group_names': Checklist.objects.order_by('group_name')
                                         .values_list('group_name', flat=True)
                                         .distinct(),
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
            obj = form.save(commit=False)
            dt = obj.planned_date
            if dt and (dt.time() == time(0, 0) or dt.time() is None):
                planned_time = get_default_time()
                planned_dt = datetime.combine(dt.date(), planned_time)
                obj.planned_date = timezone.make_aware(planned_dt)
            obj.save()
            form.save_m2m()
            if obj.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
                subject = f"New Checklist Task Assigned: {obj.task_name}"
                html_message = render_to_string(
                    'email/checklist_assigned.html',
                    {
                        'task': obj,
                        'assign_by': obj.assign_by,
                        'assign_to': obj.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=False)
            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(initial={'assign_by': request.user})
    return render(request, 'tasks/add_checklist.html', {'form': form})

@has_permission('add_checklist')
def edit_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        form = ChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            obj2 = form.save(commit=False)
            dt = obj2.planned_date
            if dt and (dt.time() == time(0, 0) or dt.time() is None):
                planned_time = get_default_time()
                planned_dt = datetime.combine(dt.date(), planned_time)
                obj2.planned_date = timezone.make_aware(planned_dt)
            obj2.save()
            form.save_m2m()
            if obj2.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj2.id])}"
                subject = f"Checklist Task Updated: {obj2.task_name}"
                html_message = render_to_string(
                    'email/checklist_assigned.html',
                    {
                        'task': obj2,
                        'assign_by': obj2.assign_by,
                        'assign_to': obj2.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj2.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(instance=obj)
    return render(request, 'tasks/add_checklist.html', {'form': form})

@has_permission('list_checklist')
def delete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_checklist')
    return render(request, 'tasks/confirm_delete.html', {
        'object': obj, 'type': 'Checklist'
    })

@has_permission('list_checklist')
def reassign_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        if uid := request.POST.get('assign_to'):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()
            if obj.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
                subject = f"Checklist Task Reassigned: {obj.task_name}"
                html_message = render_to_string(
                    'email/checklist_assigned.html',
                    {
                        'task': obj,
                        'assign_by': obj.assign_by,
                        'assign_to': obj.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
            return redirect('tasks:list_checklist')
    return render(request, 'tasks/reassign_checklist.html', {
        'object': obj,
        'all_users': User.objects.order_by('username')
    })

@login_required
def complete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk, assign_to=request.user)
    if request.method == 'POST':
        form = CompleteChecklistForm(
            request.POST, request.FILES, instance=obj
        )
        if form.is_valid():
            form.save()
            now = timezone.now()
            obj.status = 'Completed'
            obj.completed_at = now
            mins = int((now - obj.planned_date).total_seconds() // 60)
            obj.actual_duration_minutes = max(mins, 0)
            obj.save()
            return redirect(request.GET.get('next', 'dashboard:home'))
    else:
        form = CompleteChecklistForm(instance=obj)
    return render(request, 'tasks/complete_checklist.html', {
        'form': form, 'object': obj
    })

@has_permission('list_delegation')
def list_delegation(request):
    if request.method == 'POST':
        if ids := request.POST.getlist('sel'):
            Delegation.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_delegation')
    items = Delegation.objects.all().order_by('-planned_date')

    if request.GET.get('today_only'):
        today = timezone.localdate()
        items = items.filter(planned_date=today)

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
            obj = form.save()
            if obj.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
                subject = f"New Delegation Task Assigned: {obj.task_name}"
                html_message = render_to_string(
                    'email/delegation_assigned.html',
                    {
                        'delegation': obj,
                        'assign_by': obj.assign_by,
                        'assign_to': obj.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
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
            obj2 = form.save()
            if obj2.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj2.id])}"
                subject = f"Delegation Task Updated: {obj2.task_name}"
                html_message = render_to_string(
                    'email/delegation_assigned.html',
                    {
                        'delegation': obj2,
                        'assign_by': obj2.assign_by,
                        'assign_to': obj2.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj2.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
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
    return render(request, 'tasks/confirm_delete.html', {
        'object': obj, 'type': 'Delegation'
    })

@has_permission('list_delegation')
def reassign_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        if uid := request.POST.get('assign_to'):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()
            if obj.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
                subject = f"Delegation Task Reassigned: {obj.task_name}"
                html_message = render_to_string(
                    'email/delegation_assigned.html',
                    {
                        'delegation': obj,
                        'assign_by': obj.assign_by,
                        'assign_to': obj.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [obj.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
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
            planned_datetime = datetime.combine(obj.planned_date, time(0, 0), tzinfo=now.tzinfo)
            mins = int((now - planned_datetime).total_seconds() // 60)
            obj.actual_duration_minutes = max(mins, 0)
            obj.save()
            return redirect(
                request.GET.get('next', 'dashboard:home') +
                '?task_type=delegation'
            )
    else:
        form = CompleteDelegationForm(instance=obj)
    return render(request, 'tasks/complete_delegation.html', {
        'form': form, 'object': obj
    })

@has_permission('bulk_upload')
def bulk_upload(request):
    if request.method == 'POST':
        form = BulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            f = upload.csv_file
            ext = f.name.rsplit('.', 1)[-1].lower()
            if ext in ('xls', 'xlsx'):
                xl = pd.read_excel(f, sheet_name=None)
                sheet = (
                    'Checklist Upload'
                    if upload.form_type == 'checklist'
                    else 'Delegation Upload'
                )
                rows = xl.get(sheet, next(iter(xl.values()))).to_dict('records')
            else:
                raw = f.read()
                for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
                    try:
                        text = raw.decode(enc)
                        break
                    except:
                        continue
                rows = list(csv.DictReader(io.StringIO(text)))
            if upload.form_type == 'checklist':
                for row in rows:
                    uname = str(row.get('Assign To', '')).strip()
                    atou = User.objects.filter(username=uname).first()
                    if not atou:
                        continue
                    pl = row.get('Planned Date', '')
                    if isinstance(pl, pd.Timestamp):
                        planned_date = pl.to_pydatetime()
                    else:
                        raw_pd = str(pl).strip()
                        planned_date = parse_datetime(raw_pd)
                        if not planned_date and raw_pd:
                            parts = raw_pd.split()
                            d = parse_date(parts[0])
                            t = parse_time(parts[1]) if len(parts) > 1 else None
                            if d:
                                planned_date = (
                                    datetime.combine(d, t)
                                    if t else
                                    datetime.combine(d, datetime.min.time())
                                )
                    if not planned_date:
                        planned_date = timezone.now()
                    if timezone.is_naive(planned_date):
                        planned_date = timezone.make_aware(
                            planned_date, timezone.get_current_timezone()
                        )
                    time_per = parse_int(
                        row.get('Time per Task (minutes)', 0)
                    )
                    rst_time = parse_time_val(row.get('Reminder Starting Time'))
                    chk = Checklist(
                        assign_by=request.user,
                        task_name=str(row.get('Task Name', '')).strip(),
                        message=str(row.get('Message', '')).strip(),
                        assign_to=atou,
                        planned_date=planned_date,
                        priority=str(row.get('Priority', 'Low')).strip(),
                        attachment_mandatory=str(
                            row.get('Make Attachment Mandatory', '')
                        ).strip().lower() in ['yes','true','1'],
                        mode=str(row.get('Mode', 'Daily')).strip(),
                        frequency=parse_int(row.get('Frequency', '1')),
                        time_per_task_minutes=time_per,
                        remind_before_days=parse_int(
                            row.get('Reminder Before Days', '0')
                        ),
                        assign_pc=User.objects.filter(
                            username=str(row.get('Assign PC', '')).strip()
                        ).first(),
                        notify_to=User.objects.filter(
                            username=str(row.get('Notify To', '')).strip()
                        ).first(),
                        set_reminder=str(
                            row.get('Set Reminder', '')
                        ).strip().lower() in ['yes','true','1'],
                        reminder_mode=str(row.get('Reminder Mode', '')).strip(),
                        reminder_frequency=parse_int(
                            row.get('Reminder Frequency', '1')
                        ),
                        reminder_before_days=parse_int(
                            row.get('Reminder Before Days', '0')
                        ),
                        reminder_starting_time=rst_time,
                        checklist_auto_close=str(
                            row.get('Checklist Auto Close', '')
                        ).strip().lower() in ['yes','true','1'],
                        checklist_auto_close_days=parse_int(
                            row.get('Checklist Auto Close Days', '0')
                        ),
                        actual_duration_minutes=0
                    )
                    chk.save()
            else:
                for row in rows:
                    uname = str(row.get('Assign To','')).strip()
                    atou = User.objects.filter(username=uname).first()
                    if not atou:
                        continue
                    pd_val = row.get('Planned Date','')
                    if isinstance(pd_val, pd.Timestamp):
                        pdate = pd_val.date()
                    else:
                        raw_pd = str(pd_val).strip()
                        pdate = parse_date(raw_pd)
                        if not pdate and raw_pd:
                            try:
                                pdate = datetime.strptime(
                                    raw_pd, '%m/%d/%Y'
                                ).date()
                            except:
                                pdate = None
                    if not pdate:
                        continue
                    mode_val = str(row.get('Mode','Daily')).strip()
                    freq_val = parse_int(row.get('Frequency','1'))
                    time_per = parse_int(
                        row.get('Time per Task (minutes)', 0)
                    )
                    deg = Delegation(
                        assign_by=request.user,
                        task_name=str(row.get('Task Name','')).strip(),
                        assign_to=atou,
                        planned_date=pdate,
                        priority=str(row.get('Priority','Low')).strip(),
                        attachment_mandatory=str(
                            row.get('Make Attachment Mandatory','')
                        ).strip().lower() in ['yes','true','1'],
                        time_per_task_minutes=time_per,
                        mode=mode_val,
                        frequency=freq_val,
                        actual_duration_minutes=0
                    )
                    deg.save()
            messages.success(
                request,
                "Your file has been uploaded and processed successfully."
            )
            return redirect('tasks:bulk_upload')
        else:
            messages.error(
                request,
                "Upload failed: please check the file and try again."
            )
    else:
        form = BulkUploadForm()
    return render(request, 'tasks/bulk_upload.html', {'form': form})

@has_permission('bulk_upload')
def download_checklist_template(request):
    path = finders.find('bulk_upload_templates/checklist_template.csv')
    if not path:
        raise Http404
    return FileResponse(
        open(path, 'rb'),
        as_attachment=True,
        filename='checklist_template.csv'
    )

@has_permission('bulk_upload')
def download_delegation_template(request):
    path = finders.find('bulk_upload_templates/delegation_template.csv')
    if not path:
        raise Http404
    return FileResponse(
        open(path, 'rb'),
        as_attachment=True,
        filename='delegation_template.csv'
    )

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
    items = HelpTicket.objects.filter(
        assign_to=request.user
    ).exclude(status='Closed').order_by('-planned_date')
    return render(request, 'tasks/list_help_ticket_assigned_to.html', {
        'items': items,
        'current_tab': 'assigned_to',
    })

@login_required
def assigned_by_me(request):
    items = HelpTicket.objects.filter(
        assign_by=request.user
    ).order_by('-planned_date')
    return render(request, 'tasks/list_help_ticket_assigned_by.html', {
        'items': items,
        'current_tab': 'assigned_by',
    })

@login_required
def add_help_ticket(request):
    if request.method == 'POST':
        form = HelpTicketForm(request.POST, request.FILES)
        if form.is_valid():
            ticket = form.save(commit=False)
            ticket.assign_by = request.user
            ticket.save()
            if ticket.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
                subject = f"New Help Ticket Assigned: {ticket.title}"
                html_message = render_to_string(
                    'email/help_ticket_assigned.html',
                    {
                        'ticket': ticket,
                        'assign_by': ticket.assign_by,
                        'assign_to': ticket.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [ticket.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
            return redirect('tasks:list_help_ticket')
    else:
        form = HelpTicketForm()
    return render(request, 'tasks/add_help_ticket.html', {
        'form': form,
        'current_tab': 'add',
        'can_create': can_create(request.user)
    })

@login_required
def edit_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    if request.method == 'POST':
        form = HelpTicketForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            ticket = form.save()
            if ticket.assign_to.email:
                complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
                subject = f"Help Ticket Updated: {ticket.title}"
                html_message = render_to_string(
                    'email/help_ticket_assigned.html',
                    {
                        'ticket': ticket,
                        'assign_by': ticket.assign_by,
                        'assign_to': ticket.assign_to,
                        'complete_url': complete_url,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    [ticket.assign_to.email]
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
            return redirect('tasks:list_help_ticket')
    else:
        form = HelpTicketForm(instance=obj)
    return render(request, 'tasks/add_help_ticket.html', {
        'form': form,
        'current_tab': 'edit',
        'can_create': can_create(request.user)
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
                subject = f"Help Ticket Closed: {ticket.title}"
                html_message = render_to_string(
                    'email/help_ticket_closed.html',
                    {
                        'ticket': ticket,
                        'assign_by': ticket.assign_by,
                        'assign_to': ticket.assign_to,
                    }
                )
                msg = EmailMultiAlternatives(
                    subject,
                    html_message,
                    None,
                    recipients
                )
                msg.attach_alternative(html_message, "text/html")
                msg.send(fail_silently=True)
        messages.success(request, f"Note saved for HT-{ticket.id}.")
        return redirect(request.GET.get('next', reverse('tasks:assigned_to_me')))
    return render(request, 'tasks/note_help_ticket.html', {
        'ticket': ticket,
        'next':    request.GET.get('next', reverse('tasks:assigned_to_me'))
    })

@login_required
def delete_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_help_ticket')
    return render(request, 'tasks/confirm_delete.html', {
        'object': obj, 'type': 'Help Ticket'
    })
