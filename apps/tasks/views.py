# apps/tasks/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth import get_user_model
from django.http import HttpResponse, FileResponse, Http404
from django.contrib.staticfiles import finders
from django.utils import timezone
from django.utils.dateparse import parse_datetime, parse_date, parse_time
from django.db.models import Q, Min
from .models import Checklist, Delegation, BulkUpload, FMS, HelpTicket
from .forms import ChecklistForm, DelegationForm, BulkUploadForm
import csv, io, pandas as pd, math, datetime

User = get_user_model()
can_create = lambda u: u.is_superuser or u.groups.filter(name__in=['Admin','Manager','EA','CEO']).exists()

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
    if isinstance(val, datetime.time):
        return val
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime().time()
    s = str(val).strip()
    return parse_time(s) if s else None

@login_required
def list_checklist(request):
    if request.method == 'POST':
        ids = request.POST.getlist('sel')
        if ids:
            Checklist.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_checklist')
    qs = Checklist.objects.all()
    keyword = request.GET.get('keyword', '').strip()
    if keyword:
        qs = qs.filter(Q(task_name__icontains=keyword) | Q(message__icontains=keyword))
    assign_to = request.GET.get('assign_to', '')
    if assign_to:
        qs = qs.filter(assign_to_id=assign_to)
    priority = request.GET.get('priority', '')
    if priority:
        qs = qs.filter(priority=priority)
    group_name = request.GET.get('group_name', '').strip()
    if group_name:
        qs = qs.filter(group_name__icontains=group_name)
    start_date = request.GET.get('start_date', '')
    if start_date:
        qs = qs.filter(planned_date__date__gte=start_date)
    end_date = request.GET.get('end_date', '')
    if end_date:
        qs = qs.filter(planned_date__date__lte=end_date)
    ordered = qs.order_by('-planned_date')
    if request.GET.get('download'):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="checklist.csv"'
        writer = csv.writer(response)
        writer.writerow(['Task Name','Assign To','Planned Date','Priority','Group Name','Status'])
        for item in ordered:
            writer.writerow([
                item.task_name,
                item.assign_to.get_full_name() or item.assign_to.username,
                item.planned_date.strftime('%Y-%m-%d %H:%M'),
                item.priority,
                item.group_name,
                item.status,
            ])
        return response
    grouped = ordered.values('task_name','assign_to_id').annotate(first_id=Min('id')).values_list('first_id', flat=True)
    items = Checklist.objects.filter(id__in=grouped).order_by('-planned_date')
    users = User.objects.order_by('username')
    priority_choices = Checklist._meta.get_field('priority').choices
    group_names = Checklist.objects.order_by('group_name').values_list('group_name', flat=True).distinct()
    ctx = {
        'items': items,
        'users': users,
        'priority_choices': priority_choices,
        'group_names': group_names,
        'current_tab': 'checklist',
    }
    if request.GET.get('partial'):
        return render(request, 'tasks/partial_list_checklist.html', ctx)
    return render(request, 'tasks/list_checklist.html', ctx)

@login_required
@user_passes_test(can_create)
def add_checklist(request):
    if request.method == 'POST':
        form = ChecklistForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(initial={'assign_by': request.user})
    return render(request, 'tasks/add_checklist.html', {'form': form})

@login_required
@user_passes_test(can_create)
def edit_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        form = ChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            form.save()
            return redirect('tasks:list_checklist')
    else:
        form = ChecklistForm(instance=obj)
    return render(request, 'tasks/add_checklist.html', {'form': form})

@login_required
@user_passes_test(can_create)
def delete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_checklist')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Checklist'})

@login_required
@user_passes_test(can_create)
def reassign_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        uid = request.POST.get('assign_to')
        if uid:
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()
            return redirect('tasks:list_checklist')
    users = User.objects.order_by('username')
    return render(request, 'tasks/reassign_checklist.html', {'object': obj, 'all_users': users})

@login_required
def list_delegation(request):
    if request.method == 'POST':
        ids = request.POST.getlist('sel')
        if ids:
            Delegation.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_delegation')
    items = Delegation.objects.all().order_by('-planned_date')
    ctx = {'items': items, 'current_tab': 'delegation'}
    if request.GET.get('partial'):
        return render(request, 'tasks/partial_list_delegation.html', ctx)
    return render(request, 'tasks/list_delegation.html', ctx)

@login_required
@user_passes_test(can_create)
def add_delegation(request):
    if request.method == 'POST':
        form = DelegationForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('tasks:list_delegation')
    else:
        form = DelegationForm(initial={'assign_by': request.user})
    return render(request, 'tasks/add_delegation.html', {'form': form})

@login_required
@user_passes_test(can_create)
def edit_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        form = DelegationForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            form.save()
            return redirect('tasks:list_delegation')
    else:
        form = DelegationForm(instance=obj)
    return render(request, 'tasks/add_delegation.html', {'form': form})

@login_required
@user_passes_test(can_create)
def delete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        obj.delete()
        return redirect('tasks:list_delegation')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Delegation'})

@login_required
@user_passes_test(can_create)
def reassign_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        uid = request.POST.get('assign_to')
        if uid:
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()
            return redirect('tasks:list_delegation')
    users = User.objects.order_by('username')
    return render(request, 'tasks/reassign_delegation.html', {'object': obj, 'all_users': users})

@login_required
@user_passes_test(can_create)
def bulk_upload(request):
    if request.method == 'POST':
        form = BulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            f = upload.csv_file
            ext = f.name.rsplit('.',1)[-1].lower()
            if ext in ('xls','xlsx'):
                xl = pd.read_excel(f, sheet_name=None)
                sheet = 'Checklist Upload' if upload.form_type=='checklist' else 'Delegation Upload'
                rows = xl.get(sheet, next(iter(xl.values()))).to_dict('records')
            else:
                raw = f.read()
                for enc in ('utf-8-sig','utf-8','latin-1'):
                    try: text = raw.decode(enc); break
                    except: continue
                rows = list(csv.DictReader(io.StringIO(text)))
            if upload.form_type=='checklist':
                for row in rows:
                    uname = str(row.get('Assign To','')).strip()
                    atou = User.objects.filter(username=uname).first()
                    if not atou:
                        continue
                    pl = row.get('Planned Date','')
                    if isinstance(pl, pd.Timestamp):
                        planned_date = pl.to_pydatetime()
                    else:
                        planned_date = parse_datetime(str(pl).strip())
                    if planned_date and timezone.is_naive(planned_date):
                        planned_date = timezone.make_aware(planned_date, timezone.get_current_timezone())
                    time_per = parse_int(row.get('Time per Task (minutes)',0))
                    rst = row.get('Reminder Starting Time')
                    rst_time = parse_time_val(rst)
                    chk = Checklist(
                        assign_by=request.user,
                        task_name=str(row.get('Task Name','')).strip(),
                        message=str(row.get('Message','')).strip(),
                        assign_to=atou,
                        planned_date=planned_date,
                        priority=str(row.get('Priority','Low')).strip(),
                        attachment_mandatory=str(row.get('Make Attachment Mandatory','')).strip().lower() in ['yes','true','1'],
                        mode=str(row.get('Mode','Daily')).strip(),
                        frequency=parse_int(row.get('Frequency','1')),
                        time_per_task_minutes=time_per,
                        remind_before_days=parse_int(row.get('Reminder Before Days','0')),
                        assign_pc=User.objects.filter(username=str(row.get('Assign PC','')).strip()).first(),
                        notify_to=User.objects.filter(username=str(row.get('Notify To','')).strip()).first(),
                        set_reminder=str(row.get('Set Reminder','')).strip().lower() in ['yes','true','1'],
                        reminder_mode=str(row.get('Reminder Mode','')).strip(),
                        reminder_frequency=parse_int(row.get('Reminder Frequency','1')),
                        reminder_before_days=parse_int(row.get('Reminder Before Days','0')),
                        reminder_starting_time=rst_time,
                        checklist_auto_close=str(row.get('Checklist Auto Close','')).strip().lower() in ['yes','true','1'],
                        checklist_auto_close_days=parse_int(row.get('Checklist Auto Close Days','0')),
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
                        pdate = parse_date(str(pd_val).strip())
                    time_per = parse_int(row.get('Time per Task (minutes)',0))
                    deg = Delegation(
                        assign_by=request.user,
                        task_name=str(row.get('Task Name','')).strip(),
                        assign_to=atou,
                        planned_date=pdate,
                        priority=str(row.get('Priority','Low')).strip(),
                        attachment_mandatory=str(row.get('Make Attachment Mandatory','')).strip().lower() in ['yes','true','1'],
                        time_per_task_minutes=time_per
                    )
                    deg.save()
            return redirect('tasks:bulk_upload')
    else:
        form = BulkUploadForm()
    return render(request, 'tasks/bulk_upload.html', {'form': form})

@login_required
@user_passes_test(can_create)
def download_checklist_template(request):
    path = finders.find('bulk_upload_templates/checklist_template.csv')
    if not path:
        raise Http404
    return FileResponse(open(path, 'rb'), as_attachment=True, filename='checklist_template.csv')

@login_required
@user_passes_test(can_create)
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
    items = HelpTicket.objects.select_related('assign_by','assign_to').order_by('-planned_date')
    if not (request.user.is_staff or request.user.is_superuser):
        items = items.filter(assign_to=request.user)
    ctx = {'items': items, 'current_tab': 'help_ticket'}
    if request.GET.get('partial'):
        return render(request, 'tasks/partial_list_help_ticket.html', ctx)
    return render(request, 'tasks/list_help_ticket.html', ctx)

@login_required
def complete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk, assign_to=request.user)
    now = timezone.now()
    obj.status = 'Completed'
    obj.completed_at = now
    minutes = int((now - obj.planned_date).total_seconds() // 60)
    obj.actual_duration_minutes = minutes if minutes >= 0 else 0
    obj.save()
    next_url = request.GET.get('next') or 'dashboard:home'
    return redirect(next_url)
