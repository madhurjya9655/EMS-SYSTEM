# apps/tasks/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth import get_user_model
from .models import Checklist, Delegation, BulkUpload, FMS, HelpTicket
from .forms import ChecklistForm, DelegationForm, BulkUploadForm
import csv, io

User = get_user_model()

# allow superusers, Admins, Managers, EAs, or CEOs to create/edit master‐tasks
can_create = lambda u: (
    u.is_superuser
    or u.groups.filter(name__in=['Admin', 'Manager', 'EA', 'CEO']).exists()
)

@login_required
def list_checklist(request):
    if request.method == 'POST':
        ids = request.POST.getlist('sel')
        if ids:
            Checklist.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_checklist')
    items = Checklist.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_checklist.html', {'items': items})

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
    return render(request, 'tasks/confirm_delete.html', {
        'object': obj,
        'type': 'Checklist'
    })

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
    return render(request, 'tasks/reassign_checklist.html', {
        'object': obj,
        'all_users': users
    })

@login_required
def list_delegation(request):
    if request.method == 'POST':
        ids = request.POST.getlist('sel')
        if ids:
            Delegation.objects.filter(pk__in=ids).delete()
        return redirect('tasks:list_delegation')
    items = Delegation.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_delegation.html', {'items': items})

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
    return render(request, 'tasks/confirm_delete.html', {
        'object': obj,
        'type': 'Delegation'
    })

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
    return render(request, 'tasks/reassign_delegation.html', {
        'object': obj,
        'all_users': users
    })

@login_required
@user_passes_test(can_create)
def bulk_upload(request):
    if request.method == 'POST':
        form = BulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            raw = upload.csv_file.read()
            # try a few common encodings
            for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue

            reader = csv.DictReader(io.StringIO(text))

            # skip malformed lines
            def safe_rows(r):
                while True:
                    try:
                        yield next(r)
                    except StopIteration:
                        return
                    except csv.Error:
                        continue

            if upload.form_type == 'checklist':
                for row in safe_rows(reader):
                    username = row.get('Assign To', '').strip()
                    try:
                        atou = User.objects.get(username=username)
                    except User.DoesNotExist:
                        continue
                    Checklist(
                        assign_by=request.user,
                        task_name=row.get('Task Name', '').strip(),
                        assign_to=atou,
                        planned_date=row.get('Planned Date', '').strip(),
                        priority=row.get('Priority', 'Low').strip(),
                        attachment_mandatory=row.get('Make Attachment Mandatory', '').strip().lower() in ['yes','true','1'],
                        mode=row.get('Mode', 'Daily').strip(),
                        frequency=int(row.get('Frequency', '1').strip() or '1'),
                        remind_before_days=int(row.get('Reminder Before Days', '0').strip() or '0'),
                        message=row.get('Message', '').strip(),
                        assign_pc=User.objects.get(username=row.get('Assign PC','').strip()) if row.get('Assign PC') else None,
                        notify_to=User.objects.get(username=row.get('Notify To','').strip()) if row.get('Notify To') else None,
                        set_reminder=row.get('Set Reminder', '').strip().lower() in ['yes','true','1'],
                        reminder_mode=row.get('Reminder Mode','').strip(),
                        reminder_frequency=int(row.get('Reminder Frequency','1').strip() or '1'),
                        reminder_before_days=int(row.get('Reminder Before Days','0').strip() or '0'),
                        reminder_starting_time=row.get('Reminder Starting Time','') or None,
                        checklist_auto_close=row.get('Checklist Auto Close','').strip().lower() in ['yes','true','1'],
                        checklist_auto_close_days=int(row.get('Checklist Auto Close Days','0').strip() or '0'),
                        estimated_minutes=0
                    ).save()
            else:
                for row in safe_rows(reader):
                    username = row.get('Assign To', '').strip()
                    try:
                        atou = User.objects.get(username=username)
                    except User.DoesNotExist:
                        continue
                    Delegation(
                        assign_by=request.user,
                        task_name=row.get('Task Name','').strip(),
                        assign_to=atou,
                        planned_date=row.get('Planned Date','').strip(),
                        priority=row.get('Priority','Low').strip(),
                        attachment_mandatory=row.get('Make Attachment Mandatory','').strip().lower() in ['yes','true','1'],
                        estimated_minutes=0
                    ).save()

            return redirect('tasks:bulk_upload')
    else:
        form = BulkUploadForm()
    return render(request, 'tasks/bulk_upload.html', {'form': form})

@login_required
def list_fms(request):
    items = FMS.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_fms.html', {'items': items})

@login_required
def list_help_ticket(request):
    items = HelpTicket.objects.select_related('assign_by', 'assign_to').order_by('-planned_date')
    if not (request.user.is_staff or request.user.is_superuser):
        items = items.filter(assign_to=request.user)
    return render(request, 'tasks/list_help_ticket.html', {'items': items})
