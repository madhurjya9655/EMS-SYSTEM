from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from .models import Checklist, Delegation, BulkUpload, FMS
from .forms import ChecklistForm, DelegationForm, BulkUploadForm
from django.contrib.auth import get_user_model
import csv, io

User = get_user_model()

def can_create(u):
    return u.groups.filter(name__in=['Admin', 'Manager', 'EA to CEO']).exists()

# ─── LIST + BULK DELETE ────────────────────────────────────────────────────────
@login_required
def list_checklist(request):
    if request.method == 'POST':
        # Bulk‐delete logic:
        selected_ids = request.POST.getlist('sel')
        if selected_ids:
            Checklist.objects.filter(pk__in=selected_ids).delete()
            messages.success(request, f"Deleted {len(selected_ids)} checklist(s).")
        return redirect('tasks:list_checklist')

    items = Checklist.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_checklist.html', {'items': items})


# ─── ADD, EDIT, DELETE, REASSIGN for Checklist ─────────────────────────────────
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
        messages.success(request, "Checklist deleted.")
        return redirect('tasks:list_checklist')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Checklist'})

@login_required
@user_passes_test(can_create)
def reassign_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == 'POST':
        new_user_id = request.POST.get('assign_to')
        if new_user_id:
            obj.assign_to = User.objects.get(pk=new_user_id)
            obj.save()
            messages.success(request, "Checklist reassigned.")
            return redirect('tasks:list_checklist')
    all_users = User.objects.all().order_by('username')
    return render(request, 'tasks/reassign_checklist.html', {'object': obj, 'all_users': all_users})


# ─── LIST + BULK DELETE for Delegation (optional) ───────────────────────────────
@login_required
def list_delegation(request):
    if request.method == 'POST':
        selected_ids = request.POST.getlist('sel')
        if selected_ids:
            Delegation.objects.filter(pk__in=selected_ids).delete()
            messages.success(request, f"Deleted {len(selected_ids)} delegation(s).")
        return redirect('tasks:list_delegation')

    items = Delegation.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_delegation.html', {'items': items})


# ─── ADD, EDIT, DELETE, REASSIGN for Delegation ─────────────────────────────────
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
        messages.success(request, "Delegation deleted.")
        return redirect('tasks:list_delegation')
    return render(request, 'tasks/confirm_delete.html', {'object': obj, 'type': 'Delegation'})

@login_required
@user_passes_test(can_create)
def reassign_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == 'POST':
        new_user_id = request.POST.get('assign_to')
        if new_user_id:
            obj.assign_to = User.objects.get(pk=new_user_id)
            obj.save()
            messages.success(request, "Delegation reassigned.")
            return redirect('tasks:list_delegation')
    all_users = User.objects.all().order_by('username')
    return render(request, 'tasks/reassign_delegation.html', {'object': obj, 'all_users': all_users})


# ─── BULK UPLOAD ───────────────────────────────────────────────────────────────
@login_required
@user_passes_test(can_create)
def bulk_upload(request):
    if request.method == 'POST':
        form = BulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            data = upload.csv_file.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(data))

            if upload.form_type == 'checklist':
                for row in reader:
                    obj = Checklist(
                        assign_by=request.user,
                        task_name=row.get('Task Name', '').strip(),
                        assign_to=User.objects.get(username=row.get('Assign To', '').strip()),
                        planned_date=row.get('Planned Date', '').strip(),
                        priority=row.get('Priority', 'Low').strip(),
                        attachment_mandatory=row.get('Make Attachment Mandatory', '').strip().lower() in ['yes', 'true', '1'],
                        mode=row.get('Mode', 'Daily').strip(),
                        frequency=int(row.get('Frequency', '1').strip() or '1'),
                        remind_before_days=int(row.get('Reminder Before Days','0').strip() or '0'),
                        message=row.get('Message','').strip(),
                        assign_pc=User.objects.get(username=row.get('Assign PC','').strip()) if row.get('Assign PC') else None,
                        notify_to=User.objects.get(username=row.get('Notify To','').strip()) if row.get('Notify To') else None,
                        set_reminder=row.get('Set Reminder','').strip().lower() in ['yes','true','1'],
                        reminder_mode=row.get('Reminder Mode','').strip() if row.get('Reminder Mode') else '',
                        reminder_frequency=int(row.get('Reminder Frequency','1').strip() or '1'),
                        reminder_before_days=int(row.get('Reminder Before Days','0').strip() or '0'),
                        reminder_starting_time=row.get('Reminder Starting Time','') or None,
                        checklist_auto_close=row.get('Checklist Auto Close','').strip().lower() in ['yes','true','1'],
                        checklist_auto_close_days=int(row.get('Checklist Auto Close Days','0').strip() or '0')
                    )
                    obj.save()
            else:
                for row in reader:
                    obj = Delegation(
                        assign_by=request.user,
                        task_name=row.get('Task Name', '').strip(),
                        assign_to=User.objects.get(username=row.get('Assign To', '').strip()),
                        planned_date=row.get('Planned Date','').strip(),
                        priority=row.get('Priority','Low').strip(),
                        attachment_mandatory=row.get('Make Attachment Mandatory','').strip().lower() in ['yes','true','1'],
                    )
                    obj.save()

            messages.success(request, 'Upload successful')
            return redirect('tasks:bulk_upload')
    else:
        form = BulkUploadForm()
    return render(request, 'tasks/bulk_upload.html', {'form': form})


# ─── PLACEHOLDER LISTS ────────────────────────────────────────────────────────
@login_required
def list_fms(request):
    items = FMS.objects.all().order_by('-planned_date')
    return render(request, 'tasks/list_fms.html', {'items': items})

@login_required
def list_help_ticket(request):
    items = []  # Replace with actual HelpTicket model
    return render(request, 'tasks/list_help_ticket.html', {'items': items})
