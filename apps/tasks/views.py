from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from .models import Checklist, Delegation, BulkUpload
from .forms import ChecklistForm, DelegationForm, BulkUploadForm
import csv, io
from django.contrib.auth import get_user_model

User = get_user_model()

def can_create(u):
    return u.groups.filter(name__in=['Admin','Manager','EA to CEO']).exists()

@login_required
def list_checklist(request):
    items = Checklist.objects.all()
    return render(request,'tasks/list_checklist.html',{'items':items})

@login_required
@user_passes_test(can_create)
def add_checklist(request):
    if request.method=='POST':
        form = ChecklistForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('tasks_list_checklist')
    else:
        form = ChecklistForm(initial={'assign_by':request.user})
    return render(request,'tasks/add_checklist.html',{'form':form})

@login_required
def list_delegation(request):
    items = Delegation.objects.all()
    return render(request,'tasks/list_delegation.html',{'items':items})

@login_required
@user_passes_test(can_create)
def add_delegation(request):
    if request.method=='POST':
        form = DelegationForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('tasks_list_delegation')
    else:
        form = DelegationForm(initial={'assign_by':request.user})
    return render(request,'tasks/add_delegation.html',{'form':form})

@login_required
@user_passes_test(can_create)
def bulk_upload(request):
    if request.method=='POST':
        form = BulkUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            data = upload.csv_file.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(data))
            if upload.form_type=='checklist':
                for row in reader:
                    Checklist.objects.create(
                        assign_by=request.user,
                        task_name=row['Task Name'],
                        assign_to=User.objects.get(username=row['Assign To']),
                        planned_date=row['Planned Date'],
                        priority=row['Priority']
                    )
            else:
                for row in reader:
                    Delegation.objects.create(
                        assign_by=request.user,
                        task_name=row['Task Name'],
                        assign_to=User.objects.get(username=row['Assign To']),
                        planned_date=row['Planned Date'],
                        priority=row['Priority']
                    )
            messages.success(request,'Upload successful')
            return redirect('tasks_bulk_upload')
    else:
        form = BulkUploadForm()
    return render(request,'tasks/bulk_upload.html',{'form':form})
