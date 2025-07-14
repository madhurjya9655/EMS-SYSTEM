from django.shortcuts import render, redirect, get_object_or_404
from apps.users.decorators import has_permission
from .models import LeaveRequest
from .forms import LeaveRequestForm

@has_permission('leave_apply')
def apply_leave(request):
    if request.method == 'POST':
        form = LeaveRequestForm(request.POST)
        if form.is_valid():
            lr = form.save(commit=False)
            lr.employee = request.user
            lr.status = 'Pending Manager'
            lr.save()
            return redirect('leave:my_leaves')
    else:
        form = LeaveRequestForm()
    return render(request, 'leave/apply_leave.html', {'form': form})

@has_permission('leave_list')
def my_leaves(request):
    leaves = LeaveRequest.objects.filter(employee=request.user)
    return render(request, 'leave/my_leaves.html', {'leaves': leaves})

@has_permission('leave_pending_manager')
def pending_leaves(request):
    leaves = LeaveRequest.objects.filter(status='Pending Manager')
    if request.method == 'POST':
        lr = get_object_or_404(LeaveRequest, pk=request.POST['id'])
        lr.status = 'Pending HR' if 'approve' in request.POST else 'Rejected'
        lr.save()
        return redirect('leave:pending_leaves')
    return render(request, 'leave/pending_leaves.html', {'leaves': leaves})

@has_permission('leave_pending_hr')
def hr_leaves(request):
    leaves = LeaveRequest.objects.filter(status='Pending HR')
    if request.method == 'POST':
        lr = get_object_or_404(LeaveRequest, pk=request.POST['id'])
        lr.status = 'Approved'
        lr.save()
        return redirect('leave:hr_leaves')
    return render(request, 'leave/hr_leaves.html', {'leaves': leaves})
