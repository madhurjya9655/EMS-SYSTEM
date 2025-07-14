from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import get_user_model
from apps.users.decorators import has_permission

from .models import PettyCashRequest
from .forms  import PettyCashForm

User = get_user_model()

@has_permission('pettycash_list')
def list_requests(request):
    """
    Show this user's own petty-cash requests.
    """
    qs = PettyCashRequest.objects.filter(requester=request.user).order_by('-created_at')
    return render(request, 'petty_cash/list_requests.html', {
        'requests': qs
    })

@has_permission('pettycash_apply')
def apply_request(request):
    """
    Users with the 'Petty-Cash Apply' privilege can submit a new petty-cash request.
    """
    if request.method == 'POST':
        form = PettyCashForm(request.POST, request.FILES)
        if form.is_valid():
            pr = form.save(commit=False)
            pr.requester = request.user
            pr.save()
            return redirect('petty_cash:list_requests')
    else:
        form = PettyCashForm()

    return render(request, 'petty_cash/apply_request.html', {
        'form': form
    })

@has_permission('pettycash_list')
def manager_requests(request):
    """
    Users with the 'Petty-Cash List' privilege see all requests pending their approval.
    """
    qs = PettyCashRequest.objects.filter(status='Pending Manager').order_by('-created_at')
    return render(request, 'petty_cash/manager_requests.html', {
        'requests': qs
    })

@has_permission('pettycash_list')
def manager_detail(request, pk):
    """
    Approve/reject and add a comment to a request (manager view).
    """
    pr = get_object_or_404(PettyCashRequest, pk=pk)
    if request.method == 'POST':
        action = request.POST.get('action')             # "approve" or "reject"
        comment = request.POST.get('manager_comment', '')
        pr.manager_comment = comment
        pr.status = 'Pending Finance' if action == 'approve' else 'Rejected'
        pr.save()
        return redirect('petty_cash:manager_requests')

    return render(request, 'petty_cash/manager_detail.html', {
        'request_obj': pr
    })

@has_permission('pettycash_list')
def finance_requests(request):
    """
    Users with the 'Petty-Cash List' privilege see all requests pending finance approval.
    """
    qs = PettyCashRequest.objects.filter(status='Pending Finance').order_by('-created_at')
    return render(request, 'petty_cash/finance_requests.html', {
        'requests': qs
    })

@has_permission('pettycash_list')
def finance_detail(request, pk):
    """
    Approve/reject and add a comment to a request (finance view).
    """
    pr = get_object_or_404(PettyCashRequest, pk=pk)
    if request.method == 'POST':
        action = request.POST.get('action')              # "approve" or "reject"
        comment = request.POST.get('finance_comment', '')
        pr.finance_comment = comment
        pr.status = 'Approved' if action == 'approve' else 'Rejected'
        pr.save()
        return redirect('petty_cash:finance_requests')

    return render(request, 'petty_cash/finance_detail.html', {
        'request_obj': pr
    })
