from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth import get_user_model

from .models import PettyCashRequest
from .forms  import PettyCashForm

User = get_user_model()

# only EA members (or superusers) can apply
def is_ea(user):
    return user.is_superuser or user.groups.filter(name='EA').exists()

# only Managers (or superusers) can do manager approvals
def is_manager(user):
    return user.is_superuser or user.groups.filter(name='Manager').exists()

# only Finance (or superusers) can do finance approvals
def is_finance(user):
    return user.is_superuser or user.groups.filter(name='Finance').exists()


@login_required
def list_requests(request):
    """
    Show this user's own petty-cash requests.
    """
    qs = PettyCashRequest.objects.filter(requester=request.user).order_by('-created_at')
    return render(request, 'petty_cash/list_requests.html', {
        'requests': qs
    })


@login_required
@user_passes_test(is_ea)
def apply_request(request):
    """
    EA users can submit a new petty-cash request.
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


@login_required
@user_passes_test(is_manager)
def manager_requests(request):
    """
    Managers see all requests pending their approval.
    """
    qs = PettyCashRequest.objects.filter(status='Pending Manager').order_by('-created_at')
    return render(request, 'petty_cash/manager_requests.html', {
        'requests': qs
    })


@login_required
@user_passes_test(is_manager)
def manager_detail(request, pk):
    """
    Manager can approve/reject and add a comment.
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


@login_required
@user_passes_test(is_finance)
def finance_requests(request):
    """
    Finance team sees all requests pending their approval.
    """
    qs = PettyCashRequest.objects.filter(status='Pending Finance').order_by('-created_at')
    return render(request, 'petty_cash/finance_requests.html', {
        'requests': qs
    })


@login_required
@user_passes_test(is_finance)
def finance_detail(request, pk):
    """
    Finance can approve/reject and add a comment.
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
