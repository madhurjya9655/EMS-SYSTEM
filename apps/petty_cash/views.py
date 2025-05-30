from django.shortcuts import render
from django.contrib.auth.decorators import login_required, user_passes_test
from .models import PettyCashRequest

@login_required
def my_requests(request):
    qs = PettyCashRequest.objects.filter(requester=request.user)
    return render(request, 'petty_cash/my_requests.html', {'items': qs})

@login_required
@user_passes_test(lambda u: u.groups.filter(name='Manager').exists())
def manager_requests(request):
    qs = PettyCashRequest.objects.filter(status='Pending Manager')
    return render(request, 'petty_cash/manager_requests.html', {'items': qs})

@login_required
@user_passes_test(lambda u: u.groups.filter(name='Finance').exists())
def finance_requests(request):
    qs = PettyCashRequest.objects.filter(status='Approved by Manager')
    return render(request, 'petty_cash/finance_requests.html', {'items': qs})
