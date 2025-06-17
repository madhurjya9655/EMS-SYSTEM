from django.shortcuts import render
from django.contrib.auth.decorators import login_required, user_passes_test
from .models import PettyCashRequest

manager_or_super = lambda u: u.is_superuser or u.groups.filter(name='Manager').exists()
finance_or_super = lambda u: u.is_superuser or u.groups.filter(name='Finance').exists()

@login_required
def my_requests(request):
    qs = PettyCashRequest.objects.filter(requester=request.user)
    return render(request, 'petty_cash/my_requests.html', {'items': qs})

@login_required
@user_passes_test(manager_or_super)
def manager_requests(request):
    qs = PettyCashRequest.objects.filter(status='Pending Manager')
    return render(request, 'petty_cash/manager_requests.html', {'items': qs})

@login_required
@user_passes_test(finance_or_super)
def finance_requests(request):
    qs = PettyCashRequest.objects.filter(status='Approved by Manager')
    return render(request, 'petty_cash/finance_requests.html', {'items': qs})
