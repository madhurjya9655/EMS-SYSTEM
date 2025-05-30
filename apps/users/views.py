# apps/users/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from .models import Profile
from .forms import UserForm, ProfileForm

def admin_only(u):
    return u.is_superuser

@login_required
@user_passes_test(admin_only)
def list_users(request):
    users = User.objects.order_by('first_name')
    return render(request, 'users/list_user.html', {'users': users})

@login_required
@user_passes_test(admin_only)
def add_user(request):
    if request.method == 'POST':
        uf = UserForm(request.POST)
        pf = ProfileForm(request.POST)
        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)
            user.set_password(uf.cleaned_data['password'])
            user.is_staff = True
            user.save()
            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data['permissions']
            profile.save()
            return redirect('users:user_list')    # ← namespaced
    else:
        uf = UserForm()
        pf = ProfileForm()
    return render(request, 'users/add_user.html', {'uf': uf, 'pf': pf})

@login_required
@user_passes_test(admin_only)
def delete_user(request, pk):
    u = get_object_or_404(User, pk=pk)
    u.delete()
    return redirect('users:user_list')            # ← namespaced
