from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib.auth.views import LoginView
from django.urls import reverse_lazy
from .models import Profile
from .forms import UserForm, ProfileForm, CustomAuthForm

def admin_only(user):
    return user.is_superuser

class CustomLoginView(LoginView):
    template_name = 'registration/login.html'
    authentication_form = CustomAuthForm
    redirect_authenticated_user = True

    def form_valid(self, form):
        remember = self.request.POST.get('remember')
        if remember:
            # 2 weeks
            self.request.session.set_expiry(1209600)
        else:
            # Browser close
            self.request.session.set_expiry(0)
        return super().form_valid(form)

    def get_success_url(self):
        # Always redirect to dashboard after login
        return reverse_lazy('dashboard:home')

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
            user.is_staff = False
            user.is_active = True
            user.save()
            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data['permissions']
            profile.save()
            return redirect('users:list_users')
    else:
        uf = UserForm()
        pf = ProfileForm()
    return render(request, 'users/add_user.html', {'uf': uf, 'pf': pf})

@login_required
@user_passes_test(admin_only)
def edit_user(request, pk):
    user_obj = get_object_or_404(User, pk=pk)
    profile_obj, _ = Profile.objects.get_or_create(user=user_obj)
    if request.method == 'POST':
        uf = UserForm(request.POST, instance=user_obj)
        pf = ProfileForm(request.POST, instance=profile_obj)
        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)
            pwd = uf.cleaned_data['password']
            if pwd:
                user.set_password(pwd)
            user.save()
            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data['permissions']
            profile.save()
            return redirect('users:list_users')
    else:
        uf = UserForm(instance=user_obj, initial={'password': ''})
        pf = ProfileForm(instance=profile_obj)
    return render(request, 'users/edit_user.html', {
        'uf': uf,
        'pf': pf,
        'user_obj': user_obj,
    })

@login_required
@user_passes_test(admin_only)
def delete_user(request, pk):
    u = get_object_or_404(User, pk=pk)
    u.delete()
    return redirect('users:list_users')

@login_required
@user_passes_test(admin_only)
def toggle_active(request, pk):
    u = get_object_or_404(User, pk=pk)
    u.is_active = not u.is_active
    u.save()
    return redirect('users:list_users')

def custom_permission_denied_view(request, exception=None):
    return render(request, 'users/no_permission.html', status=403)
