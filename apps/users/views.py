from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib.auth.views import LoginView
from django.urls import reverse_lazy
from .models import Profile
from .forms import UserForm, ProfileForm, CustomAuthForm
from .permissions import PERMISSIONS_STRUCTURE
from django.contrib import messages

def admin_only(user):
    return user.is_superuser

class CustomLoginView(LoginView):
    template_name = 'registration/login.html'
    authentication_form = CustomAuthForm
    redirect_authenticated_user = True

    def form_valid(self, form):
        remember = self.request.POST.get('remember')
        if remember:
            self.request.session.set_expiry(1209600)  # 2 weeks
        else:
            self.request.session.set_expiry(0)  # Browser close
        return super().form_valid(form)

    def get_success_url(self):
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
    return render(request, 'users/add_user.html', {
        'uf': uf,
        'pf': pf,
        'permissions_structure': PERMISSIONS_STRUCTURE,
    })

@login_required
@user_passes_test(admin_only)
def edit_user(request, pk):
    user_obj = get_object_or_404(User, pk=pk)
    # NOTE: don't create a Profile on GET — it will violate the unique phone constraint
    profile_obj = Profile.objects.filter(user=user_obj).first()

    if request.method == 'POST':
        uf = UserForm(request.POST, instance=user_obj)
        pf = ProfileForm(request.POST, instance=profile_obj)  # instance may be None (will create on save)
        if uf.is_valid() and pf.is_valid():
            # Save user (handle optional password change)
            user = uf.save(commit=False)
            pwd = uf.cleaned_data['password']
            if pwd:
                user.set_password(pwd)
            user.save()

            # Save/create profile with a valid phone from the form
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
        'permissions_structure': PERMISSIONS_STRUCTURE,
    })

@login_required
@user_passes_test(admin_only)
def delete_user(request, pk):
    try:
        user = User.objects.get(pk=pk)
    except User.DoesNotExist:
        messages.error(request, "No user matches the given query.")
        return render(request, "users/user_not_found.html", status=404)

    if request.method == "POST":
        if user == request.user:
            messages.error(request, "You cannot delete your own account!")
            return redirect("users:list_users")
        user.delete()
        messages.success(request, "User deleted successfully.")
        return redirect('users:list_users')

    # Show confirm page if GET
    return render(request, "users/confirm_delete.html", {"user": user})

@login_required
@user_passes_test(admin_only)
def toggle_active(request, pk):
    u = get_object_or_404(User, pk=pk)
    u.is_active = not u.is_active
    u.save()
    return redirect('users:list_users')
