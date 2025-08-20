from __future__ import annotations

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib.auth.views import LoginView
from django.urls import reverse_lazy, reverse, NoReverseMatch
from django.contrib import messages

from .models import Profile
from .forms import UserForm, ProfileForm, CustomAuthForm
from .permissions import PERMISSIONS_STRUCTURE, _extract_perms
from .permission_urls import PERMISSION_URLS


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
        """
        After login:
        - Superuser -> dashboard
        - Else -> first URL the user actually has permission for (via PERMISSION_URLS)
        - Fallback -> dashboard
        """
        user = self.request.user
        if getattr(user, "is_superuser", False):
            return reverse_lazy('dashboard:home')

        user_codes = _extract_perms(user)  # set of permission codes from Profile.permissions
        # Iterate in the order defined in PERMISSION_URLS
        for code, url_name in PERMISSION_URLS.items():
            if code in user_codes:
                try:
                    return reverse(url_name)
                except NoReverseMatch:
                    # URL not present in this deployment; try the next one
                    continue

        return reverse_lazy('dashboard:home')


@login_required
@user_passes_test(admin_only)
def list_users(request):
    users = User.objects.order_by('first_name', 'last_name', 'username')
    return render(request, 'users/list_user.html', {'users': users})


@login_required
@user_passes_test(admin_only)
def add_user(request):
    if request.method == 'POST':
        uf = UserForm(request.POST)
        pf = ProfileForm(request.POST)
        if uf.is_valid() and pf.is_valid():
            # --- Create user ---
            user = uf.save(commit=False)
            raw_pwd = uf.cleaned_data.get('password') or ''
            user.set_password(raw_pwd)
            user.is_staff = False
            user.is_active = True
            user.save()

            # --- Create profile ---
            profile = pf.save(commit=False)
            profile.user = user
            # Ensure we store a list (JSONField) even if no permissions selected
            profile.permissions = pf.cleaned_data.get('permissions') or []
            profile.save()

            messages.success(request, "User created successfully.")
            return redirect('users:list_users')
        else:
            messages.error(request, "Please correct the errors below.")
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
    # Don't auto-create a Profile here; we'll create it only if form is valid
    profile_obj = Profile.objects.filter(user=user_obj).first()

    if request.method == 'POST':
        uf = UserForm(request.POST, instance=user_obj)
        pf = ProfileForm(request.POST, instance=profile_obj)  # instance may be None; save() will create
        if uf.is_valid() and pf.is_valid():
            # --- Save user ---
            user = uf.save(commit=False)
            pwd = uf.cleaned_data.get('password')
            if pwd:
                user.set_password(pwd)  # only change if provided
            user.save()

            # --- Save or create profile ---
            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get('permissions') or []
            profile.save()

            messages.success(request, "User updated successfully.")
            return redirect('users:list_users')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        # password blank on edit
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
