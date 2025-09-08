# apps/users/views.py
from __future__ import annotations

from typing import Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.views import LoginView
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse, reverse_lazy
from django.views.decorators.http import require_POST

from .forms import CustomAuthForm, ProfileForm, UserForm
from .models import Profile
from .permission_urls import PERMISSION_URLS
from .permissions import PERMISSIONS_STRUCTURE, _extract_perms

User = get_user_model()


def admin_only(user) -> bool:
    """Simple predicate for admin-only views."""
    return bool(getattr(user, "is_superuser", False))


class CustomLoginView(LoginView):
    """
    Login view with "Remember me" and smart post-login routing:

    - Superuser → dashboard
    - Otherwise → first URL from PERMISSION_URLS the user has access to
    - Fallback → dashboard
    """
    template_name = "registration/login.html"
    authentication_form = CustomAuthForm
    redirect_authenticated_user = True

    def form_valid(self, form):
        remember = self.request.POST.get("remember")
        # Session lifetime: 2 weeks if remembered, else session cookie
        self.request.session.set_expiry(1209600 if remember else 0)
        return super().form_valid(form)

    def get_success_url(self) -> str:
        user = self.request.user
        if getattr(user, "is_superuser", False):
            return reverse_lazy("dashboard:home")

        # Permissions come from Profile.permissions via helper
        user_codes = _extract_perms(user)

        # In PERMISSION_URLS order, route to the first available URL
        for code, url_name in PERMISSION_URLS.items():
            if code in user_codes:
                try:
                    return reverse(url_name)
                except NoReverseMatch:
                    # URL not present in this deployment; try next mapping
                    continue

        # Fallback
        return reverse_lazy("dashboard:home")


@login_required
@user_passes_test(admin_only)
def list_users(request: HttpRequest) -> HttpResponse:
    """
    Admin: list users.
    """
    users = User.objects.order_by("first_name", "last_name", "username")
    return render(request, "users/list_user.html", {"users": users})


@login_required
@user_passes_test(admin_only)
def add_user(request: HttpRequest) -> HttpResponse:
    """
    Admin: create a new user with an attached Profile + permissions list.
    """
    if request.method == "POST":
        uf = UserForm(request.POST)
        pf = ProfileForm(request.POST)

        if uf.is_valid() and pf.is_valid():
            # Create user
            user = uf.save(commit=False)
            raw_pwd = uf.cleaned_data.get("password") or ""
            if raw_pwd:
                user.set_password(raw_pwd)
            user.is_staff = False
            user.is_active = True
            user.save()

            # Create profile with permissions
            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get("permissions") or []
            profile.save()

            messages.success(request, "User created successfully.")
            return redirect("users:list_users")

        messages.error(request, "Please correct the errors below.")
    else:
        uf = UserForm()
        pf = ProfileForm()

    return render(
        request,
        "users/add_user.html",
        {"uf": uf, "pf": pf, "permissions_structure": PERMISSIONS_STRUCTURE},
    )


@login_required
@user_passes_test(admin_only)
def edit_user(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Admin: edit a user. Password change is optional (only when provided).
    Profile is created on save if it doesn't exist yet.
    """
    user_obj = get_object_or_404(User, pk=pk)
    profile_obj: Optional[Profile] = Profile.objects.filter(user=user_obj).first()

    if request.method == "POST":
        uf = UserForm(request.POST, instance=user_obj)
        pf = ProfileForm(request.POST, instance=profile_obj)  # instance may be None

        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)
            pwd = uf.cleaned_data.get("password")
            if pwd:
                user.set_password(pwd)
            user.save()

            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get("permissions") or []
            profile.save()

            messages.success(request, "User updated successfully.")
            return redirect("users:list_users")

        messages.error(request, "Please correct the errors below.")
    else:
        # Show blank password field on edit
        uf = UserForm(instance=user_obj, initial={"password": ""})
        pf = ProfileForm(instance=profile_obj)

    return render(
        request,
        "users/edit_user.html",
        {
            "uf": uf,
            "pf": pf,
            "user_obj": user_obj,
            "permissions_structure": PERMISSIONS_STRUCTURE,
        },
    )


@login_required
@user_passes_test(admin_only)
def delete_user(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Admin: confirm + delete user (POST only for the actual delete).
    Prevents self-deletion for safety.
    """
    try:
        user = User.objects.get(pk=pk)
    except User.DoesNotExist:
        messages.error(request, "No user matches the given query.")
        return render(request, "users/user_not_found.html", status=404)

    if request.method == "POST":
        if user == request.user:
            messages.error(request, "You cannot delete your own account!")
            return redirect("users:list_users")
        # Optional: block deletion of other superusers unless current user is also superuser
        if getattr(user, "is_superuser", False) and not getattr(request.user, "is_superuser", False):
            return HttpResponseForbidden("Only a superuser can delete another superuser.")
        user.delete()
        messages.success(request, "User deleted successfully.")
        return redirect("users:list_users")

    # GET → show confirm page
    return render(request, "users/confirm_delete.html", {"user": user})


@login_required
@user_passes_test(admin_only)
@require_POST
def toggle_active(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Admin: toggle the 'is_active' flag for a user (POST only).
    """
    u = get_object_or_404(User, pk=pk)
    if u == request.user:
        messages.error(request, "You cannot deactivate your own account.")
        return redirect("users:list_users")
    if getattr(u, "is_superuser", False) and not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Only a superuser can change another superuser's status.")
    u.is_active = not u.is_active
    u.save(update_fields=["is_active"])
    messages.success(request, f"User {'activated' if u.is_active else 'deactivated'} successfully.")
    return redirect("users:list_users")
