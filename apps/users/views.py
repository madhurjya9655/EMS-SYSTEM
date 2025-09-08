# apps/users/views.py
from __future__ import annotations

import logging
from typing import Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.views import LoginView
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse, reverse_lazy
from django.views.decorators.http import require_POST
from django.db import transaction
from django.db.models import ProtectedError

from .forms import CustomAuthForm, ProfileForm, UserForm
from .models import Profile
from .permission_urls import PERMISSION_URLS
from .permissions import PERMISSIONS_STRUCTURE, _user_permission_codes, permissions_context

User = get_user_model()
logger = logging.getLogger(__name__)


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
        user_codes = _user_permission_codes(user)

        # In PERMISSION_URLS order, route to the first available URL
        for code, url_name in PERMISSION_URLS.items():
            if code.lower() in user_codes:
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
        
        try:
            # Forcefully delete related objects to ensure complete removal
            from django.db import connection
            
            # Get the user ID before deletion for logging
            user_id = user.id
            username = user.username
            
            # Force delete in raw SQL to bypass Django's protections
            with transaction.atomic():
                cursor = connection.cursor()
                
                # Delete profile first
                cursor.execute("DELETE FROM users_profile WHERE user_id = %s", [user_id])
                
                # Delete from auth_user
                cursor.execute("DELETE FROM auth_user WHERE id = %s", [user_id])
                
                # Additional deletions for other known related tables
                # These are common Django tables that might have user relationships
                try:
                    cursor.execute("DELETE FROM django_admin_log WHERE user_id = %s", [user_id])
                except Exception:
                    pass
                
                try:
                    cursor.execute("DELETE FROM auth_user_groups WHERE user_id = %s", [user_id])
                except Exception:
                    pass
                
                try:
                    cursor.execute("DELETE FROM auth_user_user_permissions WHERE user_id = %s", [user_id])
                except Exception:
                    pass
                
                # Add any other app-specific tables that have user references
                try:
                    cursor.execute("DELETE FROM leave_leaverequest WHERE user_id = %s", [user_id])
                except Exception:
                    pass
                
                try:
                    cursor.execute("DELETE FROM leave_leavedecisionaudit WHERE user_id = %s", [user_id])
                except Exception:
                    pass
            
            messages.success(request, f"User '{username}' completely deleted from the system.")
            
        except Exception as e:
            # Handle any database errors
            logger.error(f"Error deleting user {user.id}: {str(e)}")
            messages.error(request, f"Could not delete user: {str(e)}")
        
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


@login_required
def debug_permissions(request: HttpRequest) -> HttpResponse:
    """
    Debug view to show permissions for the current user.
    Add to urls.py as: path('debug-permissions/', debug_permissions, name='debug_permissions')
    """
    user = request.user
    user_perms = _user_permission_codes(user)
    
    # Map permissions to available URLs
    permission_urls = {}
    for code, url_name in PERMISSION_URLS.items():
        if code.lower() in user_perms or getattr(user, "is_superuser", False):
            try:
                url = reverse(url_name)
                permission_urls[code] = {
                    'url_name': url_name,
                    'url': url,
                }
            except NoReverseMatch:
                permission_urls[code] = {
                    'url_name': url_name,
                    'url': None,
                    'error': 'URL not found'
                }
    
    # Get the reverse URL mapping for debugging
    url_to_perms = {}
    for code, url in PERMISSION_URLS.items():
        if url not in url_to_perms:
            url_to_perms[url] = []
        url_to_perms[url].append(code)
    
    return render(request, 'users/debug_permissions.html', {
        'user': user,
        'is_superuser': user.is_superuser,
        'is_staff': user.is_staff,
        'permissions': sorted(user_perms),
        'permission_urls': permission_urls,
        'url_to_perms': url_to_perms,
        'all_permissions': sorted(PERMISSION_URLS.keys()),
        'context': permissions_context(request),
    })