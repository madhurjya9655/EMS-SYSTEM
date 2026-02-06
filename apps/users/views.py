# apps/users/views.py
from __future__ import annotations

import logging
from typing import Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import Permission
from django.contrib.auth.views import LoginView
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse, reverse_lazy
from django.views.decorators.http import require_http_methods
from django.utils.http import url_has_allowed_host_and_scheme

from .forms import CustomAuthForm, ProfileForm, UserForm
from .models import Profile
from .permission_urls import PERMISSION_URLS
from .permissions import PERMISSIONS_STRUCTURE, _user_permission_codes, permissions_context
from .utils import soft_delete_user

User = get_user_model()
logger = logging.getLogger(__name__)


def admin_only(user) -> bool:
    return bool(getattr(user, "is_superuser", False))


def _set_kam_access(target_user: User, enabled: bool, *, actor: Optional[User]) -> None:
    """
    Grant/revoke explicit KAM module access using the Django permission 'kam.access_kam_module'.
    Idempotent. Logs an audit trail entry.
    """
    try:
        perm = Permission.objects.get(codename="access_kam_module", content_type__app_label="kam")
    except Permission.DoesNotExist:
        logger.error("KAM permission 'kam.access_kam_module' not found. Ensure KAM app declares it.")
        return
    except Exception as e:
        logger.exception("Error fetching KAM permission: %s", e)
        return

    try:
        if enabled:
            target_user.user_permissions.add(perm)
            logger.info(
                "KAM access ENABLED for user_id=%s by actor_id=%s",
                getattr(target_user, "id", "?"),
                getattr(actor, "id", None),
            )
        else:
            target_user.user_permissions.remove(perm)
            logger.info(
                "KAM access DISABLED for user_id=%s by actor_id=%s",
                getattr(target_user, "id", "?"),
                getattr(actor, "id", None),
            )
    except Exception:
        logger.exception("Failed to update KAM access for user %s", getattr(target_user, "id", "?"))


class CustomLoginView(LoginView):
    template_name = "registration/login.html"
    authentication_form = CustomAuthForm
    redirect_authenticated_user = True

    def form_valid(self, form):
        remember_checked = bool(self.request.POST.get("remember"))
        self.request.session.set_expiry(None if remember_checked else 0)
        return super().form_valid(form)

    def get_success_url(self) -> str:
        nxt = self.request.POST.get("next") or self.request.GET.get("next")
        if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={self.request.get_host()}):
            return nxt

        user = self.request.user
        if getattr(user, "is_superuser", False):
            return reverse_lazy("dashboard:home")

        user_codes = _user_permission_codes(user)
        for code, url_name in PERMISSION_URLS.items():
            if code.lower() in user_codes:
                try:
                    return reverse(url_name)
                except NoReverseMatch:
                    continue

        return reverse_lazy("dashboard:home")


@login_required
@user_passes_test(admin_only)
def list_users(request: HttpRequest) -> HttpResponse:
    users = User.objects.order_by("first_name", "last_name", "username")
    return render(request, "users/list_user.html", {"users": users})


@login_required
@user_passes_test(admin_only)
def add_user(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        uf = UserForm(request.POST)
        # include request.FILES so profile photo can be uploaded
        pf = ProfileForm(request.POST, request.FILES)

        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)
            raw_pwd = uf.cleaned_data.get("password")
            # On create it's guaranteed by the form; still guard:
            if raw_pwd:
                user.set_password(raw_pwd)
            user.is_staff = False
            user.is_active = True
            user.save()

            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get("permissions") or []
            profile.save()

            # Explicit KAM access toggle (Option B: Django permission)
            enable_kam = bool(request.POST.get("enable_kam"))
            _set_kam_access(user, enable_kam, actor=request.user)

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
    user_obj = get_object_or_404(User, pk=pk)
    profile_obj: Optional[Profile] = Profile.objects.filter(user=user_obj).first()

    if request.method == "POST":
        uf = UserForm(request.POST, instance=user_obj)
        # include request.FILES so profile photo updates work
        pf = ProfileForm(request.POST, request.FILES, instance=profile_obj)

        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)  # password not touched by the form
            pwd = uf.cleaned_data.get("password")
            if pwd:
                user.set_password(pwd)     # only when a new password was provided
            user.save()

            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get("permissions") or []
            profile.save()

            # Explicit KAM access toggle (Option B: Django permission)
            enable_kam = bool(request.POST.get("enable_kam"))
            _set_kam_access(user, enable_kam, actor=request.user)

            messages.success(request, "User updated successfully.")
            return redirect("users:list_users")

        messages.error(request, "Please correct the errors below.")
    else:
        uf = UserForm(instance=user_obj)   # no password initial needed anymore
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
    try:
        user = User.objects.get(pk=pk)
    except User.DoesNotExist:
        messages.error(request, "No user matches the given query.")
        return render(request, "users/user_not_found.html", status=404)

    if request.method == "POST":
        if user == request.user:
            messages.error(request, "You cannot delete your own account!")
            return redirect("users:list_users")

        if getattr(user, "is_superuser", False) and not getattr(request.user, "is_superuser", False):
            return HttpResponseForbidden("Only a superuser can delete another superuser.")

        try:
            username = user.username
            soft_delete_user(user, performed_by=request.user)
            messages.success(request, f"User '{username}' deleted (soft) and anonymized.")
        except Exception as e:
            logger.error("Soft-delete failed for user %s: %s", user.pk, e)
            messages.error(request, f"Could not delete user: {e}")

        return redirect("users:list_users")

    return render(request, "users/confirm_delete.html", {"user": user})


@login_required
@user_passes_test(admin_only)
@require_http_methods(["GET", "POST"])
def toggle_active(request: HttpRequest, pk: int) -> HttpResponse:
    u = get_object_or_404(User, pk=pk)
    if u == request.user:
        messages.error(request, "You cannot deactivate your own account.")
        return redirect("users:list_users")
    if getattr(u, "is_superuser", False) and not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Only a superuser can change another superuser's status.")

    # Flip and persist
    u.is_active = not u.is_active
    u.save(update_fields=["is_active"])

    messages.success(request, f"User {'activated' if u.is_active else 'deactivated'} successfully.")

    nxt = request.GET.get("next") or request.POST.get("next")
    if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
        return redirect(nxt)
    return redirect("users:list_users")


@login_required
def debug_permissions(request: HttpRequest) -> HttpResponse:
    user = request.user
    user_perms = _user_permission_codes(user)

    permission_urls = {}
    for code, url_name in PERMISSION_URLS.items():
        if code.lower() in user_perms or getattr(user, "is_superuser", False):
            try:
                url = reverse(url_name)
                permission_urls[code] = {"url_name": url_name, "url": url}
            except NoReverseMatch:
                permission_urls[code] = {"url_name": url_name, "url": None, "error": "URL not found"}

    url_to_perms = {}
    for code, url in PERMISSION_URLS.items():
        url_to_perms.setdefault(url, []).append(code)

    return render(
        request,
        "users/debug_permissions.html",
        {
            "user": user,
            "is_superuser": user.is_superuser,
            "is_staff": user.is_staff,
            "permissions": sorted(user_perms),
            "permission_urls": permission_urls,
            "url_to_perms": url_to_perms,
            "all_permissions": sorted(PERMISSION_URLS.keys()),
            "context": permissions_context(request),
        },
    )
