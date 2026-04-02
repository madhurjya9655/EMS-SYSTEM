#FILE: apps/users/views.py
from __future__ import annotations

import logging
from typing import Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import Permission
from django.contrib.auth.views import LoginView
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import NoReverseMatch, reverse, reverse_lazy
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_http_methods

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
    Backward-compatible helper: grant/revoke explicit KAM module access using
    Django permission 'kam.access_kam_module'.

    NOTE:
      - If your project has removed the KAM hard-gate, this becomes a no-op
        (permission may not exist). It is safe and idempotent.
      - Keep it for deployments where the perm still exists.
    """
    try:
        perm = Permission.objects.get(codename="access_kam_module", content_type__app_label="kam")
    except Permission.DoesNotExist:
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


def _fallback_deactivate_user(user: User, *, actor: Optional[User] = None) -> None:
    """
    Production-safe fallback when soft_delete_user cannot complete due to
    protected historical records or cleanup failures.

    This function preserves all linked history and only deactivates access.
    """
    update_fields = []

    if getattr(user, "is_active", True):
        user.is_active = False
        update_fields.append("is_active")

    try:
        user.set_unusable_password()
        update_fields.append("password")
    except Exception:
        logger.exception("Failed to set unusable password for user_id=%s", getattr(user, "id", None))

    if update_fields:
        deduped_fields = []
        for field in update_fields:
            if field not in deduped_fields:
                deduped_fields.append(field)
        user.save(update_fields=deduped_fields)

    try:
        _set_kam_access(user, False, actor=actor)
    except Exception:
        logger.exception("Failed to revoke KAM access during fallback deactivate for user_id=%s", getattr(user, "id", None))

    try:
        profile = Profile.objects.filter(user=user).first()
        if profile and hasattr(profile, "is_active") and getattr(profile, "is_active", True):
            profile.is_active = False
            profile.save(update_fields=["is_active"])
    except Exception:
        logger.exception("Failed to deactivate related profile for user_id=%s", getattr(user, "id", None))


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
    users = User.objects.filter(is_active=True).order_by("first_name", "last_name", "username")
    return render(request, "users/list_user.html", {"users": users})


@login_required
@user_passes_test(admin_only)
def add_user(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        uf = UserForm(request.POST)
        pf = ProfileForm(request.POST, request.FILES)

        if uf.is_valid() and pf.is_valid():
            user = uf.save(commit=False)
            raw_pwd = uf.cleaned_data.get("password")
            if raw_pwd:
                user.set_password(raw_pwd)
            user.is_staff = False
            user.is_active = True
            user.save()

            profile = pf.save(commit=False)
            profile.user = user
            profile.permissions = pf.cleaned_data.get("permissions") or []
            profile.save()

            perms = set((pf.cleaned_data.get("permissions") or []))
            has_any_kam = any(str(c).lower().startswith("kam_") for c in perms)
            _set_kam_access(user, has_any_kam, actor=request.user)

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
        pf = ProfileForm(request.POST, request.FILES, instance=profile_obj)

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

            perms = set((pf.cleaned_data.get("permissions") or []))
            has_any_kam = any(str(c).lower().startswith("kam_") for c in perms)
            _set_kam_access(user, has_any_kam, actor=request.user)

            messages.success(request, "User updated successfully.")
            return redirect("users:list_users")

        messages.error(request, "Please correct the errors below.")
    else:
        uf = UserForm(instance=user_obj)
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
@require_http_methods(["GET", "POST"])
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

        if not getattr(user, "is_active", True):
            messages.info(request, "User is already inactive.")
            return redirect("users:list_users")

        username = user.username

        try:
            with transaction.atomic():
                soft_delete_user(user, performed_by=request.user)
            messages.success(request, f"User '{username}' deleted (soft) and anonymized.")
        except ProtectedError:
            logger.warning(
                "ProtectedError while deleting user_id=%s. Falling back to safe deactivation.",
                user.pk,
                exc_info=True,
            )
            try:
                with transaction.atomic():
                    _fallback_deactivate_user(user, actor=request.user)
                messages.warning(
                    request,
                    f"User '{username}' could not be fully deleted because linked historical records exist. "
                    f"The account has been deactivated instead.",
                )
            except Exception as fallback_error:
                logger.exception(
                    "Fallback deactivation failed for user_id=%s: %s",
                    user.pk,
                    fallback_error,
                )
                messages.error(
                    request,
                    "Could not delete this user because protected historical records exist, "
                    "and automatic deactivation also failed. Please contact support.",
                )
        except Exception as e:
            logger.exception("Soft-delete failed for user %s: %s", user.pk, e)
            try:
                with transaction.atomic():
                    _fallback_deactivate_user(user, actor=request.user)
                messages.warning(
                    request,
                    f"User '{username}' could not be fully deleted. The account has been deactivated safely instead.",
                )
            except Exception:
                logger.exception("Fallback deactivation failed after generic delete error for user_id=%s", user.pk)
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