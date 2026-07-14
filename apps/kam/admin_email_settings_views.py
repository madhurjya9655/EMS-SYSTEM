# FILE: apps/kam/admin_email_settings_views.py

from __future__ import annotations

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.urls import reverse

from .models import KAMEmailApprovalSettings

User = get_user_model()


def _is_kam_admin(user) -> bool:
    """
    Allow access only to:
    - Django superusers
    - users belonging to the Django Admin group
    """
    if not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    try:
        return user.groups.filter(name__iexact="Admin").exists()
    except Exception:
        return False


def _selected_user_ids(
    request: HttpRequest,
    field_name: str,
) -> list[int]:
    """
    Convert repeated POST checkbox values into a clean unique integer list.
    """
    output: list[int] = []

    for raw_value in request.POST.getlist(field_name):
        value = str(raw_value or "").strip()

        if value.isdigit():
            output.append(int(value))

    return list(dict.fromkeys(output))


@login_required(login_url="/accounts/login/")
def admin_kam_email_settings(
    request: HttpRequest,
) -> HttpResponse:
    """
    Administrator-managed KAM approval email recipient settings.

    Controls:
    - approval users;
    - CC users;
    - mapped-manager inclusion;
    - configuration enabled/disabled state.
    """
    if not _is_kam_admin(request.user):
        return HttpResponseForbidden(
            "403 Forbidden: Admin access required."
        )

    config = KAMEmailApprovalSettings.get_solo()

    if request.method == "POST":
        approval_user_ids = _selected_user_ids(
            request,
            "approval_users",
        )

        cc_user_ids = _selected_user_ids(
            request,
            "cc_users",
        )

        approval_users = list(
            User.objects.filter(
                id__in=approval_user_ids,
                is_active=True,
            )
            .exclude(email__isnull=True)
            .exclude(email__exact="")
        )

        cc_users = list(
            User.objects.filter(
                id__in=cc_user_ids,
                is_active=True,
            )
            .exclude(email__isnull=True)
            .exclude(email__exact="")
        )

        # Prevent duplicate addresses in both approval TO and CC.
        approval_email_keys = {
            (user.email or "").strip().lower()
            for user in approval_users
            if (user.email or "").strip()
        }

        cc_users = [
            user
            for user in cc_users
            if (user.email or "").strip().lower()
            not in approval_email_keys
        ]

        config.is_active = (
            request.POST.get("is_active") == "on"
        )

        config.include_mapped_manager = (
            request.POST.get("include_mapped_manager") == "on"
        )

        config.save(
            update_fields=[
                "is_active",
                "include_mapped_manager",
            ]
        )

        config.approval_users.set(approval_users)
        config.cc_users.set(cc_users)

        messages.success(
            request,
            "KAM approval email settings saved successfully.",
        )

        return redirect(
            reverse("kam:admin_kam_email_settings")
        )

    users = list(
        User.objects.filter(is_active=True)
        .exclude(email__isnull=True)
        .exclude(email__exact="")
        .order_by(
            "first_name",
            "last_name",
            "email",
            "username",
        )
    )

    context = {
        "page_title": "KAM Approval Email Settings",
        "config": config,
        "users": users,
        "selected_approval_ids": set(
            config.approval_users.values_list(
                "id",
                flat=True,
            )
        ),
        "selected_cc_ids": set(
            config.cc_users.values_list(
                "id",
                flat=True,
            )
        ),
    }

    return render(
        request,
        "kam/admin_kam_email_settings.html",
        context,
    )