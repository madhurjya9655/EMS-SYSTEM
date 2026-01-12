# apps/users/templatetags/users_permissions.py
from __future__ import annotations

from typing import Set

from django import template
from django.contrib.auth import get_user_model

# Import the central permission helpers & filters
# These are the single source of truth for your app-level permission codes.
from apps.users.permissions import (
    _user_permission_codes,
    user_has_permission as _filter_has_permission,
    user_has_any_permission as _filter_has_any_permission,
    user_has_module_permission as _filter_has_module_permission,
    get_permission_label as _get_permission_label,
)

register = template.Library()
User = get_user_model()


# ---------------------------------------------------------------------
# Safe utils
# ---------------------------------------------------------------------
def _is_auth(user) -> bool:
    return bool(getattr(user, "is_authenticated", False))


def _all_codes(user) -> Set[str]:
    """
    Returns the full expanded set of permission codes for a user.
    Uses your app-level permission system (profile JSON, groups, RP mapping, etc.).
    """
    try:
        if getattr(user, "is_superuser", False):
            return {"*"}
        return _user_permission_codes(user)
    except Exception:
        # Never break templates because of a runtime error
        return set()


# ---------------------------------------------------------------------
# Template filters (public API)
# ---------------------------------------------------------------------
@register.filter(name="has_permission")
def has_permission(user, code: str) -> bool:
    """
    Template usage:
        {% if request.user|has_permission:'leave_pending_manager' %} ... {% endif %}

    Checks your app-level code (NOT Django's auth "app.codename").
    Superusers always pass.
    """
    if not _is_auth(user):
        return False
    if getattr(user, "is_superuser", False):
        return True
    try:
        return _filter_has_permission(user, str(code or "").strip())
    except Exception:
        return False


@register.filter(name="has_any_permission")
def has_any_permission(user, csv_codes: str) -> bool:
    """
    Template usage:
        {% if request.user|has_any_permission:"add_ticket,list_all_tickets" %} ... {% endif %}
    """
    if not _is_auth(user):
        return False
    if getattr(user, "is_superuser", False):
        return True
    try:
        return _filter_has_any_permission(user, str(csv_codes or ""))
    except Exception:
        return False


@register.filter(name="has_module_permission")
def has_module_permission(user, module_name: str) -> bool:
    """
    Template usage:
        {% if request.user|has_module_permission:'Leave' %} ... {% endif %}
    """
    if not _is_auth(user):
        return False
    if getattr(user, "is_superuser", False):
        return True
    try:
        return _filter_has_module_permission(user, str(module_name or ""))
    except Exception:
        return False


@register.filter(name="permission_label")
def permission_label(code: str) -> str:
    """
    Optional helper to render human-friendly labels from a code.
    Example:
        {{ "leave_pending_manager"|permission_label }}
        => "Leave – Manager Approvals"
    """
    try:
        return _get_permission_label(str(code or ""))
    except Exception:
        return str(code or "")


@register.simple_tag(name="user_permission_codes")
def user_permission_codes(user) -> str:
    """
    Optional: dump all effective codes for the current user as a CSV string.
    Useful for debugging in templates (behind DEBUG or staff checks).
        {% user_permission_codes request.user as codes %}
        {{ codes }}
    """
    if not _is_auth(user):
        return ""
    codes = _all_codes(user)
    if "*" in codes:
        return "*"
    return ",".join(sorted(codes))
