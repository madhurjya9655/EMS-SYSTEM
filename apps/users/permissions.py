from __future__ import annotations

import json
from functools import wraps
from typing import Iterable, Set, Any

from django import template
from django.core.exceptions import PermissionDenied
from django.contrib.auth.views import redirect_to_login

# Optional import used by helpers (safe if unused elsewhere)
try:
    from .permission_urls import PERMISSION_URLS  # noqa: F401
except Exception:
    PERMISSION_URLS = {}

register = template.Library()


# -----------------------------------------------------------------------------
# Public: PERMISSIONS_STRUCTURE describes all possible app-level permissions.
# Keep as-is if you import it from elsewhere; define here if this is the source.
# -----------------------------------------------------------------------------
PERMISSIONS_STRUCTURE = {
    "Leave": [
        ("leave_apply", "Leave Apply"),
        ("leave_list", "Leave List"),
    ],
    "Checklist": [
        ("add_checklist", "Add Checklist"),
        ("list_checklist", "List Checklist"),
    ],
    "Delegation": [
        ("add_delegation", "Add Delegation"),
        ("list_delegation", "List Delegation"),
    ],
    "Tickets": [
        ("add_ticket", "Add Ticket"),
        ("list_all_tickets", "List All Tickets"),
        ("assigned_to_me", "Assigned to Me"),
        ("assigned_by_me", "Assigned by Me"),
    ],
    "Petty Cash": [
        ("petty_cash_list", "Petty-Cash List"),
        ("petty_cash_apply", "Petty-Cash Apply"),
    ],
    "Sales": [
        ("add_sales_plan", "Add Sales Plan"),
        ("list_sales_plan", "List Sales Plan"),
    ],
    "Reimbursement": [
        ("reimbursement_apply", "Reimbursement Apply"),
        ("reimbursement_list", "Reimbursement List"),
    ],
    "Reports": [
        ("doer_tasks", "Doer Tasks"),
        ("weekly_mis_score", "Weekly MIS Score"),
        ("performance_score", "Performance Score"),
    ],
    "Users": [
        ("list_users", "List Users"),
        ("add_user", "Add User"),
        ("system_settings", "System Settings"),
        ("authorized_numbers", "Authorized Numbers"),
    ],
    "Clients": [
        ("manage_clients_add", "Add"),
        ("manage_clients_list", "List"),
        ("manage_clients_edit", "Edit"),
        ("manage_clients_delete", "Delete"),
        ("manage_clients_upload", "Upload"),
        ("manage_clients_upload_dndrnd", "Upload DND/RND"),
    ],
    "Customer Group": [
        ("customer_group_add", "Add"),
        ("customer_group_list", "List"),
        ("customer_group_edit", "Edit"),
        ("customer_group_delete", "Delete"),
        ("customer_group_csv", "CSV Export"),
    ],
    "WhatsApp Template": [
        ("wa_template_add", "Add"),
        ("wa_template_list", "List"),
        ("wa_template_edit", "Edit"),
        ("wa_template_delete", "Delete"),
    ],
    "Master Tasks": [
        ("mt_add_checklist", "Add Checklist"),
        ("mt_list_checklist", "List Checklist"),
        ("mt_edit_checklist", "Edit Checklist"),
        ("mt_delete_checklist", "Delete Checklist"),
        ("mt_add_delegation", "Add Delegation"),
        ("mt_list_delegation", "List Delegation"),
        ("mt_edit_delegation", "Edit Delegation"),
        ("mt_delete_delegation", "Delete Delegation"),
        ("mt_bulk_upload", "Bulk Upload"),
        ("mt_delegation_planned_date_edit", "Delegation Planned Date Edit"),
        ("mt_delegation_planned_date_list", "Delegation Planned Date List"),
    ],
    "Organization": [
        ("org_add_branch", "Add Branch"),
        ("org_list_branch", "List Branch"),
        ("org_edit_branch", "Edit Branch"),
        ("org_delete_branch", "Delete Branch"),
        ("org_add_company", "Add Company"),
        ("org_list_company", "List Company"),
        ("org_edit_company", "Edit Company"),
        ("org_delete_company", "Delete Company"),
        ("org_add_department", "Add Department"),
        ("org_list_department", "List Department"),
        ("org_edit_department", "Edit Department"),
        ("org_delete_department", "Delete Department"),
    ],
}

ALL_PERMISSION_CODES: list[str] = [
    code for perms in PERMISSIONS_STRUCTURE.values() for code, _ in perms
]


def get_permission_label(code: str) -> str:
    for group, perms in PERMISSIONS_STRUCTURE.items():
        for c, label in perms:
            if c == code:
                return f"{group} – {label}"
    return code


# -----------------------------------------------------------------------------
# Core extraction util: read Profile.permissions (JSONField) into a clean set
# Supports: list/tuple/set; JSON string; CSV string; None.
# -----------------------------------------------------------------------------
def _normalize_codes(values: Iterable[Any]) -> Set[str]:
    normalized: Set[str] = set()
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            normalized.add(s)
    return normalized


def _extract_perms(user) -> Set[str]:
    """
    Return a set of permission codes for this user from Profile.permissions.
    - Superusers are handled by callers (treated as allow-all).
    - Tolerates bad/mixed types and keeps it resilient.
    """
    profile = getattr(user, "profile", None)
    if not profile:
        return set()

    perms = getattr(profile, "permissions", [])  # JSONField (preferably a list)

    # Already a collection?
    if isinstance(perms, (list, tuple, set)):
        return _normalize_codes(perms)

    # Stringified JSON list?
    if isinstance(perms, str):
        s = perms.strip()
        if not s:
            return set()
        # Try JSON decode
        try:
            decoded = json.loads(s)
            if isinstance(decoded, (list, tuple, set)):
                return _normalize_codes(decoded)
        except Exception:
            pass
        # Fallback: CSV string
        return _normalize_codes([p for p in s.split(",")])

    # Unknown: best effort
    try:
        return _normalize_codes(perms)  # type: ignore[arg-type]
    except Exception:
        return set()


# -----------------------------------------------------------------------------
# View decorator & mixin
# -----------------------------------------------------------------------------
def has_permission(code: str):
    """
    Decorator for function views:
    - Anonymous -> login
    - Superuser -> allow
    - If code in user's permissions -> allow
    - Else -> 403
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            user = getattr(request, "user", None)
            if not getattr(user, "is_authenticated", False):
                return redirect_to_login(request.get_full_path())
            if getattr(user, "is_superuser", False):
                return view_func(request, *args, **kwargs)
            if code in _extract_perms(user):
                return view_func(request, *args, **kwargs)
            raise PermissionDenied
        return _wrapped
    return decorator


class PermissionRequiredMixin:
    """
    CBV mixin:
    - Set `permission_code = "code"`
    - Superuser bypasses checks.
    """
    permission_code: str | None = None

    def dispatch(self, request, *args, **kwargs):
        user = getattr(request, "user", None)
        if not getattr(user, "is_authenticated", False):
            return redirect_to_login(request.get_full_path())
        if getattr(user, "is_superuser", False):
            return super().dispatch(request, *args, **kwargs)
        if self.permission_code and self.permission_code in _extract_perms(user):
            return super().dispatch(request, *args, **kwargs)
        raise PermissionDenied


# -----------------------------------------------------------------------------
# Template filters (used by your sidebar)
# -----------------------------------------------------------------------------
@register.filter(name="has_permission")
def user_has_permission(user, code: str) -> bool:
    """Template usage:  {% if user|has_permission:'add_ticket' %} ... {% endif %}"""
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    return code in _extract_perms(user)


@register.filter(name="has_any_permission")
def user_has_any_permission(user, csv_codes: str) -> bool:
    """
    Check if the user has any permission from a CSV string of codes.
    Example: {% if user|has_any_permission:"add_ticket,list_all_tickets" %} ... {% endif %}
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    wanted = {c.strip() for c in (csv_codes or "").split(",") if c.strip()}
    return bool(_extract_perms(user) & wanted)
