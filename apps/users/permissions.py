# apps/users/permissions.py
from __future__ import annotations

import json
from functools import wraps
from typing import Any, Iterable, Set

from django import template
from django.contrib.auth.views import redirect_to_login
from django.http import HttpResponseForbidden
from django.apps import apps as django_apps

register = template.Library()

# Optional import used by helpers (safe if the file is absent)
try:
    from .permission_urls import PERMISSION_URLS  # noqa: F401
except Exception:  # pragma: no cover
    PERMISSION_URLS = {}

# -----------------------------------------------------------------------------
# Public: PERMISSIONS_STRUCTURE describes all possible app-level permissions.
# -----------------------------------------------------------------------------
PERMISSIONS_STRUCTURE = {
    "Leave": [
        ("leave_apply", "Leave Apply"),
        ("leave_list", "Leave List"),
        # NEW: needed by /leave/manager/... endpoints (approval queue & decide)
        ("leave_pending_manager", "Manager Approvals"),
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
# Internals: robust extraction from Profile.permissions
# Accepts: list/tuple/set/dict, JSON string, CSV string, or None.
# Case-insensitive; returns lowercased codes.
# -----------------------------------------------------------------------------
def _normalize_raw(values: Any) -> Set[str]:
    if not values:
        return set()

    if isinstance(values, dict):
        # treat truthy values as enabled flags
        return {str(k).strip().lower() for k, v in values.items() if v}

    if isinstance(values, (list, tuple, set)):
        return {str(v).strip().lower() for v in values if str(v).strip()}

    if isinstance(values, str):
        s = values.strip()
        if not s:
            return set()
        # Try JSON decode
        try:
            decoded = json.loads(s)
            return _normalize_raw(decoded)
        except Exception:
            # Fallback: CSV string
            return {p.strip().lower() for p in s.split(",") if p.strip()}

    # Best effort on unexpected types
    try:
        return {str(v).strip().lower() for v in values}  # type: ignore[arg-type]
    except Exception:  # pragma: no cover
        return set()


# Synonym map: having any synonym grants the base code too.
_SYNONYMS = {
    # Checklist
    "list_checklist": {"mt_list_checklist"},
    "add_checklist": {"mt_add_checklist"},
    # Delegation
    "list_delegation": {"mt_list_delegation"},
    "add_delegation": {"mt_add_delegation"},
    # (Add more synonyms here if needed)
}


def _expand_synonyms(codes: Set[str]) -> Set[str]:
    out = set(codes)
    for base, syns in _SYNONYMS.items():
        if base in codes or any(s in codes for s in syns):
            out.add(base)
            out |= syns
    return out


def _codes_from_groups(user) -> Set[str]:
    """
    Map Django auth groups to implied permission codes.
    Keeps Profile permissions as the source of truth; this just adds grants.
    """
    try:
        names = {n.strip().lower() for n in user.groups.values_list("name", flat=True)}
    except Exception:
        names = set()

    grants: Set[str] = set()

    # If a user is in "Manager", grant the leave-approval queue permission.
    if "manager" in names:
        grants.add("leave_pending_manager")

    # Add other group → code mappings here as your org evolves.

    return grants


def _codes_from_mapping(user) -> Set[str]:
    """
    Dynamic grant: users who are the Reporting Person for anyone
    (via ApproverMapping) automatically get 'leave_pending_manager'.
    """
    try:
        Mapping = django_apps.get_model("leave", "ApproverMapping")
        if not Mapping:
            return set()
        if Mapping.objects.filter(reporting_person_id=getattr(user, "id", 0)).exists():
            return {"leave_pending_manager"}
    except Exception:
        # Be silent; never break permission checks
        pass
    return set()


def _user_permission_codes(user) -> Set[str]:
    profile = getattr(user, "profile", None)
    raw = getattr(profile, "permissions", []) if profile else []
    codes = _normalize_raw(raw)

    # Add grants implied by Django auth groups (e.g., "Manager")
    codes |= _codes_from_groups(user)

    # Add dynamic RP grants from ApproverMapping
    codes |= _codes_from_mapping(user)

    return _expand_synonyms(codes)


# ---- Backwards-compatibility alias (required by apps.users.views etc.) ----
def _extract_perms(user) -> Set[str]:  # noqa: N802 (legacy name)
    """
    Legacy alias so older template tags importing `_extract_perms` keep working.
    Returns a set of lowercased permission codes (with synonyms, group & RP grants).
    """
    return _user_permission_codes(user)


# -----------------------------------------------------------------------------
# Decorator & Mixins
# -----------------------------------------------------------------------------
def has_permission(*required: str):
    """
    Usage:
        @has_permission("list_checklist")
        @has_permission("leave_pending_manager")  # manager approval views

    Rules:
        • Anonymous  -> redirect to login
        • Superuser  -> allow
        • '*' or 'all' in Profile.permissions -> allow
        • Case-insensitive
        • Honors grants from Django Groups and ApproverMapping (RP auto-grant)
    """
    need = {str(c).strip().lower() for c in required if c}

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            user = getattr(request, "user", None)

            if not getattr(user, "is_authenticated", False):
                return redirect_to_login(request.get_full_path())

            if getattr(user, "is_superuser", False):
                return view_func(request, *args, **kwargs)

            have = _user_permission_codes(user)
            if {"*", "all"} & have:
                return view_func(request, *args, **kwargs)

            if not need or (have & need):
                return view_func(request, *args, **kwargs)

            return HttpResponseForbidden("403 Forbidden")
        return _wrapped
    return decorator


class PermissionRequiredMixin:
    """
    CBV mixin:
      • Set `permission_code = "code"`
      • Superuser bypasses checks
      • Honors grants from Django Groups and ApproverMapping (RP auto-grant)
    """
    permission_code: str | None = None

    def dispatch(self, request, *args, **kwargs):
        user = getattr(request, "user", None)
        if not getattr(user, "is_authenticated", False):
            return redirect_to_login(request.get_full_path())

        if getattr(user, "is_superuser", False):
            return super().dispatch(request, *args, **kwargs)

        if self.permission_code:
            have = _user_permission_codes(user)
            if {"*", "all"} & have or self.permission_code.lower() in have:
                return super().dispatch(request, *args, **kwargs)

        return HttpResponseForbidden("403 Forbidden")


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
    return str(code or "").strip().lower() in _user_permission_codes(user)


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
    wanted = {c.strip().lower() for c in (csv_codes or "").split(",") if c.strip()}
    return bool(_user_permission_codes(user) & wanted)


# -----------------------------------------------------------------------------
# Optional helper for templates/context processors
# -----------------------------------------------------------------------------
def sidebar_flags_for(user) -> dict:
    """
    Compute booleans templates can use to show/hide sections.
    - show_manager_approvals: True for superusers, explicit permission, or RP via ApproverMapping.
    - can_apply_leave / can_view_leave: convenience flags.
    """
    codes = _user_permission_codes(user) if getattr(user, "is_authenticated", False) else set()
    return {
        "show_manager_approvals": bool(
            getattr(user, "is_superuser", False)
            or ("leave_pending_manager" in codes)
        ),
        "can_apply_leave": "leave_apply" in codes or getattr(user, "is_superuser", False),
        "can_view_leave": "leave_list" in codes or getattr(user, "is_superuser", False),
    }
