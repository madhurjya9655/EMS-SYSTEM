from __future__ import annotations

import json
import logging
from functools import wraps
from typing import Any, Set

from django import template
from django.apps import apps as django_apps
from django.conf import settings
from django.contrib.auth.views import redirect_to_login
from django.http import HttpResponseForbidden

register = template.Library()
logger = logging.getLogger(__name__)

# Toggle debug logging for permissions via settings
PERMISSION_DEBUG = bool(getattr(settings, "PERMISSION_DEBUG_ENABLED", False))

# Optional import used by helpers (safe if the file is absent)
try:
    from .permission_urls import PERMISSION_URLS  # noqa: F401
except Exception:  # pragma: no cover
    PERMISSION_URLS = {}  # type: ignore

# -----------------------------------------------------------------------------
# Public: PERMISSIONS_STRUCTURE describes all possible app-level permissions.
# -----------------------------------------------------------------------------
PERMISSIONS_STRUCTURE = {
    "Leave": [
        ("leave_apply", "Leave Apply"),
        ("leave_list", "Leave List"),
        ("leave_pending_manager", "Manager Approvals"),
        ("leave_cc_admin", "Manage CC (Admin)"),
        ("leave_admin_edit", "Admin Edit Leave"),  # NEW
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
        # Employee
        ("reimbursement_apply", "Reimbursement Apply (My Inbox / New)"),
        ("reimbursement_list", "Reimbursement List (My Requests)"),
        # Manager
        ("reimbursement_manager_pending", "Manager Queue (Pending Requests)"),
        ("reimbursement_manager_review", "Manager Review (Single Request)"),
        # Management (optional; only if you use these views)
        ("reimbursement_management_pending", "Management Queue (Pending Requests)"),
        ("reimbursement_management_review", "Management Review (Single Request)"),
        # Finance
        ("reimbursement_finance_pending", "Finance Queue (Pending Requests)"),
        ("reimbursement_finance_review", "Finance Review (Single Request)"),
        # Admin console (Bills / Requests / Employee / Status / Mapping)
        ("reimbursement_admin", "Admin – Reimbursement Console"),
        # NEW: Analytics dashboard (keeps analytics inside main app theme)
        ("reimbursement_analytics", "Analytics Dashboard"),
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

    # Finance (reimburse) — support legacy alias so both codes work
    "reimbursement_finance_review": {"reimbursement_review_finance"},
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

    # If a user is in "Manager", grant the leave-approval queue permission,
    # and allow seeing reimbursement analytics.
    if "manager" in names:
        grants.add("leave_pending_manager")
        grants.add("reimbursement_analytics")

    # Finance should also be able to see analytics.
    if "finance" in names:
        grants.add("reimbursement_analytics")

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
        logger.exception("Error checking ApproverMapping")
        pass
    return set()


def _baseline_dynamic_grants(user) -> Set[str]:
    """
    Baseline employee-level grants for every authenticated user.
    Ensures all employees can see and use:
      - Expenses Inbox / Apply (reimbursement_apply)
      - My Requests (reimbursement_list)
    """
    if not getattr(user, "is_authenticated", False):
        return set()
    return {"reimbursement_apply", "reimbursement_list"}


def _user_permission_codes(user) -> Set[str]:
    """
    Get all permission codes for a user, including:
    - Profile.permissions
    - Group-based grants
    - ApproverMapping-based grants
    - Baseline dynamic grants (employee-level)
    - Expanded synonym permissions

    Returns a set of lowercased permission codes.
    """
    if getattr(user, "is_superuser", False):
        # Superusers get all permissions - return a special wildcard
        return {"*"}

    profile = getattr(user, "profile", None)
    raw = getattr(profile, "permissions", []) if profile else []
    codes = _normalize_raw(raw)

    # Debug: raw permissions
    if PERMISSION_DEBUG:
        logger.debug(
            "Raw permissions for user %s: %s",
            getattr(user, "username", getattr(user, "email", "unknown")),
            sorted(codes),
        )

    # Grants via Django groups (e.g., "Manager", "Finance")
    codes |= _codes_from_groups(user)

    # Dynamic RP grant via ApproverMapping
    codes |= _codes_from_mapping(user)

    # Baseline employee-level reimbursement access for all authenticated users
    codes |= _baseline_dynamic_grants(user)

    # Apply synonyms
    expanded = _expand_synonyms(codes)

    # Debug: expanded delta
    if PERMISSION_DEBUG and expanded != codes:
        logger.debug(
            "Expanded permissions for user %s: %s",
            getattr(user, "username", getattr(user, "email", "unknown")),
            sorted(expanded - codes),
        )

    return expanded


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

            logger.warning(
                "Permission denied: user=%s, required=%s, has=%s",
                getattr(user, "username", getattr(user, "email", "unknown")),
                sorted(need),
                sorted(have),
            )
            return HttpResponseForbidden("403 Forbidden: You don't have permission to access this page.")
        return _wrapped
    return decorator


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
# Helper for checking module-level permissions (for sidebar)
# -----------------------------------------------------------------------------
@register.filter(name="has_module_permission")
def user_has_module_permission(user, module_name: str) -> bool:
    """
    Check if user has any permission in a given module.
    Usage: {% if user|has_module_permission:'Leave' %} ... {% endif %}
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True

    module = (module_name or "").strip()
    if not module or module not in PERMISSIONS_STRUCTURE:
        return False

    user_perms = _user_permission_codes(user)
    if {"*", "all"} & user_perms:
        return True

    module_perms = {code.lower() for code, _ in PERMISSIONS_STRUCTURE[module]}
    return bool(user_perms & module_perms)


# -----------------------------------------------------------------------------
# Context processor for templates
# -----------------------------------------------------------------------------
def permissions_context(request):
    """
    Adds permission-related context to all templates.
    Add to settings.TEMPLATES context_processors.
    """
    if not hasattr(request, "user") or not request.user.is_authenticated:
        return {}

    user_perms = _user_permission_codes(request.user)

    # Calculate module access for sidebar
    module_access = {}
    for module_name in PERMISSIONS_STRUCTURE.keys():
        module_access[module_name] = user_has_module_permission(request.user, module_name)

    # Create permission checks for specific features
    sidebar_flags = {
        "show_manager_approvals": bool(
            getattr(request.user, "is_superuser", False)
            or ("leave_pending_manager" in user_perms)
        ),
        "can_apply_leave": "leave_apply" in user_perms or getattr(request.user, "is_superuser", False),
        "can_view_leave": "leave_list" in user_perms or getattr(request.user, "is_superuser", False),
        "can_view_checklist": "list_checklist" in user_perms or getattr(request.user, "is_superuser", False),
        "can_add_checklist": "add_checklist" in user_perms or getattr(request.user, "is_superuser", False),
        "can_view_delegation": "list_delegation" in user_perms or getattr(request.user, "is_superuser", False),
        "can_add_delegation": "add_delegation" in user_perms or getattr(request.user, "is_superuser", False),
    }

    return {
        "user_permissions": user_perms,
        "module_access": module_access,
        **sidebar_flags,
    }
