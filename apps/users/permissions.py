# apps/users/permissions.py

from functools import wraps
from django.core.exceptions import PermissionDenied
from django import template

# --- PERMISSION STRUCTURE ---

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
    ]
}

ALL_PERMISSION_CODES = [code for perms in PERMISSIONS_STRUCTURE.values() for code, _ in perms]

def get_permission_label(code):
    """
    Returns the human-readable group + label for a given code.
    """
    for group, perms in PERMISSIONS_STRUCTURE.items():
        for c, label in perms:
            if c == code:
                return f"{group} – {label}"
    return code

def has_permission(code):
    """
    Decorator for views. Checks user or user.profile.permissions for the code.
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            if request.user.is_superuser:
                return view_func(request, *args, **kwargs)
            profile = getattr(request.user, 'profile', None)
            perms = getattr(profile, 'permissions', []) if profile else []
            if code in perms:
                return view_func(request, *args, **kwargs)
            raise PermissionDenied
        return _wrapped
    return decorator

class PermissionRequiredMixin:
    """
    CBV mixin for class-based views.
    """
    permission_code = None

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_superuser:
            return super().dispatch(request, *args, **kwargs)
        profile = getattr(request.user, 'profile', None)
        perms = getattr(profile, 'permissions', []) if profile else []
        if self.permission_code and self.permission_code not in perms:
            raise PermissionDenied
        return super().dispatch(request, *args, **kwargs)

# --- Template Filter for use in templates ---

register = template.Library()

@register.filter(name='has_permission')
def user_has_permission(user, code):
    """
    Template usage:
    {% if user|has_permission:"some_perm_code" %}
    """
    if not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    profile = getattr(user, 'profile', None)
    perms = getattr(profile, 'permissions', []) if profile else []
    return code in perms
