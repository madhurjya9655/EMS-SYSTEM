# apps/users/templatetags/user_filters.py
from django import template
from ..permissions import _user_permission_codes

register = template.Library()

@register.filter(name="has_permission")
def user_has_permission(user, code: str) -> bool:
    """Template usage: {% if user|has_permission:'add_ticket' %}"""
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    return str(code or "").strip().lower() in _user_permission_codes(user)

@register.filter(name="has_any_permission")
def user_has_any_permission(user, csv_codes: str) -> bool:
    """Check if the user has any permission from a CSV string of codes."""
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    wanted = {c.strip().lower() for c in (csv_codes or "").split(",") if c.strip()}
    return bool(_user_permission_codes(user) & wanted)

@register.filter(name="has_module_permission")
def user_has_module_permission(user, module_name: str) -> bool:
    """Check if user has any permission in a given module."""
    from ..permissions import PERMISSIONS_STRUCTURE
    
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    
    module = module_name.strip()
    if not module or module not in PERMISSIONS_STRUCTURE:
        return False
    
    user_perms = _user_permission_codes(user)
    if {"*", "all"} & user_perms:
        return True
        
    # Get all permission codes for this module
    module_perms = {code.lower() for code, _ in PERMISSIONS_STRUCTURE[module]}
    
    # Check if user has any of these permissions
    return bool(user_perms & module_perms)