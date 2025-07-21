# apps/users/templatetags/permission_tags.py

from django import template
import json

register = template.Library()

@register.filter(name='has_permission')
def has_permission(user, perm_code):
    """
    Returns True if user has the permission code.
    Works for permissions stored as a list, JSON string, or comma-separated string.
    """
    if not getattr(user, 'is_authenticated', False):
        return False
    if user.is_superuser:
        return True
    try:
        profile = user.profile
    except Exception:
        return False

    perms = getattr(profile, 'permissions', None) or []

    # Handle if stored as JSON string
    if isinstance(perms, str):
        # Try loading as JSON, fallback to comma-split
        try:
            perms_loaded = json.loads(perms)
            if isinstance(perms_loaded, list):
                perms = perms_loaded
            else:
                perms = [str(perms_loaded)]
        except Exception:
            perms = [p.strip() for p in perms.split(',') if p.strip()]

    return perm_code in perms
