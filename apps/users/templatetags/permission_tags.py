# apps/users/templatetags/permission_tags.py

from django import template

register = template.Library()

@register.filter(name='has_permission')
def has_permission(user, perm_code):
    """
    Usage in templates:
        {% load permission_tags %}
        {% if request.user|has_permission:"leave_apply" %}
            ... show the “Leave Apply” link ...
        {% endif %}

    Returns True if:
      • user.is_superuser
      • or perm_code is in user.profile.permissions (a list stored on Profile.permissions)
    """
    # must be a logged‐in user
    if not getattr(user, 'is_authenticated', False):
        return False

    # superuser sees everything
    if user.is_superuser:
        return True

    # try to grab their Profile
    try:
        profile = user.profile
    except Exception:
        return False

    perms = getattr(profile, 'permissions', None) or []
    return perm_code in perms
