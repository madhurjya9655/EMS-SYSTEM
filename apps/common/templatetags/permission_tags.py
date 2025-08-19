from django import template

register = template.Library()


@register.filter
def has_permission(user, permission_name):
    """Check if user has a specific permission"""
    if not user or not user.is_authenticated:
        return False
    
    if user.is_superuser:
        return True
    
    # Check direct user permissions
    if user.user_permissions.filter(codename=permission_name).exists():
        return True
    
    # Check group permissions
    if user.groups.filter(permissions__codename=permission_name).exists():
        return True
    
    return False