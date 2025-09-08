from django import template
from django.contrib.auth.models import Group

register = template.Library()

@register.filter
def has_permission(user, permission_name):
    """Check if user has specific permission"""
    if not user or not user.is_authenticated:
        return False
    
    if user.is_superuser:
        return True
    
    return user.has_perm(permission_name)

@register.filter  
def has_any_permission(user, permission_list):
    """Check if user has any of the permissions in comma-separated list"""
    if not user or not user.is_authenticated:
        return False
    
    if user.is_superuser:
        return True
        
    permissions = [perm.strip() for perm in permission_list.split(',')]
    return any(user.has_perm(perm) for perm in permissions)

@register.filter
def has_group(user, group_name):
    """Check if user belongs to specific group"""
    if not user or not user.is_authenticated:
        return False
    
    if user.is_superuser:
        return True
        
    try:
        return user.groups.filter(name=group_name).exists()
    except:
        return False