from django import template
from django.contrib.auth import get_user_model
from django.utils.safestring import mark_safe

register = template.Library()
User = get_user_model()


@register.filter
def user_full_name(user):
    """Get user's full name or username"""
    if not user:
        return ''
    return user.get_full_name() or user.username


@register.filter
def user_initials(user):
    """Get user's initials"""
    if not user:
        return ''
    full_name = user.get_full_name()
    if full_name:
        parts = full_name.split()
        if len(parts) >= 2:
            return f"{parts[0][0]}{parts[-1][0]}".upper()
        return full_name[0].upper()
    return user.username[0].upper() if user.username else 'U'


@register.filter
def has_group(user, group_name):
    """Check if user belongs to a group"""
    if not user or not user.is_authenticated:
        return False
    return user.groups.filter(name=group_name).exists()


@register.filter
def has_any_group(user, group_names):
    """Check if user belongs to any of the specified groups"""
    if not user or not user.is_authenticated:
        return False
    group_list = [g.strip() for g in group_names.split(',')]
    return user.groups.filter(name__in=group_list).exists()


@register.filter
def user_avatar_url(user):
    """Get user's avatar URL or default"""
    if not user:
        return '/static/img/default-avatar.png'
    
    # Check if user has profile with avatar
    if hasattr(user, 'profile') and user.profile.avatar:
        return user.profile.avatar.url
    
    # Generate avatar based on initials
    initials = user_initials(user)
    return f"https://ui-avatars.com/api/?name={initials}&background=007bff&color=fff&size=40"


@register.filter
def user_role_display(user):
    """Get user's primary role for display"""
    if not user:
        return 'User'
    
    if user.is_superuser:
        return 'Super Admin'
    
    # Check groups in order of importance
    priority_groups = ['CEO', 'Admin', 'Manager', 'EA', 'Team Lead']
    user_groups = user.groups.values_list('name', flat=True)
    
    for group in priority_groups:
        if group in user_groups:
            return group
    
    return 'Employee'


@register.filter
def user_permission_level(user):
    """Get user's permission level as number"""
    if not user or not user.is_authenticated:
        return 0
    
    if user.is_superuser:
        return 100
    
    group_levels = {
        'CEO': 90,
        'Admin': 80,
        'Manager': 70,
        'EA': 60,
        'Team Lead': 50,
    }
    
    user_groups = user.groups.values_list('name', flat=True)
    max_level = 10  # Default employee level
    
    for group in user_groups:
        level = group_levels.get(group, 10)
        max_level = max(max_level, level)
    
    return max_level


@register.filter
def can_manage_user(current_user, target_user):
    """Check if current user can manage target user"""
    if not current_user or not target_user:
        return False
    
    if current_user.is_superuser:
        return True
    
    current_level = user_permission_level(current_user)
    target_level = user_permission_level(target_user)
    
    return current_level > target_level


@register.inclusion_tag('users/partials/user_card.html')
def user_card(user, show_role=True, show_email=False):
    """Render user card component"""
    return {
        'user': user,
        'show_role': show_role,
        'show_email': show_email,
        'avatar_url': user_avatar_url(user),
        'role': user_role_display(user),
        'initials': user_initials(user),
    }