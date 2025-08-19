from django import template

register = template.Library()


@register.filter
def has_group(user, group_name):
    """Check if user belongs to a group"""
    if not user or not user.is_authenticated:
        return False
    return user.groups.filter(name=group_name).exists()