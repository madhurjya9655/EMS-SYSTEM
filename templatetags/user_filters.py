from django import template

register = template.Library()


@register.filter(name="has_group")
def has_group(user, group_name):
    """
    Usage:
        {% if user|has_group:"HR" %}
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False

    if not group_name:
        return False

    return user.groups.filter(name=group_name).exists()


@register.filter(name="has_permission")
def has_permission(user, perm_name):
    """
    Usage:
        {% if user|has_permission:"add_user" %}
        {% if user|has_permission:"app_label.codename" %}
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if not perm_name:
        return False

    perm_name = str(perm_name).strip()
    if not perm_name:
        return False

    # If full permission name is passed, use it directly
    if "." in perm_name:
        return user.has_perm(perm_name)

    # If only codename is passed, check against all user permissions
    all_perms = user.get_all_permissions()
    return any(p.split(".")[-1] == perm_name for p in all_perms)


@register.filter(name="has_any_permission")
def has_any_permission(user, perm_list):
    """
    Usage:
        {% if user|has_any_permission:"add_user,list_users,delete_user" %}
    Returns True if user has ANY ONE of the listed permissions.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if not perm_list:
        return False

    perms = [p.strip() for p in str(perm_list).split(",") if p.strip()]
    if not perms:
        return False

    all_perms = user.get_all_permissions()

    for perm in perms:
        if "." in perm:
            if user.has_perm(perm):
                return True
        else:
            if any(p.split(".")[-1] == perm for p in all_perms):
                return True

    return False


@register.filter(name="has_any_permissions")
def has_any_permissions(user, perm_list):
    """
    Alias for has_any_permission
    Usage:
        {% if user|has_any_permissions:"add_user,list_users" %}
    """
    return has_any_permission(user, perm_list)


@register.filter(name="has_all_permissions")
def has_all_permissions(user, perm_list):
    """
    Usage:
        {% if user|has_all_permissions:"add_user,list_users" %}
    Returns True only if user has ALL listed permissions.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if not perm_list:
        return False

    perms = [p.strip() for p in str(perm_list).split(",") if p.strip()]
    if not perms:
        return False

    all_perms = user.get_all_permissions()

    for perm in perms:
        if "." in perm:
            if not user.has_perm(perm):
                return False
        else:
            if not any(p.split(".")[-1] == perm for p in all_perms):
                return False

    return True