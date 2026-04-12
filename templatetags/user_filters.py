from django import template

register = template.Library()


def _is_authenticated(user):
    return bool(user and getattr(user, "is_authenticated", False))


def _normalize_permissions(perm_list):
    """
    Accepts:
    - comma-separated string: "add_user,list_users"
    - list/tuple/set of permission names
    Returns a clean list of permission strings.
    """
    if not perm_list:
        return []

    if isinstance(perm_list, (list, tuple, set)):
        return [str(p).strip() for p in perm_list if str(p).strip()]

    return [p.strip() for p in str(perm_list).split(",") if p.strip()]


def _user_all_permissions(user):
    """
    Safely fetch all permissions for the user.
    Returns a set like:
        {"auth.add_user", "users.list_users"}
    """
    if not _is_authenticated(user):
        return set()

    try:
        perms = user.get_all_permissions()
        return perms if perms else set()
    except Exception:
        return set()


def _has_single_permission(user, perm_name, all_perms=None):
    """
    Supports:
    - full permission: "app_label.codename"
    - codename only: "add_user"
    """
    if not _is_authenticated(user):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if not perm_name:
        return False

    perm_name = str(perm_name).strip()
    if not perm_name:
        return False

    # Full permission format: app_label.codename
    if "." in perm_name:
        try:
            return user.has_perm(perm_name)
        except Exception:
            return False

    # Codename-only format: add_user
    if all_perms is None:
        all_perms = _user_all_permissions(user)

    try:
        return any(p.split(".")[-1] == perm_name for p in all_perms)
    except Exception:
        return False


@register.filter(name="has_group")
def has_group(user, group_name):
    """
    Usage:
        {% if user|has_group:"HR" %}
    """
    if not _is_authenticated(user):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if not group_name:
        return False

    group_name = str(group_name).strip()
    if not group_name:
        return False

    try:
        return user.groups.filter(name=group_name).exists()
    except Exception:
        return False


@register.filter(name="has_permission")
def has_permission(user, perm_name):
    """
    Usage:
        {% if user|has_permission:"add_user" %}
        {% if user|has_permission:"app_label.codename" %}
    """
    return _has_single_permission(user, perm_name)


@register.filter(name="has_any_permission")
def has_any_permission(user, perm_list):
    """
    Usage:
        {% if user|has_any_permission:"add_user,list_users,delete_user" %}

    Returns True if user has ANY ONE of the listed permissions.
    """
    if not _is_authenticated(user):
        return False

    if getattr(user, "is_superuser", False):
        return True

    perms = _normalize_permissions(perm_list)
    if not perms:
        return False

    all_perms = _user_all_permissions(user)
    return any(_has_single_permission(user, perm, all_perms=all_perms) for perm in perms)


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
    if not _is_authenticated(user):
        return False

    if getattr(user, "is_superuser", False):
        return True

    perms = _normalize_permissions(perm_list)
    if not perms:
        return False

    all_perms = _user_all_permissions(user)
    return all(_has_single_permission(user, perm, all_perms=all_perms) for perm in perms)