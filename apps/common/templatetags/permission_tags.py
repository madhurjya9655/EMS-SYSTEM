# Register with: {% load permission_tags %}
from django import template

register = template.Library()


def _split_perms(value):
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if v]
    s = str(value)
    parts = []
    for token in s.replace(",", " ").split():
        t = token.strip()
        if t:
            parts.append(t)
    return parts


# ----------------- Permission filters ----------------- #
@register.filter(name="has_perm")
def has_perm(user, perm_name: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return user.has_perm(str(perm_name))


@register.filter(name="has_permission")  # alias
def has_permission(user, perm_name: str) -> bool:
    return has_perm(user, perm_name)


@register.filter(name="has_any_permission")
@register.filter(name="has_any_permissions")
@register.filter(name="has_any_perm")
@register.filter(name="has_any_perms")
def has_any_permissions(user, perm_names) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    perms = _split_perms(perm_names)
    return any(user.has_perm(p) for p in perms)


@register.filter(name="has_all_permissions")
@register.filter(name="has_all_permission")
@register.filter(name="has_all_perms")
def has_all_permissions(user, perm_names) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    perms = _split_perms(perm_names)
    return all(user.has_perm(p) for p in perms)


# ----------------- Group helpers ----------------- #
@register.filter(name="has_group")
def has_group(user, group_name: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return user.groups.filter(name=str(group_name)).exists()


@register.filter(name="in_groups")
def in_groups(user, group_names) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    groups = _split_perms(group_names)
    return user.groups.filter(name__in=groups).exists()


@register.filter(name="can_approve_leave")
def can_approve_leave(user) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if user.has_perm("leave.change_leaverequest"):
        return True
    return user.groups.filter(name__in=["Manager", "HR", "Admin", "Supervisor"]).exists()


@register.filter(name="can_view_all_leaves")
def can_view_all_leaves(user) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if user.has_perm("leave.view_leaverequest"):
        return True
    return user.groups.filter(name__in=["HR", "Admin"]).exists()


@register.filter(name="is_manager")
def is_manager(user) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return user.groups.filter(name__in=["Manager", "Supervisor"]).exists()


@register.filter(name="is_hr")
def is_hr(user) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return user.groups.filter(name="HR").exists()


@register.filter(name="is_admin")
def is_admin(user) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return bool(getattr(user, "is_superuser", False) or user.groups.filter(name="Admin").exists())


# ----------------- Data getters ----------------- #
@register.simple_tag
def get_user_permissions(user):
    if not user or not getattr(user, "is_authenticated", False):
        return set()
    return user.get_all_permissions()


@register.simple_tag
def get_user_groups(user):
    if not user or not getattr(user, "is_authenticated", False):
        return []
    return list(user.groups.all())


# ----------------- Template helpers ----------------- #
@register.inclusion_tag("common/permission_check.html")
def permission_required(user, permission, fallback_message="You don't have permission to perform this action."):
    ok = bool(user and getattr(user, "is_authenticated", False) and user.has_perm(str(permission)))
    return {"has_permission": ok, "fallback_message": fallback_message}


@register.simple_tag
def check_object_permission(user, permission, obj=None) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return user.has_perm(str(permission), obj) if obj is not None else user.has_perm(str(permission))
