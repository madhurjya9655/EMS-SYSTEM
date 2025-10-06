# apps/users/templatetags/user_filters.py
from __future__ import annotations
from django import template

register = template.Library()

# -----------------------------
# Group checks (moved here to avoid duplicate 'group_tags' libraries)
# -----------------------------
@register.filter(name="has_group")
def has_group(user, group_name: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if not group_name:
        return False
    try:
        return user.groups.filter(name=group_name).exists()
    except Exception:
        return False


@register.filter(name="has_any_group")
def has_any_group(user, csv_group_names: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if not csv_group_names:
        return False
    groups = [g.strip() for g in str(csv_group_names).split(",") if g.strip()]
    if not groups:
        return False
    try:
        return user.groups.filter(name__in=groups).exists()
    except Exception:
        return False


# -----------------------------
# Permission helpers
# -----------------------------
def _check_business_permission(user, code: str) -> bool:
    try:
        from apps.users import permissions as perm_mod  # type: ignore
    except Exception:
        perm_mod = None

    if perm_mod:
        checker = None
        for fn_name in ("user_has_permission", "has_permission_code"):
            fn = getattr(perm_mod, fn_name, None)
            if callable(fn):
                checker = fn
                break
        if checker:
            try:
                return bool(checker(user, code))
            except Exception:
                return False

    try:
        return bool(user.has_perm(code))
    except Exception:
        return False


@register.filter(name="has_permission")
def has_permission(user, code: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if not code:
        return False
    return _check_business_permission(user, str(code).strip())


@register.filter(name="has_any_permission")
def has_any_permission(user, csv_codes: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if not csv_codes:
        return False
    codes = [c.strip() for c in str(csv_codes).split(",") if c.strip()]
    for c in codes:
        if _check_business_permission(user, c):
            return True
    return False
