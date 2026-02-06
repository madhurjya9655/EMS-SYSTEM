# apps/users/templatetags/user_filters.py
from __future__ import annotations

from typing import Iterable, Optional, Set

from django import template

register = template.Library()


# =============================
# Group checks
# =============================
@register.filter(name="has_group")
def has_group(user, group_name: str) -> bool:
    """
    True if the user belongs to the given Django auth Group.
    """
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
    """
    True if the user belongs to ANY of the comma-separated group names.
    """
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


# =============================
# Permission helpers
# =============================

def _import_perm_module():
    """
    Lazy import to avoid circular imports at Django startup.
    Returns the module or None on failure.
    """
    try:
        from apps.users import permissions as perm_mod  # type: ignore
        return perm_mod
    except Exception:
        return None


def _safe_lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _business_has_permission(user, code: str) -> bool:
    """
    Ask the central permission system first; fall back to Django's auth perms.
    """
    if not code:
        return False

    perm_mod = _import_perm_module()
    if perm_mod:
        # Prefer the exported helpers if present.
        for fn_name in ("user_has_permission", "has_permission_code"):
            fn = getattr(perm_mod, fn_name, None)
            if callable(fn):
                try:
                    return bool(fn(user, code))
                except Exception:
                    # If custom checker misbehaves, fall through to fallback.
                    break

        # Fallback via raw expanded codes (if available)
        try:
            codes: Set[str] = set()
            if hasattr(perm_mod, "_user_permission_codes"):
                codes = set(perm_mod._user_permission_codes(user))  # type: ignore[attr-defined]
            elif hasattr(perm_mod, "_extract_perms"):
                codes = set(perm_mod._extract_perms(user))  # legacy alias
            if codes:
                c = _safe_lower(code)
                # Special cases: our business codes are NOT Django "app_label.codename"
                # so we compare with the flattened business codes.
                return c in codes or "*" in codes or "all" in codes
        except Exception:
            pass

    # Absolute last resort: try Django's built-in permission system
    # (useful when 'code' is a real Django perm like "app_label.codename").
    try:
        return bool(user.has_perm(code))
    except Exception:
        return False


@register.filter(name="has_permission")
def has_permission(user, code: str) -> bool:
    """
    True if the user has the given business permission code
    (or is superuser). Delegates to apps.users.permissions when available.
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    return _business_has_permission(user, str(code).strip())


@register.filter(name="has_any_permission")
def has_any_permission(user, csv_codes: str) -> bool:
    """
    True if the user has ANY of the comma-separated business permission codes
    (or is superuser).
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if not csv_codes:
        return False
    for c in (p.strip() for p in str(csv_codes).split(",") if p.strip()):
        if _business_has_permission(user, c):
            return True
    return False


@register.filter(name="has_module_permission")
def has_module_permission(user, module_name: str) -> bool:
    """
    True if the user has ANY permission within a logical module bucket
    from PERMISSIONS_STRUCTURE. This mirrors the sidebar logic.
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True

    module_name = (module_name or "").strip()
    if not module_name:
        return False

    perm_mod = _import_perm_module()
    if not perm_mod:
        return False

    try:
        structure = getattr(perm_mod, "PERMISSIONS_STRUCTURE", {})
        if module_name not in structure:
            return False

        # Pull user's expanded codes
        if hasattr(perm_mod, "_user_permission_codes"):
            user_codes: Set[str] = set(perm_mod._user_permission_codes(user))  # type: ignore[attr-defined]
        elif hasattr(perm_mod, "_extract_perms"):
            user_codes = set(perm_mod._extract_perms(user))  # legacy alias
        else:
            user_codes = set()

        if {"*", "all"} & user_codes:
            return True

        module_codes = {str(code).strip().lower() for code, _ in structure[module_name]}
        return bool(user_codes & module_codes)
    except Exception:
        return False
