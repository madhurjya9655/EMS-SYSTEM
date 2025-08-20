from __future__ import annotations

from functools import wraps
from typing import Iterable, Optional, Sequence, Set, Union

from django.contrib.auth.views import redirect_to_login
from django.core.exceptions import PermissionDenied

from .permissions import _extract_perms


def _normalize_group_names(groups: Optional[Union[str, Iterable[str]]]) -> Set[str]:
    if not groups:
        return set()
    if isinstance(groups, str):
        return {groups.strip()} if groups.strip() else set()
    return {str(g).strip() for g in groups if str(g).strip()}


def _has_required(
    user,
    *,
    code: Optional[str] = None,
    any_codes: Optional[Sequence[str]] = None,
    all_codes: Optional[Sequence[str]] = None,
    groups: Optional[Union[str, Iterable[str]]] = None,
) -> bool:
    """
    Core checker shared by decorators:
      - Superuser: allow.
      - If 'groups' provided and user is in any of those groups: allow.
      - If 'code' provided: require that single code.
      - If 'any_codes' provided: require any of them.
      - If 'all_codes' provided: require all of them.
      - If nothing provided: allow.
    """
    # Superuser bypass
    if getattr(user, "is_superuser", False):
        return True

    # Optional group bypass (e.g., Manager/HR shortcuts)
    wanted_groups = _normalize_group_names(groups)
    if wanted_groups:
        if user.groups.filter(name__in=wanted_groups).exists():
            return True

    # Pull JSON-backed permission codes from profile
    user_perms: Set[str] = _extract_perms(user)

    if code:
        return code in user_perms

    if any_codes:
        any_set = {str(c).strip() for c in any_codes if str(c).strip()}
        return bool(user_perms & any_set)

    if all_codes:
        all_set = {str(c).strip() for c in all_codes if str(c).strip()}
        return all_set.issubset(user_perms)

    # No constraints specified
    return True


def _guard(view_func, *, code=None, any_codes=None, all_codes=None, groups=None):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        user = getattr(request, "user", None)
        if not getattr(user, "is_authenticated", False):
            return redirect_to_login(request.get_full_path())

        if _has_required(user, code=code, any_codes=any_codes, all_codes=all_codes, groups=groups):
            return view_func(request, *args, **kwargs)

        raise PermissionDenied

    return _wrapped


# -----------------------------------------------------------------------------
# Backwards-compatible decorator (keeps your existing usage)
# -----------------------------------------------------------------------------
def has_permission(perm_code: str, group: Optional[Union[str, Iterable[str]]] = None):
    """
    Function-view decorator.

    Examples:
      @has_permission("add_ticket")
      @has_permission("list_delegation", group="Manager")
    """
    return lambda view: _guard(view, code=perm_code, groups=group)


# -----------------------------------------------------------------------------
# New flexible decorators
# -----------------------------------------------------------------------------
def permission_any(*perm_codes: str, groups: Optional[Union[str, Iterable[str]]] = None):
    """
    Allow if user has ANY of the given permission codes, or is in any 'groups'.

    Example:
      @permission_any("add_ticket", "list_all_tickets")
    """
    return lambda view: _guard(view, any_codes=perm_codes, groups=groups)


def permission_all(*perm_codes: str, groups: Optional[Union[str, Iterable[str]]] = None):
    """
    Allow if user has ALL of the given permission codes, or is in any 'groups'.

    Example:
      @permission_all("add_delegation", "list_delegation", groups=("Manager", "HR"))
    """
    return lambda view: _guard(view, all_codes=perm_codes, groups=groups)
