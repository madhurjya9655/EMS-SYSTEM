from __future__ import annotations

from typing import Iterable, Optional, Sequence, Set

from django.core.exceptions import PermissionDenied
from django.contrib.auth.views import redirect_to_login

from .permissions import _extract_perms


class PermissionRequiredMixin:
    """
    Drop-in CBV mixin for JSON-backed, per-user permissions.

    Usage:
      class MyView(PermissionRequiredMixin, View):
          permission_code = "add_ticket"
          # OR:
          permission_any = ("add_ticket", "list_all_tickets")
          # OR:
          permission_all = ("add_ticket", "assigned_to_me")

    Rules:
      - Unauthenticated users are redirected to login.
      - Superusers bypass checks.
      - If `permission_code` is set: require that single code.
      - Else if `permission_any` is set: require *any* of the listed codes.
      - Else if `permission_all` is set: require *all* of the listed codes.
      - If none of the above are set, allow access (acts like no permission needed).
    """

    permission_code: Optional[str] = None
    permission_any: Optional[Sequence[str]] = None
    permission_all: Optional[Sequence[str]] = None

    def _has_required_perms(self, user) -> bool:
        # Superuser always allowed
        if getattr(user, "is_superuser", False):
            return True

        user_perms: Set[str] = _extract_perms(user)

        if self.permission_code:
            return self.permission_code in user_perms

        if self.permission_any:
            wanted_any = {c.strip() for c in self.permission_any if c and str(c).strip()}
            return bool(user_perms & wanted_any)

        if self.permission_all:
            wanted_all = {c.strip() for c in self.permission_all if c and str(c).strip()}
            return wanted_all.issubset(user_perms)

        # No requirement specified: allow
        return True

    def dispatch(self, request, *args, **kwargs):
        user = getattr(request, "user", None)
        if not getattr(user, "is_authenticated", False):
            return redirect_to_login(request.get_full_path())

        if not self._has_required_perms(user):
            raise PermissionDenied

        return super().dispatch(request, *args, **kwargs)
