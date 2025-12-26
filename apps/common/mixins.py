# apps/common/mixins.py
from __future__ import annotations

from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest
from django.shortcuts import redirect
from django.urls import reverse

class ReportingManagerRequiredMixin:
    """
    Reusable mixin for CBVs operating on a LeaveRequest-like object.
    Ensures request.user is the reporting manager or a superuser.
    Expects: self.get_object() -> object with `reporting_person_id`.
    """

    permission_denied_message = "Only the assigned Reporting Person can perform this action."
    next_fallback_view = "leave:manager_pending"

    def _can_manage(self, request: HttpRequest, obj) -> bool:
        u = getattr(request, "user", None)
        if not u or not getattr(u, "is_authenticated", False):
            return False
        if getattr(u, "is_superuser", False):
            return True
        return getattr(obj, "reporting_person_id", None) == getattr(u, "id", None)

    def dispatch(self, request, *args, **kwargs):
        obj = self.get_object()
        if not self._can_manage(request, obj):
            # Prefer UX-friendly redirect with a message
            try:
                messages.error(request, self.permission_denied_message)
                return redirect(self._safe_next(request))
            except Exception:
                # If messaging/redirect not available, fall back to a hard 403
                raise PermissionDenied(self.permission_denied_message)
        return super().dispatch(request, *args, **kwargs)

    def _safe_next(self, request: HttpRequest) -> str:
        nxt = (request.GET.get("next") or request.POST.get("next") or "").strip()
        if nxt.startswith("/"):
            return nxt
        try:
            return reverse(self.next_fallback_view)
        except Exception:
            return "/"
