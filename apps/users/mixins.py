# apps/users/mixins.py
from django.shortcuts import render
from django.core.exceptions import PermissionDenied

class PermissionRequiredMixin:
    permission_code = None

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_superuser:
            return super().dispatch(request, *args, **kwargs)
        try:
            profile = request.user.profile
        except Exception:
            return render(request, 'users/no_permission.html', status=403)
        perms = getattr(profile, 'permissions', None) or []
        if self.permission_code not in perms:
            return render(request, 'users/no_permission.html', status=403)
        return super().dispatch(request, *args, **kwargs)
