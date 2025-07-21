from django.core.exceptions import PermissionDenied

class PermissionRequiredMixin:
    permission_code = None

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_superuser:
            return super().dispatch(request, *args, **kwargs)
        try:
            profile = request.user.profile
        except Exception:
            raise PermissionDenied
        perms = getattr(profile, 'permissions', None) or []
        if self.permission_code not in perms:
            raise PermissionDenied
        return super().dispatch(request, *args, **kwargs)
