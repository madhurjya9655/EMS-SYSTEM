# apps/users/decorators.py

from functools import wraps
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied

def has_permission(perm_code):
    """
    @has_permission('tasks_list_checklist')
    def list_checklist(request): …
    """
    def decorator(view_func):
        @login_required
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            user = request.user

            # superusers bypass
            if user.is_superuser:
                return view_func(request, *args, **kwargs)

            # must have a profile with a permissions list
            try:
                profile = user.profile
            except Exception:
                raise PermissionDenied

            perms = getattr(profile, 'permissions', None) or []
            if perm_code in perms:
                return view_func(request, *args, **kwargs)

            # otherwise 403
            raise PermissionDenied

        return _wrapped
    return decorator
