from django.shortcuts import render
from functools import wraps
from django.contrib.auth.decorators import login_required

def has_permission(perm_code, group=None):
    def decorator(view_func):
        @login_required
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            user = request.user
            if user.is_superuser:
                return view_func(request, *args, **kwargs)
            if group and user.groups.filter(name=group).exists():
                return view_func(request, *args, **kwargs)
            try:
                profile = user.profile
            except Exception:
                return render(request, 'users/no_permission.html', status=403)
            perms = getattr(profile, 'permissions', None) or []
            if perm_code in perms:
                return view_func(request, *args, **kwargs)
            return render(request, 'users/no_permission.html', status=403)
        return _wrapped
    return decorator
