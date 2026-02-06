# apps/users/middleware.py
import logging
from django.urls import resolve, Resolver404
from django.shortcuts import redirect
from django.urls import reverse

from .permissions import _user_permission_codes, PERMISSION_URLS

logger = logging.getLogger(__name__)


class PermissionDebugMiddleware:
    """
    Middleware to log permission debugging information on 403 errors.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Process request normally
        response = self.get_response(request)

        # Debug 403 Forbidden responses for authenticated users
        if response.status_code == 403 and hasattr(request, 'user') and request.user.is_authenticated:
            self._log_permission_debug(request)

        return response

    def _log_permission_debug(self, request):
        """Log detailed permission information for debugging."""
        user = request.user
        path = request.path
        user_perms = _user_permission_codes(user)

        # Try to determine which permission would be needed
        try:
            resolved = resolve(path)
            url_name = f"{resolved.namespace}:{resolved.url_name}" if resolved.namespace else resolved.url_name

            # Find potential required permissions based on URL patterns
            required_perms = [code for code, url in PERMISSION_URLS.items() if url == url_name]

            logger.warning(
                "Permission denied to %s (user=%s, is_staff=%s, is_superuser=%s)",
                path, user.username, user.is_staff, user.is_superuser
            )
            logger.warning("User permissions: %s", sorted(user_perms))

            if required_perms:
                logger.warning("Required permissions (based on URL): %s", required_perms)
                logger.warning("Missing permissions: %s", [p for p in required_perms if p.lower() not in user_perms])
            else:
                logger.warning("Could not determine required permissions for URL: %s", url_name)

            # Extra hint for KAM hard gate
            if path.startswith("/kam/") and not user.has_perm("kam.access_kam_module"):
                logger.warning("KAM hard gate: user missing Django perm 'kam.access_kam_module'.")

        except Resolver404:
            logger.warning(
                "Permission denied to unresolvable path %s (user=%s)",
                path, user.username
            )
            logger.warning("User permissions: %s", sorted(user_perms))


class PermissionEnforcementMiddleware:
    """
    Middleware to enforce permissions at the URL level, and to hard-gate /kam/*
    routes using the Django auth permission `kam.access_kam_module`.

    Behaviour:
      • Skip /admin/ and /accounts/ (auth/admin handle their own permissions)
      • Unauthenticated users are allowed through here (views handle auth),
        consistent with previous behaviour.
      • Superusers bypass all checks.
      • For /kam/*: require user.has_perm('kam.access_kam_module')
      • Else: enforce app-level permissions via PERMISSION_URLS mapping
              (any one of the mapped codes suffices).
      • On denial: redirect to dashboard:home (your existing pattern).
    """

    KAM_REQUIRED_DJANGO_PERM = "kam.access_kam_module"

    def __init__(self, get_response):
        self.get_response = get_response
        # Create a reverse mapping from URL names to permission codes
        self.url_to_perm = {}
        for code, url in PERMISSION_URLS.items():
            if url not in self.url_to_perm:
                self.url_to_perm[url] = []
            self.url_to_perm[url].append(code)

        logger.info("PermissionEnforcementMiddleware initialized with URL mappings: %s", self.url_to_perm)

    def __call__(self, request):
        path = request.path or ""

        # Skip middleware for certain paths
        if path.startswith('/admin/') or path.startswith('/accounts/'):
            return self.get_response(request)

        # Skip for non-authenticated users (let view layer handle)
        if not hasattr(request, 'user') or not request.user.is_authenticated:
            return self.get_response(request)

        # Superusers bypass all permission checks
        if getattr(request.user, 'is_superuser', False):
            return self.get_response(request)

        # ---------- Hard gate for KAM module (all /kam/* pages) ----------
        if path.startswith("/kam/"):
            if not request.user.has_perm(self.KAM_REQUIRED_DJANGO_PERM):
                logger.warning(
                    "KAM access denied for user=%s path=%s (missing Django perm %s)",
                    request.user.username, path, self.KAM_REQUIRED_DJANGO_PERM
                )
                return redirect(reverse('dashboard:home'))
            # If allowed, carry on (no need to evaluate PERMISSION_URLS)
            return self.get_response(request)
        # -----------------------------------------------------------------

        # Get user's app-level permissions (lowercase)
        user_perms = _user_permission_codes(request.user)

        # Special case: * or all grants all permissions
        if {'*', 'all'} & user_perms:
            return self.get_response(request)

        # Dashboard and login views are always accessible
        if (path == '/dashboard/' or path == '/dashboard'
                or path == '/' or path.startswith('/login')):
            return self.get_response(request)

        # Check if the current URL needs a permission (app-level)
        try:
            resolved = resolve(path)
            url_name = f"{resolved.namespace}:{resolved.url_name}" if resolved.namespace else resolved.url_name

            # If URL requires permission, check it
            if url_name in self.url_to_perm:
                required_perms = self.url_to_perm[url_name]
                # Check if user has any of the required permissions
                has_permission = any(perm.lower() in user_perms for perm in required_perms)

                if not has_permission:
                    logger.warning(
                        "Access denied: User %s lacks permission for %s (needed one of: %s, has: %s)",
                        request.user.username, url_name, required_perms, sorted(user_perms)
                    )
                    # Redirect to dashboard on permission failure
                    return redirect(reverse('dashboard:home'))
                else:
                    logger.debug(
                        "Access granted: User %s has permission for %s (needed one of: %s, has: %s)",
                        request.user.username, url_name, required_perms, sorted(user_perms)
                    )
        except Resolver404:
            # URL not in our routing system, let the view handle it
            pass

        return self.get_response(request)
