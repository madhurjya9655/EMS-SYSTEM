# apps/users/middleware.py
import logging

from django.shortcuts import redirect
from django.urls import Resolver404, resolve, reverse

from .permission_urls import PERMISSION_URLS
from .permissions import _user_permission_codes

logger = logging.getLogger(__name__)


class PermissionDebugMiddleware:
    """
    Middleware to log permission debugging information on 403 errors.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        if response.status_code == 403 and hasattr(request, "user") and request.user.is_authenticated:
            self._log_permission_debug(request)

        return response

    def _log_permission_debug(self, request):
        user = request.user
        path = request.path
        user_perms = _user_permission_codes(user)

        try:
            resolved = resolve(path)
            url_name = f"{resolved.namespace}:{resolved.url_name}" if resolved.namespace else resolved.url_name

            required_perms = [code for code, url in PERMISSION_URLS.items() if url == url_name]

            logger.warning(
                "Permission denied to %s (user=%s, is_staff=%s, is_superuser=%s)",
                path,
                getattr(user, "username", ""),
                getattr(user, "is_staff", False),
                getattr(user, "is_superuser", False),
            )
            logger.warning("User permissions: %s", sorted(user_perms))

            if required_perms:
                logger.warning("Required permissions (based on URL): %s", required_perms)
                logger.warning("Missing permissions: %s", [p for p in required_perms if p.lower() not in user_perms])
            else:
                logger.warning("Could not determine required permissions for URL: %s", url_name)

        except Resolver404:
            logger.warning("Permission denied to unresolvable path %s (user=%s)", path, getattr(user, "username", ""))
            logger.warning("User permissions: %s", sorted(user_perms))


class PermissionEnforcementMiddleware:
    """
    URL-level permission enforcement using PERMISSION_URLS mapping.

    IMPORTANT CHANGE:
      - Removed the old hard-gate for /kam/* via Django perm `kam.access_kam_module`.
      - KAM now behaves like other modules: access is controlled by Profile.permissions
        and the URL->permission mapping in PERMISSION_URLS.

    Behaviour:
      • Skip /admin/ and /accounts/ (auth/admin handle their own permissions)
      • Unauthenticated users are allowed through here (views handle auth)
      • Superusers bypass all checks
      • Enforce app-level permissions via PERMISSION_URLS mapping
        (any one of the mapped codes suffices)
      • On denial: redirect to dashboard:home (existing pattern)
    """

    def __init__(self, get_response):
        self.get_response = get_response

        # Reverse map: url_name -> [permission_codes...]
        self.url_to_perm = {}
        for code, url in PERMISSION_URLS.items():
            self.url_to_perm.setdefault(url, []).append(code)

        logger.info("PermissionEnforcementMiddleware initialized with URL mappings: %s", self.url_to_perm)

    def __call__(self, request):
        path = request.path or ""

        # Skip middleware for certain paths
        if path.startswith("/admin/") or path.startswith("/accounts/"):
            return self.get_response(request)

        # Skip for non-authenticated users (let view layer handle)
        if not hasattr(request, "user") or not request.user.is_authenticated:
            return self.get_response(request)

        # Superusers bypass all permission checks
        if getattr(request.user, "is_superuser", False):
            return self.get_response(request)

        # Get user's app-level permissions (lowercase)
        user_perms = _user_permission_codes(request.user)

        # Special case: * or all grants all permissions
        if {"*", "all"} & user_perms:
            return self.get_response(request)

        # Dashboard and login views are always accessible
        if (path == "/dashboard/" or path == "/dashboard" or path == "/" or path.startswith("/login")):
            return self.get_response(request)

        # Check if the current URL needs a permission (app-level)
        try:
            resolved = resolve(path)
            url_name = f"{resolved.namespace}:{resolved.url_name}" if resolved.namespace else resolved.url_name

            if url_name in self.url_to_perm:
                required_perms = self.url_to_perm[url_name]
                has_permission = any(perm.lower() in user_perms for perm in required_perms)

                if not has_permission:
                    logger.warning(
                        "Access denied: User %s lacks permission for %s (needed one of: %s, has: %s)",
                        getattr(request.user, "username", ""),
                        url_name,
                        required_perms,
                        sorted(user_perms),
                    )
                    return redirect(reverse("dashboard:home"))

        except Resolver404:
            # URL not in our routing system, let the view handle it
            pass

        return self.get_response(request)

