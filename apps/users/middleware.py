#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\users\middleware.py
import logging

from django.shortcuts import redirect
from django.urls import Resolver404, resolve, reverse

from .permission_urls import PERMISSION_URLS
from .permissions import _user_permission_codes

logger = logging.getLogger(__name__)


TOKEN_APPROVAL_URL_PATTERNS = [
    # KAM visit approval/rejection email links
    # Token itself is the trust mechanism; middleware must allow these through.
    ("kam:visit_batch_approve_link", []),
    ("visit_batch_approve_link", []),
    ("kam:visit_batch_reject_link", []),
    ("visit_batch_reject_link", []),

    # Reimbursement email action link (magic token — no permission required; view handles it)
    ("reimbursement:email_action", []),
    ("email_action", []),

    # Reimbursement manager review (after login redirect from email)
    ("manager_review", ["reimbursement_manager_review", "reimbursement_manager_pending"]),
    ("management_review", ["reimbursement_management_review", "reimbursement_management_pending"]),
    ("finance_verify", ["reimbursement_finance_verify", "reimbursement_finance_review", "reimbursement_finance_pending"]),
    ("finance_review", ["reimbursement_finance_review", "reimbursement_review_finance", "reimbursement_finance_pending"]),
    (
        "request_detail",
        [
            "reimbursement_request_detail",
            "reimbursement_list",
            "reimbursement_apply",
            "reimbursement_manager_pending",
            "reimbursement_finance_pending",
            "reimbursement_admin",
        ],
    ),
]


def _normalize_url_name(resolved) -> str | None:
    """
    Safely normalize a resolved URL match into namespace:url_name form.
    Returns None when url_name is missing.
    """
    if not resolved:
        return None

    raw_url_name = getattr(resolved, "url_name", None)
    if not raw_url_name:
        return None

    namespace = getattr(resolved, "namespace", None)
    if namespace:
        return f"{namespace}:{raw_url_name}"
    return raw_url_name


def _check_token_url_permission(url_name: str | None, user_perms: set) -> tuple[bool, bool]:
    """
    Check if a URL matches a token/approval link pattern and whether the user
    is allowed through at middleware level.

    Returns:
        (is_token_url, is_allowed)
        - is_token_url: True if this URL matches a token pattern
        - is_allowed: True if user has sufficient permission for this URL
    """
    if not url_name:
        return False, False

    # Explicit hard allow for secure KAM token approval URLs.
    # View must perform signed token validation.
    if url_name == "kam:visit_batch_approve_link":
        return True, True

    if url_name == "kam:visit_batch_reject_link":
        return True, True

    for pattern, allowed_codes in TOKEN_APPROVAL_URL_PATTERNS:
        if pattern in url_name:
            if not allowed_codes:
                # Empty allowed list = always allow at middleware level
                # (view handles its own token/auth rules)
                return True, True

            allowed_lower = {c.lower() for c in allowed_codes}
            if user_perms & allowed_lower:
                return True, True

            return True, False

    return False, False


class PermissionDebugMiddleware:
    """
    Middleware to log permission debugging information on 403 errors.

    Enhanced to distinguish between:
    - Middleware-level denials (PermissionEnforcementMiddleware)
    - View-level denials (@require_kam_code, PermissionRequiredMixin, etc.)

    The log line now includes enough context to identify whether the denial
    likely happened in middleware or inside the view/decorator stack.
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
            url_name = _normalize_url_name(resolved)

            if not url_name:
                logger.warning(
                    "Permission denied to %s but resolver returned no url_name | user=%s",
                    path,
                    getattr(user, "username", ""),
                )
                logger.warning("User permissions (all): %s", sorted(user_perms))
                return

            required_perms = [code for code, url in PERMISSION_URLS.items() if url == url_name]

            logger.warning(
                "Permission denied to %s | url_name=%s | user=%s | is_staff=%s | is_superuser=%s",
                path,
                url_name,
                getattr(user, "username", ""),
                getattr(user, "is_staff", False),
                getattr(user, "is_superuser", False),
            )
            logger.warning("Required permissions: %s", required_perms)
            logger.warning(
                "Missing permissions: %s",
                [p for p in required_perms if p.lower() not in user_perms],
            )
            logger.warning("User permissions (all): %s", sorted(user_perms))

            is_token, would_allow = _check_token_url_permission(url_name, user_perms)
            if is_token:
                logger.warning(
                    "TOKEN/APPROVAL URL detected. Middleware would %s this user. "
                    "If 403 came from view, remove view-level permission enforcement and rely on token validation "
                    "for url_name=%s.",
                    "ALLOW" if would_allow else "DENY",
                    url_name,
                )
            elif not required_perms:
                logger.warning(
                    "Could not determine required permissions for URL: %s — "
                    "add it to PERMISSION_URLS in permission_urls.py",
                    url_name,
                )

        except Resolver404:
            logger.warning(
                "Permission denied to unresolvable path %s (user=%s)",
                path,
                getattr(user, "username", ""),
            )
            logger.warning("User permissions: %s", sorted(user_perms))


class PermissionEnforcementMiddleware:
    """
    URL-level permission enforcement using PERMISSION_URLS mapping.

    BEHAVIOUR:
      • Skip /admin/ and /accounts/ (auth/admin handle their own permissions)
      • Unauthenticated users are allowed through here (views handle auth)
      • Superusers bypass all checks
      • Enforce app-level permissions via PERMISSION_URLS mapping
        (any one of the mapped codes suffices)
      • Token/approval link URLs get special treatment via TOKEN_APPROVAL_URL_PATTERNS:
        - Token approval URLs are allowed through middleware and validated in the view.
      • On denial: redirect to dashboard:home
    """

    def __init__(self, get_response):
        self.get_response = get_response

        # Reverse map: url_name -> [permission_codes...]
        self.url_to_perm: dict[str, list[str]] = {}
        for code, url in PERMISSION_URLS.items():
            self.url_to_perm.setdefault(url, []).append(code)

        logger.info(
            "PermissionEnforcementMiddleware initialized. %d URL mappings loaded.",
            len(self.url_to_perm),
        )

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

        # Get user's app-level permissions (lowercase set for fast lookup)
        user_perms = _user_permission_codes(request.user)

        # Special case: * or all grants all permissions
        if {"*", "all"} & user_perms:
            return self.get_response(request)

        # Dashboard and login views are always accessible
        if path in ("/dashboard/", "/dashboard", "/") or path.startswith("/login"):
            return self.get_response(request)

        try:
            resolved = resolve(path)
            url_name = _normalize_url_name(resolved)

            # Step 1: Token/approval link check
            is_token_url, token_allowed = _check_token_url_permission(url_name, user_perms)
            if is_token_url:
                if token_allowed:
                    return self.get_response(request)

                logger.warning(
                    "Token URL access denied at middleware: user=%s url=%s user_perms=%s",
                    getattr(request.user, "username", ""),
                    url_name,
                    sorted(user_perms),
                )
                return redirect(reverse("dashboard:home"))

            # Null-safe fallback: if resolver has no url_name, do not enforce here
            if not url_name:
                return self.get_response(request)

            # Step 2: Normal PERMISSION_URLS enforcement
            if url_name in self.url_to_perm:
                required_perms = self.url_to_perm[url_name]
                has_any = any(perm.lower() in user_perms for perm in required_perms)

                if not has_any:
                    logger.warning(
                        "Access denied: user=%s lacks permission for %s "
                        "(needed one of: %s, has: %s)",
                        getattr(request.user, "username", ""),
                        url_name,
                        required_perms,
                        sorted(user_perms),
                    )
                    return redirect(reverse("dashboard:home"))

            # URL not in PERMISSION_URLS and not a token URL → let view handle it

        except Resolver404:
            # URL not in our routing system, let the view handle it
            pass

        return self.get_response(request)