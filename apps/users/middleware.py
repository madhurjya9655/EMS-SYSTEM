# apps/users/middleware.py
import logging

from django.shortcuts import redirect
from django.urls import Resolver404, resolve, reverse

from .permission_urls import PERMISSION_URLS
from .permissions import _user_permission_codes

logger = logging.getLogger(__name__)


TOKEN_APPROVAL_URL_PATTERNS = [
    # KAM visit approval/rejection email links.
    # Token itself is the trust mechanism; middleware must allow these through.
    ("kam:visit_batch_approve_link", []),
    ("visit_batch_approve_link", []),
    ("kam:visit_batch_reject_link", []),
    ("visit_batch_reject_link", []),
    ("kam:single_visit_approve_link", []),
    ("single_visit_approve_link", []),
    ("kam:single_visit_reject_link", []),
    ("single_visit_reject_link", []),

    # Reimbursement email action link (magic token — no permission required;
    # view handles its own token/auth rules).
    ("reimbursement:email_action", []),
    ("email_action", []),

    # Reimbursement manager review (after login redirect from email).
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
        - is_token_url : True if this URL matches a token pattern
        - is_allowed   : True if user has sufficient permission for this URL
    """
    if not url_name:
        return False, False

    # Hard-allow all KAM token approval URLs.
    # View must perform signed token validation independently.
    _always_allow = {
        "kam:visit_batch_approve_link",
        "kam:visit_batch_reject_link",
        "kam:single_visit_approve_link",
        "kam:single_visit_reject_link",
    }
    if url_name in _always_allow:
        return True, True

    for pattern, allowed_codes in TOKEN_APPROVAL_URL_PATTERNS:
        if pattern in url_name:
            if not allowed_codes:
                # Empty allowed list = always allow at middleware level.
                return True, True

            allowed_lower = {c.lower() for c in allowed_codes}
            if user_perms & allowed_lower:
                return True, True

            return True, False

    return False, False


class PermissionDebugMiddleware:
    """
    Logs permission debugging information on 403 responses.

    This middleware runs AFTER the view and only logs — it never blocks.
    Its purpose is to help diagnose whether a denial came from middleware
    or from a view decorator (like @require_kam_code).

    IMPORTANT: A warning log here does NOT mean the middleware caused the 403.
    The 403 may have come from the view itself (_is_manager check, decorator, etc.).
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

            # Build the list of codes that map to this URL (for diagnostic only).
            # PERMISSION_URLS is code→url, so invert here.
            required_perms = [
                code for code, mapped_url in PERMISSION_URLS.items()
                if mapped_url == url_name
            ]

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
            logger.warning("User permissions include: %s", sorted(user_perms))

            is_token, would_allow = _check_token_url_permission(url_name, user_perms)
            if is_token:
                logger.warning(
                    "TOKEN/APPROVAL URL detected. Middleware would %s this user. "
                    "If 403 came from view, remove view-level permission enforcement "
                    "and rely on token validation for url_name=%s.",
                    "ALLOW" if would_allow else "DENY",
                    url_name,
                )
            elif not required_perms:
                # URL is not in PERMISSION_URLS at all.
                # The 403 almost certainly came from the VIEW (decorator or inline check),
                # NOT from PermissionEnforcementMiddleware.
                logger.warning(
                    "URL not found in PERMISSION_URLS: %s — "
                    "403 came from view-level check (decorator or _is_manager). "
                    "If this should be enforced at middleware level, add it to permission_urls.py.",
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

    DECISION FLOW (in order — first match wins):
    ──────────────────────────────────────────────
    1.  Path starts with /admin/ or /accounts/  → SKIP (let Django handle)
    2.  User is not authenticated               → SKIP (view's @login_required handles)
    3.  User is superuser                       → ALLOW
    4.  User has wildcard code (* or all)       → ALLOW
    5.  Path is / or /dashboard/               → ALLOW
    6.  URL matches TOKEN_APPROVAL_URL_PATTERNS:
          token_allowed = True                  → ALLOW
          token_allowed = False                 → DENY → redirect dashboard:home
    7.  URL not resolvable (Resolver404)        → ALLOW (let 404 handler run)
    8.  URL not in url_to_perm map              → ALLOW (view handles it)
    9.  User has ANY required code              → ALLOW
    10. User has NONE of the required codes     → DENY → redirect dashboard:home

    KEY RULE (Issue 1 fix context):
    ─────────────────────────────────
    Step 9 uses ANY-of semantics. If required_perms = ['kam_targets'] and user
    has 'kam_targets', has_any = True → ALLOW. The middleware NEVER double-denies
    when the user already has the required permission.

    If a 403 still appears after middleware allows through, it comes from the VIEW
    (e.g. _is_manager() check). Fix that in views.py, not here.
    """

    def __init__(self, get_response):
        self.get_response = get_response

        # Build reverse map: url_name → [code1, code2, ...]
        # Multiple codes pointing to same URL are merged into a list.
        self.url_to_perm: dict[str, list[str]] = {}
        for code, url in PERMISSION_URLS.items():
            self.url_to_perm.setdefault(url, []).append(code)

        logger.info(
            "PermissionEnforcementMiddleware initialized. "
            "%d permission codes, %d unique URLs mapped.",
            len(PERMISSION_URLS),
            len(self.url_to_perm),
        )

    def __call__(self, request):
        path = request.path or ""

        # ── Step 1: Skip admin and auth paths ────────────────────────────
        if path.startswith("/admin/") or path.startswith("/accounts/"):
            return self.get_response(request)

        # ── Step 2: Skip unauthenticated users ───────────────────────────
        if not hasattr(request, "user") or not request.user.is_authenticated:
            return self.get_response(request)

        # ── Step 3: Superuser bypass ─────────────────────────────────────
        if getattr(request.user, "is_superuser", False):
            return self.get_response(request)

        # ── Step 4: Fetch permission codes ───────────────────────────────
        user_perms = _user_permission_codes(request.user)

        # Wildcard → allow everything
        if {"*", "all"} & user_perms:
            return self.get_response(request)

        # ── Step 5: Always-allowed paths ─────────────────────────────────
        if path in ("/dashboard/", "/dashboard", "/") or path.startswith("/login"):
            return self.get_response(request)

        try:
            resolved = resolve(path)
            url_name = _normalize_url_name(resolved)

            # ── Step 6: Token/approval link check ────────────────────────
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

            # ── Step 7 / 8: No url_name or URL not in map → view handles ─
            if not url_name or url_name not in self.url_to_perm:
                return self.get_response(request)

            # ── Step 9 / 10: Check ANY-of required permissions ───────────
            required_perms = self.url_to_perm[url_name]

            # ANY-of semantics: user needs at least one of the listed codes.
            has_any = any(perm.lower() in user_perms for perm in required_perms)

            if has_any:
                # ✅ ALLOW — user has at least one required permission.
                return self.get_response(request)

            # ❌ DENY — user has none of the required permissions.
            logger.warning(
                "Access denied: user=%s lacks permission for %s "
                "(needed one of: %s, has: %s)",
                getattr(request.user, "username", ""),
                url_name,
                required_perms,
                sorted(user_perms),
            )
            return redirect(reverse("dashboard:home"))

        except Resolver404:
            # URL not in routing system → let the 404 handler run.
            pass

        return self.get_response(request)