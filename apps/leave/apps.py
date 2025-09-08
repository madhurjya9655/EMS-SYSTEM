# apps/leave/apps.py
from __future__ import annotations

import logging
from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class LeaveConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.leave"
    verbose_name = "Leave & Approvals"

    def ready(self) -> None:
        """
        Connect signals and log lightweight sanity info (non-fatal).
        """
        # Ensure signal handlers are registered
        try:
            from . import signals  # noqa: F401
        except Exception:  # pragma: no cover
            logger.exception("apps.leave: failed to import signals")

        # Base URL (used in emails/links)
        site_url = (getattr(settings, "SITE_URL", "") or getattr(settings, "SITE_BASE_URL", "")).strip()
        if not site_url:
            logger.info(
                "apps.leave: SITE_URL not set; approval links will default to http://localhost:8000"
            )

        # Routing file used by recipients_for_leave()
        routing_file = getattr(settings, "LEAVE_ROUTING_FILE", "apps/users/data/leave_routing.json")
        logger.debug("apps.leave: using routing map at %s", routing_file)

        # Token config (used for one-click approve/reject)
        token_salt = getattr(settings, "LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
        token_age = getattr(settings, "LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)
        logger.debug(
            "apps.leave: token salt=%s, max_age=%ss",
            "custom" if token_salt != "leave-action-v1" else "default",
            token_age,
        )

        # Feature flags
        features = getattr(settings, "FEATURES", {})
        if not features or not features.get("EMAIL_NOTIFICATIONS", True):
            logger.info("apps.leave: EMAIL_NOTIFICATIONS is disabled; leave emails will not be sent.")
