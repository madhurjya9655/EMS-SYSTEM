from __future__ import annotations

import logging
from importlib import import_module

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class LeaveConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.leave"
    verbose_name = "Leave & Approvals"

    # Re-entrancy guard: prevents recursive/duplicate signal imports
    _signals_loaded: bool = False

    def ready(self) -> None:
        """
        Connect signals once and log lightweight sanity info (non-fatal).

        IMPORTANT:
        - Guarded to avoid recursive imports that can occur when other apps
          import leave modules during their own AppConfig.ready().
        - Never import anything from apps.tasks (or other apps) here.
        """
        # Ensure signal handlers are registered (once)
        if not self.__class__._signals_loaded:
            try:
                import_module("apps.leave.signals")
                self.__class__._signals_loaded = True
                logger.debug("apps.leave: signals loaded (once).")
            except Exception:  # pragma: no cover
                logger.exception("apps.leave: failed to import signals")

        # Base URL (used in emails/links)
        try:
            site_url = (getattr(settings, "SITE_URL", "") or getattr(settings, "SITE_BASE_URL", "")).strip()
        except Exception:
            site_url = ""
        if not site_url:
            logger.info("apps.leave: SITE_URL not set; approval links will default to http://localhost:8000")

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
        try:
            features = getattr(settings, "FEATURES", {})
            if not features or not features.get("EMAIL_NOTIFICATIONS", True):
                logger.info("apps.leave: EMAIL_NOTIFICATIONS is disabled; leave emails will not be sent.")
        except Exception:
            # Settings may be in flux during certain management commands
            pass

        # Light-touch presence check for handover service (optional)
        try:
            # Do not import functions; only module-level availability check.
            import_module("apps.leave.services.task_handover")
            logger.debug("apps.leave: task_handover service loaded.")
        except Exception:  # pragma: no cover
            logger.debug("apps.leave: task_handover service not available yet (ok during migrations).")
