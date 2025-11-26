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

    # Re-entrancy guards so we wire things exactly once
    _signals_loaded: bool = False
    _completion_hooks_loaded: bool = False

    def ready(self) -> None:
        """
        Connect signals once and log lightweight sanity info (non-fatal).

        Notes:
        - We only import our own modules here. We never import apps.tasks directly.
        - Completion hooks in apps.leave.signals_tasks rely on the app registry
          (apps.get_model) and don't import task models, so they're safe to call.
        """
        # 1) Core leave signals
        if not self.__class__._signals_loaded:
            try:
                import_module("apps.leave.signals")
                self.__class__._signals_loaded = True
                logger.debug("apps.leave: signals loaded (once).")
            except Exception:
                logger.exception("apps.leave: failed to import signals")

        # 2) Handover completion hooks (notify + stop reminders when a handed-over
        #    task is completed). Uses apps registry; does NOT import apps.tasks.
        if not self.__class__._completion_hooks_loaded:
            try:
                mod = import_module("apps.leave.signals_tasks")
                if hasattr(mod, "connect_all_task_completion_signals"):
                    mod.connect_all_task_completion_signals()
                    self.__class__._completion_hooks_loaded = True
                    logger.debug("apps.leave: completion hooks connected.")
            except Exception:
                logger.debug("apps.leave: completion hooks not connected (ok during migrations).")

        # 3) Base URL (used in emails/links)
        try:
            site_url = (getattr(settings, "SITE_URL", "") or getattr(settings, "SITE_BASE_URL", "")).strip()
        except Exception:
            site_url = ""
        if not site_url:
            logger.info("apps.leave: SITE_URL not set; approval links will default to http://localhost:8000")

        # 4) Routing file used by recipients_for_leave()
        routing_file = getattr(settings, "LEAVE_ROUTING_FILE", "apps/users/data/leave_routing.json")
        logger.debug("apps.leave: using routing map at %s", routing_file)

        # 5) Token config (used for one-click approve/reject)
        token_salt = getattr(settings, "LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
        token_age = getattr(settings, "LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)
        logger.debug(
            "apps.leave: token salt=%s, max_age=%ss",
            "custom" if token_salt != "leave-action-v1" else "default",
            token_age,
        )

        # 6) Feature flags
        try:
            features = getattr(settings, "FEATURES", {})
            if not features or not features.get("EMAIL_NOTIFICATIONS", True):
                logger.info("apps.leave: EMAIL_NOTIFICATIONS is disabled; leave emails will not be sent.")
        except Exception:
            # Settings may be in flux during certain management commands
            pass

        # 7) Light-touch presence check for handover service (optional)
        try:
            import_module("apps.leave.services.task_handover")
            logger.debug("apps.leave: task_handover service loaded.")
        except Exception:
            logger.debug("apps.leave: task_handover service not available yet (ok during migrations).")
