# apps/leave/apps.py
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

    # Re-entrancy guards so we wire things exactly once per process
    _signals_loaded: bool = False
    _completion_hooks_loaded: bool = False

    def ready(self) -> None:
        """
        Deployment-safe AppConfig.ready()

        - Imports apps.leave.signals exactly once (Option A: all leave signals here).
        - Optionally connects task completion hooks from apps.leave.signals_tasks
          (safe: should use apps.get_model, must not hard-import apps.tasks).
        - Logs lightweight sanity information (never fatal).
        """

        # 1) Core leave signals (single source of truth)
        if not self.__class__._signals_loaded:
            try:
                import_module("apps.leave.signals")
                self.__class__._signals_loaded = True
                logger.debug("apps.leave: signals loaded (once).")
            except Exception:
                # Never break boot because of signals import
                logger.exception("apps.leave: failed to import apps.leave.signals")

        # 2) Optional: task completion hooks (non-fatal if module not present)
        if not self.__class__._completion_hooks_loaded:
            try:
                mod = import_module("apps.leave.signals_tasks")
                connect = getattr(mod, "connect_all_task_completion_signals", None)
                if callable(connect):
                    connect()
                    self.__class__._completion_hooks_loaded = True
                    logger.debug("apps.leave: completion hooks connected.")
            except Exception:
                # During migrations or partial startup this may fail; that's OK.
                logger.debug("apps.leave: completion hooks not connected (ok during migrations/startup).")

        # 3) Base URL (used in emails/links) - log only
        site_url = ""
        try:
            site_url = (getattr(settings, "SITE_URL", "") or getattr(settings, "SITE_BASE_URL", "")).strip()
        except Exception:
            site_url = ""

        if not site_url:
            logger.info("apps.leave: SITE_URL not set; approval links may default to localhost.")

        # 4) Routing file (used by recipients_for_leave()) - log only
        try:
            routing_file = getattr(settings, "LEAVE_ROUTING_FILE", "apps/users/data/leave_routing.json")
            logger.debug("apps.leave: using routing map at %s", routing_file)
        except Exception:
            pass

        # 5) Token config (one-click approve/reject) - log only
        try:
            token_salt = getattr(settings, "LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
            token_age = getattr(settings, "LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)
            logger.debug(
                "apps.leave: token salt=%s, max_age=%ss",
                "custom" if token_salt != "leave-action-v1" else "default",
                token_age,
            )
        except Exception:
            pass

        # 6) Feature flags - log only
        try:
            features = getattr(settings, "FEATURES", {})
            if isinstance(features, dict) and not features.get("EMAIL_NOTIFICATIONS", True):
                logger.info("apps.leave: EMAIL_NOTIFICATIONS disabled; leave emails will not be sent.")
        except Exception:
            pass

        # 7) Optional presence check (non-fatal)
        try:
            import_module("apps.leave.services.task_handover")
            logger.debug("apps.leave: task_handover service loaded.")
        except Exception:
            logger.debug("apps.leave: task_handover service not available yet (ok during migrations).")