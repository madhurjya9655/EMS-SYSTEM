# apps/leave/utils/email.py
from __future__ import annotations

import logging
from typing import List

from django.conf import settings

logger = logging.getLogger(__name__)

# We route all emails through the consolidated service layer.
try:
    from apps.leave.services.notifications import (
        send_leave_request_email as _send_leave_request_email,
        send_leave_decision_email as _send_leave_decision_email,
        send_handover_email as _send_handover_email,
    )
    _SERVICE_AVAILABLE = True
except Exception:  # pragma: no cover
    _SERVICE_AVAILABLE = False
    logger.exception("apps.leave.services.notifications is not available; email shims disabled.")


def _email_feature_enabled() -> bool:
    """
    Global feature flag gate (defaults to True).
    Preserved for backwards compatibility with callers of this shim.
    """
    try:
        return bool(getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True))
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Back-compat API
# ---------------------------------------------------------------------------
def send_leave_applied_email(leave) -> None:
    """
    Legacy entry-point used across the codebase.

    New behavior:
      - Delegates to services.notifications.send_leave_request_email(leave, manager_email=None, cc_list=None)
      - The service resolves routing (RP + admin CC + user-selected CC) and
        sends a single request email to the RP (TO) with CC recipients.
      - One-click token links are included for the RP (TO). CCs get a copy without tokens.
    """
    if not _email_feature_enabled():
        return
    if not _SERVICE_AVAILABLE:
        logger.warning("Email suppressed (service layer unavailable).")
        return

    try:
        # Let the service resolve routing and CCs internally.
        _send_leave_request_email(leave)
    except Exception:
        logger.exception("send_leave_applied_email failed for Leave #%s", getattr(leave, "id", "?"))


def send_leave_decision_email(leave) -> None:
    """
    Legacy entry-point to notify the employee after APPROVE/REJECT.
    Delegates to services.notifications.send_leave_decision_email(leave).
    """
    if not _email_feature_enabled():
        return
    if not _SERVICE_AVAILABLE:
        logger.warning("Decision email suppressed (service layer unavailable).")
        return

    try:
        _send_leave_decision_email(leave)
    except Exception:
        logger.exception("send_leave_decision_email failed for Leave #%s", getattr(leave, "id", "?"))


# ---------------------------------------------------------------------------
# Optional addition: direct handover email (useful if legacy code calls this)
# ---------------------------------------------------------------------------
def send_handover_email(leave, assignee, handovers: List) -> None:
    """
    Convenience shim to send handover notifications directly.
    Prefer letting the leave apply flow trigger this automatically.
    """
    if not _email_feature_enabled():
        return
    if not _SERVICE_AVAILABLE:
        logger.warning("Handover email suppressed (service layer unavailable).")
        return

    try:
        _send_handover_email(leave, assignee, handovers)
    except Exception:
        logger.exception(
            "send_handover_email failed for Leave #%s (assignee=%s)",
            getattr(leave, "id", "?"),
            getattr(assignee, "id", "?"),
        )
