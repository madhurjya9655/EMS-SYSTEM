# apps/tasks/mailers.py
from __future__ import annotations

"""
Thin, backward-compatible wrappers around the unified email senders in
apps.tasks.email_utils.

Why this file exists:
- Older parts of the codebase import `send_checklist_assignment_email` and
  `send_delegation_assignment_email` from `tasks.mailers`.
- We now centralize all email composition / rules in `email_utils.py`
  (assignee-only, assigner never gets user notices, nice fallbacks, IST times).
- These wrappers simply build the completion URL and delegate.

Safe to drop in.
"""

import logging

from django.conf import settings
from django.urls import reverse

from .email_utils import (
    send_checklist_assignment_to_user as _send_checklist_assignment_to_user,
    send_delegation_assignment_to_user as _send_delegation_assignment_to_user,
)

logger = logging.getLogger(__name__)


def _complete_url_for(viewname: str, pk: int) -> str:
    """Best-effort absolute URL using SITE_URL; falls back to '/'."""
    try:
        path = reverse(viewname, args=[pk])
        base = getattr(settings, "SITE_URL", "") or ""
        return f"{base}{path}"
    except Exception:
        return getattr(settings, "SITE_URL", "") or "/"


def send_checklist_assignment_email(task, *, is_recurring: bool = False) -> bool:
    """
    Backward-compatible API.
    Delegates to unified sender (assignee-only; assigner never emailed).
    """
    try:
        complete_url = _complete_url_for("tasks:complete_checklist", task.id)
        subject_prefix = "Recurring Checklist Generated" if is_recurring else "Checklist Assigned"
        _send_checklist_assignment_to_user(
            task=task,
            complete_url=complete_url,
            subject_prefix=subject_prefix,
        )
        return True
    except Exception as e:
        logger.exception(
            "Failed to send checklist assignment (recurring=%s) for id=%s: %s",
            is_recurring,
            getattr(task, "id", "?"),
            e,
        )
        return False


def send_delegation_assignment_email(task, *, is_recurring: bool = False) -> bool:
    """
    Backward-compatible API.
    Delegates to unified sender (assignee-only; assigner never emailed).
    Note: delegations are one-time in current product rules; `is_recurring`
    is kept only for signature compatibility.
    """
    try:
        complete_url = _complete_url_for("tasks:complete_delegation", task.id)
        _send_delegation_assignment_to_user(
            delegation=task,
            complete_url=complete_url,
            subject_prefix="Delegation Assigned",
        )
        return True
    except Exception as e:
        logger.exception(
            "Failed to send delegation assignment for id=%s: %s",
            getattr(task, "id", "?"),
            e,
        )
        return False
