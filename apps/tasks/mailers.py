# apps/tasks/mailers.py
from __future__ import annotations

"""
Thin, backward-compatible wrappers around the unified email senders in
apps.tasks.email_utils.

Why this file exists:
- Older parts of the codebase import task mailers from `tasks.mailers`.
- Email composition / rules live in `email_utils.py`.
- These wrappers build URLs and delegate safely.

Safe to drop in.
"""

import logging

from django.conf import settings
from django.urls import reverse

from .email_utils import (
    send_checklist_assignment_to_user as _send_checklist_assignment_to_user,
    send_delegation_assignment_to_user as _send_delegation_assignment_to_user,
    send_help_ticket_assignment_to_user as _send_help_ticket_assignment_to_user,
    send_checklist_admin_confirmation as _send_checklist_admin_confirmation,
    send_delegation_admin_confirmation as _send_delegation_admin_confirmation,
    send_help_ticket_admin_confirmation as _send_help_ticket_admin_confirmation,
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


def _detail_url_for(viewname: str, pk: int) -> str:
    """Best-effort absolute detail URL using SITE_URL; falls back to '/'."""
    try:
        path = reverse(viewname, args=[pk])
        base = getattr(settings, "SITE_URL", "") or ""
        return f"{base}{path}"
    except Exception:
        return getattr(settings, "SITE_URL", "") or "/"


def send_checklist_assignment_email(task, *, is_recurring: bool = False) -> bool:
    """
    Backward-compatible API.
    Delegates to unified sender.
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
    Delegates to unified sender.
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


def send_help_ticket_assignment_email(ticket) -> bool:
    """
    Backward-compatible Help Ticket assignment mailer.
    Uses detail page URL for help tickets.
    """
    try:
        detail_url = _detail_url_for("tasks:help_ticket_detail", ticket.id)
        _send_help_ticket_assignment_to_user(
            ticket=ticket,
            detail_url=detail_url,
            subject_prefix="Help Ticket Assigned",
        )
        return True
    except Exception as e:
        logger.exception(
            "Failed to send help ticket assignment for id=%s: %s",
            getattr(ticket, "id", "?"),
            e,
        )
        return False


def send_checklist_admin_confirmation_email(task) -> bool:
    try:
        _send_checklist_admin_confirmation(task=task)
        return True
    except Exception as e:
        logger.exception(
            "Failed to send checklist admin confirmation for id=%s: %s",
            getattr(task, "id", "?"),
            e,
        )
        return False


def send_delegation_admin_confirmation_email(task) -> bool:
    try:
        _send_delegation_admin_confirmation(delegation=task)
        return True
    except Exception as e:
        logger.exception(
            "Failed to send delegation admin confirmation for id=%s: %s",
            getattr(task, "id", "?"),
            e,
        )
        return False


def send_help_ticket_admin_confirmation_email(ticket) -> bool:
    try:
        _send_help_ticket_admin_confirmation(ticket=ticket)
        return True
    except Exception as e:
        logger.exception(
            "Failed to send help ticket admin confirmation for id=%s: %s",
            getattr(ticket, "id", "?"),
            e,
        )
        return False