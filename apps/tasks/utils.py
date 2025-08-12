# apps/tasks/utils.py
"""
Backwards-compatible task utilities.

- Re-exports email helpers so existing imports from `.utils` keep working.
- Adds a helper to preserve the FIRST occurrence datetime exactly as entered
  (manual or bulk), interpreting naive datetimes as IST and returning an
  aware datetime in the project's timezone.

If you already import directly from `.email_utils`, that continues to work.
"""

from typing import Iterable, List, Sequence, Optional, Dict, Any
from django.utils import timezone

# Re-export email helpers for compatibility
from .email_utils import (  # noqa: F401
    _dedupe_emails,
    get_admin_emails,
    send_html_email,
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
    send_checklist_unassigned_notice,
    send_delegation_assignment_to_user,
    send_help_ticket_assignment_to_user,
    send_help_ticket_admin_confirmation,
    send_help_ticket_unassigned_notice,
    send_admin_bulk_summary,
)

# Import the first-occurrence keeper from recurrence
from .recurrence import keep_first_occurrence as _keep_first_occurrence  # noqa: F401


def preserve_first_occurrence_datetime(dt):
    """
    FIRST OCCURRENCE ONLY:
    Keep EXACT date & time as provided by admin or bulk upload.

    Behavior:
    - If `dt` is naive -> interpret as IST wall clock.
    - Convert to the project's timezone (aware).
    - Do NOT move off Sundays/holidays (business rule: only future recurrences
      normalize to 10:00 AM IST and skip non-working days).

    Usage:
        planned_dt = preserve_first_occurrence_datetime(planned_dt)
    """
    return _keep_first_occurrence(dt)


__all__ = [
    # email helpers
    "_dedupe_emails",
    "get_admin_emails",
    "send_html_email",
    "send_checklist_assignment_to_user",
    "send_checklist_admin_confirmation",
    "send_checklist_unassigned_notice",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
    "send_help_ticket_admin_confirmation",
    "send_help_ticket_unassigned_notice",
    "send_admin_bulk_summary",
    # first occurrence helper
    "preserve_first_occurrence_datetime",
]
