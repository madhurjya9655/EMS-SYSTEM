from __future__ import annotations

from django import template

# Reuse the single source of truth for permission extraction
# (reads Profile.permissions and returns a set of codes).
from apps.users.permissions import _extract_perms

register = template.Library()


@register.filter(name="has_permission")
def has_permission(user, code: str) -> bool:
    """
    Template usage:
      {% if user|has_permission:'add_ticket' %} ... {% endif %}

    Rules:
      - Anonymous -> False
      - Superuser -> True
      - Else -> True if `code` in Profile.permissions (JSON list of codes)
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if not code:
        return False
    return code in _extract_perms(user)


@register.filter(name="has_any_permission")
def has_any_permission(user, csv_codes: str) -> bool:
    """
    Check if user has ANY of the provided permission codes (comma-separated).

    Example:
      {% if user|has_any_permission:"add_ticket,list_all_tickets" %} ... {% endif %}
    """
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True

    wanted = {c.strip() for c in (csv_codes or "").split(",") if c.strip()}
    return bool(_extract_perms(user) & wanted)
