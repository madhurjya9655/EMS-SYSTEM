# apps/users/utils.py
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Optional

from django.contrib.auth import get_user_model
from django.contrib.sessions.models import Session
from django.db import transaction
from django.utils.crypto import get_random_string

User = get_user_model()
logger = logging.getLogger(__name__)


def _anonymized_value(prefix: str, user_pk: int) -> str:
    """
    Generate a deterministic-looking, non-reversible pseudo-identifier for a user.
    Result remains unique and readable, e.g. 'deleted_42_f3a1b2c3'.
    """
    salt = get_random_string(8)
    base = f"{prefix}_{user_pk}_{salt}"
    return f"{prefix}_{user_pk}_{hashlib.sha256(base.encode()).hexdigest()[:8]}"


def _logout_user_everywhere(user: User) -> None:
    """
    Remove all active sessions for this user (django.contrib.sessions backend).
    Safe to run even if no sessions exist.
    """
    try:
        for session in Session.objects.all():
            try:
                data = session.get_decoded()
            except Exception:
                # corrupted/unreadable session; skip
                continue
            if str(data.get("_auth_user_id")) == str(user.pk):
                session.delete()
    except Exception as e:
        logger.warning("Failed to purge sessions for user %s: %s", user.pk, e)


@transaction.atomic
def soft_delete_user(user: User, *, performed_by: Optional[User] = None) -> None:
    """
    Soft delete + anonymize a user account.

    Actions:
      1) Disable access (is_active=False) and staff; drop superuser if actor is not a superuser.
      2) Anonymize identity fields (username, email, first_name, last_name) while preserving uniqueness.
      3) Remove groups and user_permissions.
      4) Save user, then scrub common PII on Profile (if present).
      5) Invalidate all sessions for the user.
      6) Audit log entry.

    Idempotent enough for repeated calls; each call re-anonymizes username/email.
    """
    # 1) Disable access
    user.is_active = False
    user.is_staff = False
    if not getattr(performed_by, "is_superuser", False):
        # Prevent non-superusers from leaving a superuser bit set by mistake.
        user.is_superuser = False

    # 2) Anonymize identity fields and keep them unique
    anon_username = _anonymized_value("deleted", user.pk)
    anon_email = f"{anon_username}@example.invalid"

    user.username = anon_username
    user.email = anon_email
    user.first_name = "Deleted"
    user.last_name = "User"

    # 3) Remove direct auth assignments
    user.groups.clear()
    user.user_permissions.clear()

    # 4) Persist the anonymized user first
    user.save()

    # 5) Scrub Profile (if present) — adjust attributes to your actual Profile schema
    try:
        profile = getattr(user, "profile", None)
        if profile:
            for attr in ("phone", "address", "department"):
                if hasattr(profile, attr):
                    setattr(profile, attr, "")
            if hasattr(profile, "permissions"):
                try:
                    profile.permissions = []
                except Exception:
                    # e.g., if it's not a list-like field
                    pass
            profile.save()
    except Exception as e:
        logger.warning("Profile scrub failed for user %s: %s", user.pk, e)

    # 6) Kill all sessions for this user
    _logout_user_everywhere(user)

    # 7) Audit log
    who = f" by admin {performed_by.pk}" if performed_by else ""
    logger.info("Soft-deleted user %s%s at %s", user.pk, who, datetime.utcnow().isoformat() + "Z")
