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

    Actions (order is important to avoid signals flipping flags back):
      1) Scrub Profile (if present) and save it FIRST.
      2) Disable access flags on the User (is_active=False, is_staff=False);
         drop superuser if actor is not a superuser.
      3) Anonymize identity fields (username, email, first_name, last_name).
      4) Save the User, then clear direct auth assignments (groups, permissions).
      5) Invalidate all sessions for the user.
      6) Audit log entry.

    We DO NOT change role/ACL fields; we only scrub PII-like fields.
    """
    # --- (1) Scrub Profile FIRST so any post_save signals run before we finalize user flags ---
    try:
        profile = getattr(user, "profile", None)
        if profile:
            # Best-effort PII scrub (touch only common, non-ACL fields if they exist)
            for attr in (
                "phone",
                "address",
                "department",
                "branch",
                "employee_id",
                "manager_override_email",
                "cc_override_emails",
            ):
                if hasattr(profile, attr):
                    setattr(profile, attr, "")

            if hasattr(profile, "team_leader"):
                try:
                    profile.team_leader = None
                except Exception:
                    pass

            if hasattr(profile, "permissions"):
                try:
                    profile.permissions = []
                except Exception:
                    pass

            profile.save()
    except Exception as e:
        logger.warning("Profile scrub (pre-user save) failed for user %s: %s", user.pk, e)

    # --- (2) Disable access flags on the User ---
    user.is_active = False
    user.is_staff = False
    if not getattr(performed_by, "is_superuser", False):
        user.is_superuser = False

    # --- (3) Anonymize identity fields and keep them unique ---
    anon_username = _anonymized_value("deleted", user.pk)
    user.username = anon_username
    user.email = f"{anon_username}@example.invalid"
    user.first_name = "Deleted"
    user.last_name = "User"

    # --- (4) Persist user, then clear M2M auth assignments safely ---
    user.save()
    try:
        user.groups.clear()
        user.user_permissions.clear()
    except Exception as e:
        logger.warning("Clearing groups/permissions failed for user %s: %s", user.pk, e)

    # --- (5) Kill all sessions for this user ---
    _logout_user_everywhere(user)

    # --- (6) Audit log ---
    who = f" by admin {performed_by.pk}" if performed_by else ""
    logger.info("Soft-deleted user %s%s at %s", user.pk, who, datetime.utcnow().isoformat() + "Z")
