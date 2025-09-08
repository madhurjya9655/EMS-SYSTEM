# apps/users/signals.py
from __future__ import annotations

import logging
from typing import Optional

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model

from .models import Profile

logger = logging.getLogger(__name__)
User = get_user_model()


def _safe_mark_staff(user: Optional[User]) -> None:
    """
    EXACT behavior:
      • If Profile.role == "Admin" → ensure user.is_staff = True.
      • No other changes are performed here (we do NOT toggle is_superuser,
        and we do NOT auto-demote staff if role changes away from Admin).
    """
    if not user:
        return
    if not getattr(user, "pk", None):
        return

    # Elevate to staff if needed
    if not user.is_staff:
        try:
            user.is_staff = True
            user.save(update_fields=["is_staff"])
            logger.info("Marked user %s as staff due to Admin role in Profile.", user.pk)
        except Exception:
            logger.exception("Failed to mark user %s as staff for Admin role.", user.pk)


@receiver(post_save, sender=Profile)
def ensure_admin_role_marks_staff(sender, instance: Profile, created: bool, **kwargs):
    """
    On every Profile save:
      • If role is 'Admin' → guarantee linked User is staff.
      • This is idempotent and safe. It complements form-level logic so
        that saves done elsewhere (e.g., admin, scripts) still behave correctly.
    """
    try:
        if instance.role == "Admin" and getattr(instance, "user", None):
            _safe_mark_staff(instance.user)
    except Exception:
        logger.exception("Error while syncing staff flag from Profile role.")
