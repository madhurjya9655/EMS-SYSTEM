# apps/users/signals.py
from __future__ import annotations

import logging
from typing import Optional

from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import Profile

logger = logging.getLogger(__name__)
User = get_user_model()


# ─────────────────────────────────────────────────────────────────────────────
# Helper: staff promotion for Admin-role profiles
# ─────────────────────────────────────────────────────────────────────────────

def _safe_mark_staff(user: Optional[User]) -> None:
    """
    If Profile.role == 'Admin' → ensure user.is_staff = True.
    No other changes; idempotent.
    """
    if not user or not getattr(user, "pk", None):
        return
    if not user.is_staff:
        try:
            user.is_staff = True
            user.save(update_fields=["is_staff"])
            logger.info("Marked user %s as staff due to Admin role.", user.pk)
        except Exception:
            logger.exception("Failed to mark user %s as staff.", user.pk)


@receiver(post_save, sender=Profile)
def ensure_admin_role_marks_staff(sender, instance: Profile, created: bool, **kwargs):
    """On every Profile save: if role is Admin → guarantee user is staff."""
    try:
        if instance.role == "Admin" and getattr(instance, "user", None):
            _safe_mark_staff(instance.user)
    except Exception:
        logger.exception("Error syncing staff flag from Profile role.")


# ─────────────────────────────────────────────────────────────────────────────
# CRITICAL: Sync Employee.is_active whenever User is saved
# ─────────────────────────────────────────────────────────────────────────────

@receiver(post_save, sender=User)
def sync_employee_active_from_user(sender, instance: User, created: bool, **kwargs):
    """
    Single source of truth enforcement:
      User.is_active  →  Employee.is_active (mirror)

    Fires on every User.save() regardless of caller:
      - toggle_active view
      - soft_delete_user utility
      - admin panel
      - bulk scripts
      - API mutations

    Safe and idempotent — only writes if the value actually changed.
    Import is local to avoid circular import at module load time.
    """
    try:
        # Local import prevents circular: recruitment → users → recruitment
        from apps.recruitment.models import Employee  # noqa: PLC0415

        employee: Optional[Employee] = getattr(instance, "employee_record", None)
        if employee is None:
            # No Employee row linked — nothing to sync
            return

        expected_active = bool(instance.is_active)
        if employee.is_active != expected_active:
            employee.is_active = expected_active
            employee.save(update_fields=["is_active"])
            logger.info(
                "Synced Employee(id=%s).is_active=%s from User(id=%s)",
                employee.pk,
                expected_active,
                instance.pk,
            )
    except Exception:
        logger.exception(
            "Failed to sync Employee.is_active for User(id=%s). "
            "Manual reconciliation may be required.",
            getattr(instance, "pk", "?"),
        )