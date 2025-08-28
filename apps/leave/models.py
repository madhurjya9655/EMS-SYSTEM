from __future__ import annotations

import os
import logging
from typing import Optional
from datetime import datetime, time, timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver
from django.utils import timezone
from django.utils.text import slugify

logger = logging.getLogger(__name__)
User = get_user_model()


def leave_attachment_upload_to(instance: "LeaveRequest", filename: str) -> str:
    """
    MEDIA path: leave_attachments/<user>/<YYYY>/<MM>/<slugified-filename.ext>
    """
    base, ext = os.path.splitext(filename or "")
    try:
        user_part = instance.employee.username or instance.employee.email.split("@")[0]
    except Exception:
        user_part = "user"
    user_part = slugify(user_part) or "user"
    now = timezone.localtime(timezone.now())
    safe_name = slugify(base) or "attachment"
    return f"leave_attachments/{user_part}/{now:%Y}/{now:%m}/{safe_name}{ext.lower()}"


class LeaveType(models.Model):
    name = models.CharField(max_length=50, unique=True)
    default_days = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class LeaveStatus(models.TextChoices):
    PENDING = "PENDING", "Pending"
    APPROVED = "APPROVED", "Approved"
    REJECTED = "REJECTED", "Rejected"


class LeaveRequest(models.Model):
    # Core relations
    employee = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="leave_requests")
    manager = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="managed_leave_requests"
    )  # snapshot at apply
    leave_type = models.ForeignKey(LeaveType, on_delete=models.PROTECT, related_name="leave_requests")

    # Period (DateTime, tz-aware)
    start_at = models.DateTimeField()
    end_at = models.DateTimeField()

    # Additional fields
    is_half_day = models.BooleanField(default=False)
    reason = models.TextField()
    attachment = models.FileField(upload_to=leave_attachment_upload_to, null=True, blank=True)

    # Status & decision
    status = models.CharField(max_length=16, choices=LeaveStatus.choices, default=LeaveStatus.PENDING)
    approver = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="approved_leaves"
    )
    decided_at = models.DateTimeField(null=True, blank=True)
    decision_comment = models.TextField(blank=True)

    # Snapshots (from sheet/profile at time of apply)
    employee_name = models.CharField(max_length=150, blank=True)
    employee_email = models.EmailField(blank=True)
    employee_designation = models.CharField(max_length=150, blank=True)

    # Timestamps
    applied_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # --- LEGACY FIELDS (kept temporarily for safe backfill; do not use) ---
    # We will drop these after backfilling start_at/end_at for all rows.
    start_date = models.DateField(null=True, blank=True, editable=False)
    end_date = models.DateField(null=True, blank=True, editable=False)
    # ----------------------------------------------------------------------

    class Meta:
        ordering = ["-applied_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["employee"]),
            models.Index(fields=["manager"]),
            models.Index(fields=["start_at"]),
            models.Index(fields=["end_at"]),
        ]

    def __str__(self) -> str:
        sa = timezone.localtime(self.start_at) if self.start_at else None
        ea = timezone.localtime(self.end_at) if self.end_at else None
        return f"{self.employee} • {self.leave_type} • {sa:%Y-%m-%d %H:%M} → {ea:%Y-%m-%d %H:%M} • {self.get_status_display()}"

    # ----------------------------
    # Validation & helpers
    # ----------------------------
    @property
    def is_decided(self) -> bool:
        return self.status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED)

    def clean(self) -> None:
        super().clean()

        # Require both datetimes
        if not self.start_at or not self.end_at:
            raise ValidationError({"start_at": "Start and End datetime are required.", "end_at": " "})

        # Must be tz-aware
        if timezone.is_naive(self.start_at) or timezone.is_naive(self.end_at):
            raise ValidationError("Datetimes must be timezone-aware (USE_TZ=True).")

        # end > start
        if self.end_at <= self.start_at:
            raise ValidationError({"end_at": "End must be after Start."})

        # Half-day implies same calendar date unless explicit time span is short
        if self.is_half_day:
            if self.start_at.date() != self.end_at.date():
                raise ValidationError({"is_half_day": "Half-day must start and end on the same date."})
            # guard for unusually long 'half day'
            if (self.end_at - self.start_at) > timedelta(hours=6, minutes=0):
                raise ValidationError({"is_half_day": "Half-day duration should be ≤ 6 hours."})

        # Decision lock: once approved/rejected, cannot change status
        if self.pk:
            try:
                prev = LeaveRequest.objects.only("status").get(pk=self.pk)
                if prev.status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED) and self.status != prev.status:
                    raise ValidationError("This leave has already been decided; status cannot change.")
            except LeaveRequest.DoesNotExist:
                pass

    def save(self, *args, **kwargs) -> None:
        is_new = self.pk is None

        # Snapshot & defaults on first save
        if is_new:
            self.status = self.status or LeaveStatus.PENDING
            # Snapshot employee details
            self._snapshot_employee_details()
            # Auto-pick manager if not explicitly set
            if not self.manager:
                self.manager = self._pick_default_manager()

        # Ensure valid before persist
        self.full_clean()
        super().save(*args, **kwargs)

    # ---- internal helpers ----

    def _snapshot_employee_details(self) -> None:
        """Snapshot name/email/designation at time of apply. Data source: Profile/Sheet import."""
        user: User = self.employee
        self.employee_email = (user.email or "").strip()
        full_name = (getattr(user, "get_full_name", lambda: "")() or "").strip()
        self.employee_name = full_name or (user.first_name and f"{user.first_name} {user.last_name}".strip()) or user.username

        designation = ""
        # Try to read from Profile if app exists
        try:
            from apps.users.models import Profile  # type: ignore
            prof = Profile.objects.select_related("team_leader").filter(user=user).first()
            if prof and getattr(prof, "designation", None):
                designation = (prof.designation or "").strip()
            # Prefer profile.team_leader as default manager if not set
            if not self.manager and prof and getattr(prof, "team_leader_id", None):
                self.manager = prof.team_leader
        except Exception:
            pass
        self.employee_designation = designation

    def _pick_default_manager(self) -> Optional[User]:
        """Resolve a manager: profile.team_leader → any 'Manager' group user → any superuser."""
        # Profile.team_leader
        try:
            from apps.users.models import Profile  # type: ignore
            prof = Profile.objects.select_related("team_leader").filter(user=self.employee).first()
            if prof and getattr(prof, "team_leader_id", None):
                return prof.team_leader
        except Exception:
            pass

        # Any Manager group user (prefer active, not self)
        try:
            mgr_group = Group.objects.get(name__iexact="Manager")
            candidate = (
                User.objects.filter(groups=mgr_group, is_active=True)
                .exclude(pk=getattr(self.employee, "pk", None))
                .order_by("date_joined")
                .first()
            )
            if candidate:
                return candidate
        except Group.DoesNotExist:
            pass
        except Exception:
            logger.exception("Error finding Manager group user")

        # Any superuser (not self)
        su = (
            User.objects.filter(is_superuser=True, is_active=True)
            .exclude(pk=getattr(self.employee, "pk", None))
            .order_by("date_joined")
            .first()
        )
        return su


# ----------------------------
# Signals: decision lock + notifications + task integration
# ----------------------------

def _get_prev_status(instance: LeaveRequest) -> Optional[str]:
    if not instance.pk:
        return None
    try:
        return LeaveRequest.objects.only("status").get(pk=instance.pk).status
    except LeaveRequest.DoesNotExist:
        return None


@receiver(pre_save, sender=LeaveRequest)
def _leave_pre_save(sender, instance: LeaveRequest, **kwargs):
    # Store previous status on the instance for post_save transition detection
    instance._prev_status = _get_prev_status(instance)

    # Guard: once decided, forbid status change
    if instance._prev_status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED) and instance.status != instance._prev_status:
        raise ValidationError("This leave has already been decided; status cannot change.")


@receiver(post_save, sender=LeaveRequest)
def _leave_post_save(sender, instance: LeaveRequest, created: bool, **kwargs):
    # Applied notification on create
    if created:
        try:
            # Lazy import to avoid circular dependency before utils file is added
            from .utils.email import send_leave_applied_email  # type: ignore
            send_leave_applied_email(instance)
        except Exception:
            logger.exception("Failed to send 'leave applied' email for LeaveRequest id=%s", instance.pk)
        return

    # Decision transition: PENDING → APPROVED/REJECTED
    prev = getattr(instance, "_prev_status", None)
    if prev == LeaveStatus.PENDING and instance.status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED):
        # Ensure decided_at is set if not already
        if not instance.decided_at:
            try:
                instance.decided_at = timezone.now()
                instance.save(update_fields=["decided_at"])
            except Exception:
                logger.exception("Failed to set decided_at for LeaveRequest id=%s", instance.pk)

        # Decision email
        try:
            from .utils.email import send_leave_decision_email  # type: ignore
            send_leave_decision_email(instance)
        except Exception:
            logger.exception("Failed to send 'leave decision' email for LeaveRequest id=%s", instance.pk)

        # Task integration for approved leaves
        if instance.status == LeaveStatus.APPROVED:
            try:
                from .services.tasks_integration import apply_leave_to_tasks  # type: ignore
                apply_leave_to_tasks(instance)
            except Exception:
                logger.exception("Task integration failed for LeaveRequest id=%s", instance.pk)
