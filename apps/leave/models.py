from __future__ import annotations

import hashlib
import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Iterable, Optional, List, Tuple

import pytz
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils import timezone
from django.utils.text import slugify

logger = logging.getLogger(__name__)
User = get_user_model()

# Single source of truth for all time-gated rules (IST)
IST = pytz.timezone("Asia/Kolkata")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def leave_attachment_upload_to(instance: "LeaveRequest", filename: str) -> str:
    """
    MEDIA path: leave_attachments/<user>/<YYYY>/<MM>/<slugified-filename.ext>
    """
    base, ext = os.path.splitext(filename or "")
    try:
        user_part = instance.employee.username or (
            instance.employee.email.split("@")[0] if instance.employee.email else ""
        )
    except Exception:
        user_part = "user"
    user_part = slugify(user_part) or "user"
    now = timezone.localtime(timezone.now(), IST)
    safe_name = slugify(base) or "attachment"
    return f"leave_attachments/{user_part}/{now:%Y}/{now:%m}/{safe_name}{ext.lower()}"


def now_ist() -> datetime:
    """Return timezone-aware current time in IST."""
    return timezone.localtime(timezone.now(), IST)


def _ist_date(dt: datetime) -> date:
    """Convert any aware/naive dt to its IST calendar date."""
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, IST)
    return dt.astimezone(IST).date()


def _daterange_inclusive(d1: date, d2: date) -> Iterable[date]:
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


# ---------------------------------------------------------------------------
# Admin-controlled routing (used across modules)
# ---------------------------------------------------------------------------

class ApproverMapping(models.Model):
    """
    Only Admin should edit this table.

    Defines the employee -> (reporting_person, cc_person) mapping that is reused in
    Leave approvals, Sales module approvals, and other workflows.
    """
    employee = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="approver_mapping"
    )
    reporting_person = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="reports_for_approval",
        null=True, blank=True
    )
    cc_person = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="cc_for_approval",
        null=True, blank=True
    )
    updated_at = models.DateTimeField(auto_now=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["employee__id"]
        verbose_name = "Approver Mapping"
        verbose_name_plural = "Approver Mappings"

    def __str__(self) -> str:
        return f"{self.employee} → RP:{self.reporting_person} CC:{self.cc_person}"


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------

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


class LeaveRequestQuerySet(models.QuerySet):
    def active_for_blocking(self) -> "LeaveRequestQuerySet":
        """
        Leaves that should block task assignment even before approval.
        Per workflow, both PENDING and APPROVED block; REJECTED does not.
        """
        return self.filter(status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])

    def covering_ist_date(self, d: date) -> "LeaveRequestQuerySet":
        """
        Filter leaves which cover the IST calendar date `d`.
        Uses start_date/end_date snapshots kept in sync by the model.
        """
        return self.filter(start_date__lte=d, end_date__gte=d)


class LeaveRequest(models.Model):
    objects = LeaveRequestQuerySet.as_manager()

    # Relations
    employee = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="leave_requests"
    )

    # Approval routing (snapshotted at apply time from ApproverMapping; Admin controls the mapping)
    reporting_person = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="leave_requests_to_approve",
        help_text="Reporting Person (manager) who must approve.",
    )
    cc_person = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="leave_requests_cc",
        help_text="HR (or other) observer.",
    )

    # user-selected extra CC recipients
    cc_users = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        related_name="leave_requests_cc_user",
        blank=True,
        help_text="Additional CC recipients selected by the employee."
    )

    leave_type = models.ForeignKey(LeaveType, on_delete=models.PROTECT, related_name="leave_requests")

    # Period (tz-aware; end_at is treated as *inclusive* in IST calendar)
    start_at = models.DateTimeField()
    end_at = models.DateTimeField()

    # Flags & details
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

    # Snapshots (at apply time)
    employee_name = models.CharField(max_length=150, blank=True)
    employee_email = models.EmailField(blank=True)
    employee_designation = models.CharField(max_length=150, blank=True)

    # Accounting / reporting helpers (float to support half-day = 0.5)
    blocked_days = models.FloatField(
        default=0.0, help_text="How many calendar days are blocked by this leave (IST)."
    )

    # Timestamps
    applied_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Date-only snapshots for fast queries (always kept in sync)
    start_date = models.DateField(null=True, blank=True, editable=False)
    end_date = models.DateField(null=True, blank=True, editable=False)

    class Meta:
        ordering = ["-applied_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["employee"]),
            models.Index(fields=["reporting_person"]),
            models.Index(fields=["start_at"]),
            models.Index(fields=["end_at"]),
            models.Index(fields=["start_date", "end_date"]),
        ]

    def __str__(self) -> str:
        sa = timezone.localtime(self.start_at, IST) if self.start_at else None
        ea = timezone.localtime(self.end_at, IST) if self.end_at else None
        return f"{self.employee} • {self.leave_type} • {sa:%Y-%m-%d %H:%M} → {ea:%Y-%m-%d %H:%M} • {self.get_status_display()}"

    @property
    def manager(self):
        return self.reporting_person

    @manager.setter
    def manager(self, value):
        self.reporting_person = value

    @property
    def is_decided(self) -> bool:
        return self.status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED)

    @property
    def approved_at(self) -> Optional[datetime]:
        return self.decided_at if self.status == LeaveStatus.APPROVED else None

    @property
    def active_for_blocking(self) -> bool:
        return self.status in (LeaveStatus.PENDING, LeaveStatus.APPROVED)

    def ist_dates(self) -> List[date]:
        if not self.start_at or not self.end_at:
            return []
        s = _ist_date(self.start_at)
        e = _ist_date(self.end_at - timedelta(microseconds=1))
        if s > e:
            s, e = e, s
        return list(_daterange_inclusive(s, e))

    def block_dates(self) -> List[date]:
        return self.ist_dates() if self.active_for_blocking else []

    def includes_ist_date(self, d: date) -> bool:
        return d in set(self.ist_dates())

    @staticmethod
    def resolve_routing_for(user: User) -> Tuple[Optional[User], Optional[User]]:
        try:
            mapping = ApproverMapping.objects.select_related("reporting_person", "cc_person").get(employee=user)
            return mapping.reporting_person, mapping.cc_person
        except ApproverMapping.DoesNotExist:
            return None, None

    def _snapshot_employee_details(self) -> None:
        user: User = self.employee
        self.employee_email = (user.email or "").strip()
        full_name = (getattr(user, "get_full_name", lambda: "")() or "").strip()
        self.employee_name = (
            full_name
            or (getattr(user, "first_name", "") and f"{user.first_name} {getattr(user, 'last_name', '')}".strip())
            or getattr(user, "username", "")
        ).strip()

        designation = ""
        try:
            from apps.users.models import Profile
            prof = (
                Profile.objects.select_related("user")
                .only("designation", "user_id")
                .filter(user=user)
                .first()
            )
            if prof and getattr(prof, "designation", None):
                designation = (prof.designation or "").strip()
        except Exception:
            pass
        self.employee_designation = designation

        rp, cc = self.resolve_routing_for(user)
        if rp:
            self.reporting_person = rp
        if cc:
            self.cc_person = cc

    def _validate_apply_cutoff(self) -> None:
        now = now_ist()
        start_day = _ist_date(self.start_at)

        if start_day < now.date():
            raise ValidationError("You cannot apply for leave for past dates.")

        if start_day == now.date():
            gate_0930 = datetime.combine(now.date(), time(9, 30), tzinfo=IST)
            gate_1000 = datetime.combine(now.date(), time(10, 0), tzinfo=IST)
            if now >= gate_1000:
                raise ValidationError(
                    "You cannot apply for leave after 10:00 AM because 10:00 AM recurring tasks will get assigned automatically."
                )
            if now > gate_0930:
                raise ValidationError("Same-day leaves must be applied before 09:30 AM IST.")

    def _validate_decision_cutoff(self, new_status: str) -> None:
        if new_status not in (LeaveStatus.APPROVED, LeaveStatus.REJECTED):
            return
        now = now_ist()
        today = now.date()
        if self.includes_ist_date(today):
            gate_1000 = datetime.combine(today, time(10, 0), tzinfo=IST)
            if now >= gate_1000:
                raise ValidationError("Approvals/Rejections are locked after 10:00 AM IST for today's leaves.")

    def _recompute_blocked_days(self) -> None:
        days = self.ist_dates()
        if not days:
            self.blocked_days = 0.0
            return
        if self.is_half_day and len(days) == 1:
            self.blocked_days = 0.5
        else:
            self.blocked_days = float(len(days))

    def _snapshot_dates(self) -> None:
        self.start_date = _ist_date(self.start_at) if self.start_at else None
        self.end_date = _ist_date(self.end_at - timedelta(microseconds=1)) if self.end_at else None

    def clean(self) -> None:
        super().clean()

        if not self.start_at or not self.end_at:
            raise ValidationError({"start_at": "Start and End datetime are required.", "end_at": " "})
        if timezone.is_naive(self.start_at) or timezone.is_naive(self.end_at):
            raise ValidationError("Datetimes must be timezone-aware (USE_TZ=True).")

        if self.end_at <= self.start_at:
            raise ValidationError({"end_at": "End must be after Start."})

        if self.is_half_day:
            if _ist_date(self.start_at) != _ist_date(self.end_at):
                raise ValidationError({"is_half_day": "Half-day must start and end on the same calendar date."})
            if (self.end_at - self.start_at) > timedelta(hours=6):
                raise ValidationError({"is_half_day": "Half-day duration should be ≤ 6 hours."})

        if not self.pk:
            self._validate_apply_cutoff()

        if self.status in (LeaveStatus.APPROVED, LeaveStatus.REJECTED):
            self._validate_decision_cutoff(self.status)

    def save(self, *args, **kwargs) -> None:
        is_new = self.pk is None

        if is_new:
            self.status = self.status or LeaveStatus.PENDING
            self._snapshot_employee_details()

        self._snapshot_dates()
        self._recompute_blocked_days()

        self.full_clean()
        super().save(*args, **kwargs)

    def approve(self, by_user: Optional[User], comment: str = "") -> None:
        if self.status != LeaveStatus.PENDING:
            raise ValidationError("Only pending requests can be approved.")
        with transaction.atomic():
            self.status = LeaveStatus.APPROVED
            self.approver = by_user
            self.decided_at = timezone.now()
            self.decision_comment = comment or self.decision_comment
            self.save(update_fields=["status", "approver", "decided_at", "decision_comment", "updated_at"])
            LeaveDecisionAudit.log(self, DecisionAction.APPROVED, decided_by=by_user)
        _safe_send_decision_email(self)

    def reject(self, by_user: Optional[User], comment: str = "") -> None:
        if self.status != LeaveStatus.PENDING:
            raise ValidationError("Only pending requests can be rejected.")
        with transaction.atomic():
            self.status = LeaveStatus.REJECTED
            self.approver = by_user
            self.decided_at = timezone.now()
            self.decision_comment = comment or self.decision_comment
            self.save(update_fields=["status", "approver", "decided_at", "decision_comment", "updated_at"])
            LeaveDecisionAudit.log(self, DecisionAction.REJECTED, decided_by=by_user)
        _safe_send_decision_email(self)

    @staticmethod
    def is_user_blocked_on(user: User, d: date) -> bool:
        return LeaveRequest.objects.active_for_blocking().filter(employee=user).covering_ist_date(d).exists()


# ---------------------------------------------------------------------------
# Handover model (temporary reassignment during leave)
# ---------------------------------------------------------------------------

class HandoverTaskType(models.TextChoices):
    CHECKLIST = "checklist", "Checklist"
    DELEGATION = "delegation", "Delegation"
    HELP_TICKET = "help_ticket", "Help Ticket"


class LeaveHandover(models.Model):
    leave_request = models.ForeignKey(
        LeaveRequest, on_delete=models.CASCADE, related_name="handovers"
    )
    original_assignee = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="handovers_given"
    )
    new_assignee = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="handovers_received"
    )
    task_type = models.CharField(max_length=20, choices=HandoverTaskType.choices)
    original_task_id = models.PositiveIntegerField()
    message = models.TextField(blank=True)

    # nullable to avoid migration prompts and allow seamless deploys
    effective_start_date = models.DateField(null=True, blank=True)
    effective_end_date = models.DateField(null=True, blank=True)

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["task_type", "original_task_id"]),
            models.Index(fields=["new_assignee"]),
            models.Index(fields=["effective_start_date", "effective_end_date"]),
            models.Index(fields=["is_active"]),
        ]
        unique_together = ("task_type", "original_task_id", "leave_request", "new_assignee")

    def __str__(self) -> str:
        return f"Handover({self.task_type}#{self.original_task_id}) {self.original_assignee} → {self.new_assignee} [{self.effective_start_date}..{self.effective_end_date}]"


# ---------------------------------------------------------------------------
# Audits & tokens
# ---------------------------------------------------------------------------

class DecisionAction(models.TextChoices):
    APPLIED = "APPLIED", "Applied"
    APPROVED = "APPROVED", "Approved"
    REJECTED = "REJECTED", "Rejected"
    TOKEN_OPENED = "TOKEN_OPENED", "Token Link Opened"
    TOKEN_APPROVE = "TOKEN_APPROVE", "Token Approve"
    TOKEN_REJECT = "TOKEN_REJECT", "Token Reject"
    EMAIL_SENT = "EMAIL_SENT", "Email Sent"


class LeaveDecisionAudit(models.Model):
    leave = models.ForeignKey(LeaveRequest, on_delete=models.CASCADE, related_name="audits")
    action = models.CharField(max_length=20, choices=DecisionAction.choices)
    decided_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    decided_at = models.DateTimeField(auto_now_add=True)

    token_hash = models.CharField(max_length=64, blank=True, help_text="SHA256 of signed token payload.")
    token_manager_email = models.EmailField(blank=True)
    token_used = models.BooleanField(default=False)

    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    extra = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["leave", "action"]),
            models.Index(fields=["token_hash"]),
        ]

    def __str__(self) -> str:
        who = (self.decided_by.get_username() if self.decided_by else self.token_manager_email) or "system"
        return (
            f"Leave #{self.leave_id} • {self.action} • by {who} @ "
            f"{timezone.localtime(self.decided_at, IST):%Y-%m-%d %H:%M IST}"
        )

    @staticmethod
    def hash_token(token: str) -> str:
        return hashlib.sha256((token or "").encode("utf-8")).hexdigest()

    @classmethod
    def log(cls, leave: LeaveRequest, action: str, decided_by: Optional[User] = None, **extra) -> "LeaveDecisionAudit":
        return cls.objects.create(leave=leave, action=action, decided_by=decided_by, extra=extra or {})


# ---------------------------------------------------------------------------
# Signals (emails + audits)
# ---------------------------------------------------------------------------

def _safe_send_request_email(leave: LeaveRequest) -> None:
    try:
        from apps.leave.services.notifications import send_leave_request_email
        send_leave_request_email(leave)
        LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT)
    except Exception:
        logger.exception("Failed to send leave request email")


def _safe_send_decision_email(leave: LeaveRequest) -> None:
    try:
        from apps.leave.services.notifications import send_leave_decision_email
        send_leave_decision_email(leave)
    except Exception:
        logger.exception("Failed to send leave decision email")


from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender=LeaveRequest)
def _on_leave_created(sender, instance: LeaveRequest, created: bool, **kwargs) -> None:
    if not created:
        return
    LeaveDecisionAudit.log(instance, DecisionAction.APPLIED)
    _safe_send_request_email(instance)
