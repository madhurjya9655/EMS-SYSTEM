# apps/leave/models.py
from __future__ import annotations

import hashlib
import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Iterable, Optional, List, Tuple

import pytz
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AbstractUser
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils import timezone
from django.utils.text import slugify

logger = logging.getLogger(__name__)
User = get_user_model()

# Typing-only alias (avoids Pylance: "Variable not allowed in type expression")
UserType = AbstractUser

# Single source of truth for all time-gated rules (IST)
IST = pytz.timezone("Asia/Kolkata")

# Working window (used for Half Day guards and Full Day same-day cutoff)
WORK_START_IST = time(9, 30)
WORK_END_IST = time(18, 0)


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

    Defines the employee -> (reporting_person, cc_person/default_cc_users) mapping
    reused in Leave approvals, Sales approvals, and other workflows.
    """
    employee = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="approver_mapping"
    )
    reporting_person = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="reports_for_approval",
        null=True, blank=True
    )
    # Legacy single CC (kept for backward compatibility)
    cc_person = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="cc_for_approval",
        null=True, blank=True
    )
    # NEW: Multiple default CC users per employee (admin-managed)
    default_cc_users = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        blank=True,
        related_name="default_cc_for",
        help_text="Multiple default CC recipients (admin-managed)."
    )
    updated_at = models.DateTimeField(auto_now=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["employee__id"]
        verbose_name = "Approver Mapping"
        verbose_name_plural = "Approver Mappings"

    def __str__(self) -> str:
        # Be defensive if the M2M through-table is missing
        cc_count = 0
        try:
            cc_count = self.default_cc_users.count()
        except Exception:
            cc_count = 0
        return f"{self.employee} → RP:{self.reporting_person} CC:{self.cc_person or '-'} (+{cc_count} more)"

    # Backward-compatible resolver
    @staticmethod
    def resolve_for(user: UserType) -> Tuple[Optional[UserType], Optional[UserType]]:
        """
        Returns (reporting_person, single_cc_person_for_legacy_use)

        Robust against missing M2M table: falls back to cc_person only.
        """
        try:
            m = ApproverMapping.objects.select_related("reporting_person", "cc_person").get(employee=user)
        except ApproverMapping.DoesNotExist:
            return None, None
        except Exception:
            logger.exception("ApproverMapping.resolve_for: failed to load mapping")
            return None, None

        if getattr(m, "cc_person", None):
            return m.reporting_person, m.cc_person

        first_cc = None
        try:
            first_cc = m.default_cc_users.first()
        except Exception:
            first_cc = None

        return m.reporting_person, first_cc

    # New resolver with multiple CCs
    @staticmethod
    def resolve_multi_for(user: UserType) -> Tuple[Optional[UserType], List[UserType]]:
        """
        Returns (reporting_person, list_of_cc_users)

        Robust against missing M2M table: returns [] on failure.
        """
        try:
            m = (ApproverMapping.objects
                 .select_related("reporting_person", "cc_person")
                 .prefetch_related("default_cc_users")
                 .get(employee=user))
        except ApproverMapping.DoesNotExist:
            return None, []
        except Exception:
            logger.exception("ApproverMapping.resolve_multi_for: failed to load mapping")
            return None, []

        ccs: List[UserType] = []
        try:
            ccs = list(m.default_cc_users.all())
        except Exception:
            ccs = []

        try:
            if m.cc_person and all(u.id != m.cc_person_id for u in ccs if hasattr(u, "id")):
                ccs.append(m.cc_person)
        except Exception:
            pass

        return m.reporting_person, ccs


class CCConfiguration(models.Model):
    """
    Admin-controlled CC options for leave applications.
    Only users in this list can be selected as CC recipients.
    """
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        help_text="User who can be selected as CC recipient"
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this user is available for CC selection"
    )
    display_name = models.CharField(
        max_length=200,
        blank=True,
        help_text="Optional display name override"
    )
    department = models.CharField(
        max_length=100,
        blank=True,
        help_text="Department or role for grouping"
    )
    sort_order = models.PositiveIntegerField(
        default=0,
        help_text="Display order (lower numbers first)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["sort_order", "department", "user__first_name", "user__last_name"]
        verbose_name = "CC Configuration"
        verbose_name_plural = "CC Configurations"
        unique_together = ("user",)

    def __str__(self) -> str:
        display = self.display_name or self.user.get_full_name() or self.user.username
        dept = f" ({self.department})" if self.department else ""
        return f"{display}{dept}"

    @property
    def display_label(self) -> str:
        """Get the display label for this CC option"""
        if self.display_name:
            return self.display_name
        return self.user.get_full_name() or self.user.username


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

    def active_today(self) -> "LeaveRequestQuerySet":
        """Get leaves that are active today"""
        today = timezone.now().date()
        return self.active_for_blocking().covering_ist_date(today)


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
    # Legacy single CC snapshot (kept for backward compatibility)
    cc_person = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="leave_requests_cc",
        help_text="HR (or other) observer.",
    )

    # user-selected extra CC recipients (only from admin-configured list)
    cc_users = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        related_name="leave_requests_cc_user",
        blank=True,
        help_text="Additional CC recipients selected by the employee from admin-configured options."
    )

    # IMPORTANT: allow null/blank so Half Day can omit type (form condition)
    leave_type = models.ForeignKey(
        LeaveType,
        on_delete=models.PROTECT,
        related_name="leave_requests",
        null=True,
        blank=True,
    )

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
        lt = self.leave_type.name if self.leave_type else "-"
        return f"{self.employee} • {lt} • {sa:%Y-%m-%d %H:%M} → {ea:%Y-%m-%d %H:%M} • {self.get_status_display()}"

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

    @property
    def is_currently_active(self) -> bool:
        """Check if leave is currently active (today falls within leave period)"""
        if not self.active_for_blocking:
            return False
        today = timezone.now().date()
        return self.includes_ist_date(today)

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
    def resolve_routing_for(user: UserType) -> Tuple[Optional[UserType], Optional[UserType]]:
        # Backward-compatible wrapper
        return ApproverMapping.resolve_for(user)

    @staticmethod
    def resolve_routing_multi_for(user: UserType) -> Tuple[Optional[UserType], List[UserType]]:
        # New multi-cc resolver
        return ApproverMapping.resolve_multi_for(user)

    def _snapshot_employee_details(self) -> None:
        user: UserType = self.employee
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

        # Snapshot manager + legacy single cc for backward compatibility.
        rp, cc_single = self.resolve_routing_for(user)
        if rp:
            self.reporting_person = rp
        if cc_single:
            self.cc_person = cc_single

    def _is_half_window_by_times(self) -> bool:
        """
        Infer 'half-day' from the actual requested window (used during form validation
        where is_half_day flag may not be on the instance yet).
        """
        try:
            same_date = _ist_date(self.start_at) == _ist_date(self.end_at)
            short_enough = (self.end_at - self.start_at) <= timedelta(hours=6)
            return bool(same_date and short_enough)
        except Exception:
            return False

    def _validate_apply_rules(self) -> None:
        """
        Application rules (IST):
        • No past dates.
        • Full Day for *today* must be applied before 09:30 IST.
        • Half Day has no application-time restriction.
        """
        now = now_ist()
        start_day = _ist_date(self.start_at)

        # No past dates at all
        if start_day < now.date():
            raise ValidationError("You cannot apply for leave for past dates.")

        # Enforce same-day FULL-DAY cutoff at 09:30 IST
        is_half = bool(self.is_half_day or self._is_half_window_by_times())
        if not is_half and start_day == now.date():
            if now.time() >= WORK_START_IST:
                raise ValidationError("Full-day leave for today must be applied before 09:30 AM.")

        # No restriction for future days (handled implicitly)

    def _validate_decision_cutoff(self, new_status: str) -> None:
        """
        Manager approval/rejection is allowed ANYTIME per requirement.
        (No-op retained to keep call sites intact.)
        """
        return

    def _recompute_blocked_days(self) -> None:
        days = self.ist_dates()
        if not days:
            self.blocked_days = 0.0
            return
        if (self.is_half_day or self._is_half_window_by_times()) and len(days) == 1:
            self.blocked_days = 0.5
        else:
            self.blocked_days = float(len(days))

    def _snapshot_dates(self) -> None:
        self.start_date = _ist_date(self.start_at) if self.start_at else None
        self.end_date = _ist_date(self.end_at - timedelta(microseconds=1)) if self.end_at else None

    def _validate_no_overlap(self) -> None:
        """
        Disallow overlaps against existing PENDING/APPROVED leaves for the same employee.
        """
        s_date = _ist_date(self.start_at)
        e_date = _ist_date(self.end_at - timedelta(microseconds=1))
        conflict = (
            LeaveRequest.objects
            .filter(employee=self.employee, status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])
            .exclude(pk=self.pk)
            .filter(start_date__lte=e_date, end_date__gte=s_date)
            .exists()
        )
        if conflict:
            raise ValidationError("This leave overlaps with an existing leave request.")

    def clean(self) -> None:
        super().clean()

        if not self.start_at or not self.end_at:
            raise ValidationError({"start_at": "Start and End datetime are required.", "end_at": " "})
        if timezone.is_naive(self.start_at) or timezone.is_naive(self.end_at):
            raise ValidationError("Datetimes must be timezone-aware (USE_TZ=True).")

        if self.end_at <= self.start_at:
            raise ValidationError({"end_at": "End must be after Start."})

        # Treat as half-day if either flag is set OR the window indicates a half span
        is_half = self.is_half_day or self._is_half_window_by_times()

        if is_half:
            if _ist_date(self.start_at) != _ist_date(self.end_at):
                raise ValidationError({"is_half_day": "Half-day must start and end on the same calendar date."})

            # ≤ 6 hours
            if (self.end_at - self.start_at) > timedelta(hours=6):
                raise ValidationError({"is_half_day": "Half-day duration should be ≤ 6 hours."})

            # Must be inside the working window 09:30–18:00 IST
            s_local = timezone.localtime(self.start_at, IST).time()
            e_local = timezone.localtime(self.end_at, IST).time()
            if s_local < WORK_START_IST or e_local > WORK_END_IST:
                raise ValidationError({"is_half_day": "Half-day time must be within 09:30–18:00 IST."})

        # Application rules (includes same-day FULL DAY cutoff)
        if not self.pk:
            self._validate_apply_rules()

        # Disallow overlaps
        self._validate_no_overlap()

        # Managers can approve/reject anytime (no cutoff)
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

        # Update handover effective dates after save
        if not is_new:
            self._update_handover_dates()

    def _update_handover_dates(self) -> None:
        """Update effective dates for all handovers when leave dates change"""
        try:
            handovers = LeaveHandover.objects.filter(leave_request=self)
            for handover in handovers:
                handover.effective_start_date = self.start_date
                handover.effective_end_date = self.end_date
                handover.save(update_fields=["effective_start_date", "effective_end_date"])
        except Exception:
            logger.exception("Failed to update handover dates")

    def approve(self, by_user: Optional[UserType], comment: str = "") -> None:
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

    def reject(self, by_user: Optional[UserType], comment: str = "") -> None:
        if self.status != LeaveStatus.PENDING:
            raise ValidationError("Only pending requests can be rejected.")
        with transaction.atomic():
            self.status = LeaveStatus.REJECTED
            self.approver = by_user
            self.decided_at = timezone.now()
            self.decision_comment = comment or self.decision_comment
            # Deactivate handovers when leave is rejected
            LeaveHandover.objects.filter(leave_request=self).update(is_active=False)
            DelegationReminder.objects.filter(leave_handover__leave_request=self).update(is_active=False)
            self.save(update_fields=["status", "approver", "decided_at", "decision_comment", "updated_at"])
            LeaveDecisionAudit.log(self, DecisionAction.REJECTED, decided_by=by_user)
        _safe_send_decision_email(self)

    @staticmethod
    def is_user_blocked_on(user: UserType, d: date) -> bool:
        return LeaveRequest.objects.active_for_blocking().filter(employee=user).covering_ist_date(d).exists()

    @staticmethod
    def get_user_active_leaves(user: UserType) -> "LeaveRequestQuerySet":
        """Get all currently active leaves for a user"""
        return LeaveRequest.objects.filter(employee=user).active_today()


# ---------------------------------------------------------------------------
# Handover model (temporary reassignment during leave)
# ---------------------------------------------------------------------------

class HandoverTaskType(models.TextChoices):
    CHECKLIST = "checklist", "Checklist"
    DELEGATION = "delegation", "Delegation"
    HELP_TICKET = "help_ticket", "Help Ticket"


class LeaveHandoverQuerySet(models.QuerySet):
    def active_now(self) -> "LeaveHandoverQuerySet":
        """Get handovers that are currently active"""
        today = timezone.now().date()
        return self.filter(
            is_active=True,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
            effective_start_date__lte=today,
            effective_end_date__gte=today
        )

    def for_assignee(self, user: UserType) -> "LeaveHandoverQuerySet":
        """Get handovers assigned to a specific user"""
        return self.filter(new_assignee=user)

    def currently_assigned_to(self, user: UserType) -> "LeaveHandoverQuerySet":
        """Get tasks currently assigned to user due to handovers"""
        return self.active_now().for_assignee(user)

    def expired(self) -> "LeaveHandoverQuerySet":
        """Handovers that should be deactivated because their window is past or leave is not active."""
        today = timezone.now().date()
        return self.filter(
            is_active=True
        ).filter(
            models.Q(effective_end_date__lt=today) |
            ~models.Q(leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])
        )

    def deactivate_expired(self) -> int:
        """
        Deactivate all expired handovers and stop their reminders.
        Returns number of handovers deactivated.
        """
        ids = list(self.expired().values_list("id", flat=True))
        if not ids:
            return 0
        updated = LeaveHandover.objects.filter(id__in=ids, is_active=True).update(is_active=False)
        DelegationReminder.objects.filter(leave_handover_id__in=ids, is_active=True).update(is_active=False)
        return int(updated)


class LeaveHandover(models.Model):
    objects = LeaveHandoverQuerySet.as_manager()

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

    # Effective dates are set from leave request if not already set
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
            models.Index(fields=["new_assignee", "is_active"]),
        ]
        unique_together = ("task_type", "original_task_id", "leave_request", "new_assignee")

    def __str__(self) -> str:
        return f"Handover({self.task_type}#{self.original_task_id}) {self.original_assignee} → {self.new_assignee} [{self.effective_start_date}..{self.effective_end_date}]"

    @property
    def is_currently_active(self) -> bool:
        """Check if this handover is currently active"""
        if not self.is_active:
            return False
        if self.leave_request.status not in [LeaveStatus.PENDING, LeaveStatus.APPROVED]:
            return False
        today = timezone.now().date()
        if self.effective_start_date and today < self.effective_start_date:
            return False
        if self.effective_end_date and today > self.effective_end_date:
            return False
        return True

    def get_task_object(self):
        """Get the actual task object this handover refers to"""
        try:
            if self.task_type == HandoverTaskType.CHECKLIST:
                from apps.tasks.models import Checklist
                return Checklist.objects.get(id=self.original_task_id)
            elif self.task_type == HandoverTaskType.DELEGATION:
                from apps.tasks.models import Delegation
                return Delegation.objects.get(id=self.original_task_id)
            elif self.task_type == HandoverTaskType.HELP_TICKET:
                from apps.tasks.models import HelpTicket
                return HelpTicket.objects.get(id=self.original_task_id)
        except Exception:
            pass
        return None

    def get_task_title(self):
        """Get task title/name for display"""
        task_obj = self.get_task_object()
        if task_obj:
            return getattr(task_obj, "task_name", None) or getattr(task_obj, "title", str(task_obj))
        return f"{self.task_type.title()} #{self.original_task_id}"

    def get_task_url(self):
        """Generate URL to view the task detail"""
        from django.urls import reverse
        try:
            if self.task_type == HandoverTaskType.CHECKLIST:
                return reverse("tasks:edit_checklist", args=[self.original_task_id])
            elif self.task_type == HandoverTaskType.DELEGATION:
                return reverse("tasks:edit_delegation", args=[self.original_task_id])
            elif self.task_type == HandoverTaskType.HELP_TICKET:
                return reverse("tasks:edit_help_ticket", args=[self.original_task_id])
        except Exception:
            pass
        return None

    def save(self, *args, **kwargs):
        # Set effective dates from leave request if not already set
        if not self.effective_start_date and self.leave_request:
            self.effective_start_date = self.leave_request.start_date
        if not self.effective_end_date and self.leave_request:
            self.effective_end_date = self.leave_request.end_date
        super().save(*args, **kwargs)

    def deactivate_if_expired(self) -> bool:
        """
        Deactivate this handover (and its reminders) if it's no longer valid.
        Returns True if it was deactivated.
        """
        if not self.is_active:
            return False
        today = timezone.now().date()
        expired = (
            (self.effective_end_date and self.effective_end_date < today) or
            (self.leave_request and self.leave_request.status not in [LeaveStatus.PENDING, LeaveStatus.APPROVED])
        )
        if expired:
            self.is_active = False
            self.save(update_fields=["is_active", "updated_at"])
            DelegationReminder.objects.filter(leave_handover=self, is_active=True).update(is_active=False)
            return True
        return False


# ---------------------------------------------------------------------------
# Task Dashboard Integration
# ---------------------------------------------------------------------------

class HandoverTaskMixin:
    """Mixin to add handover functionality to task models"""

    @classmethod
    def get_tasks_for_user(cls, user: UserType):
        """Get all tasks for a user including handed over tasks"""
        # Original tasks assigned to user
        original_tasks = cls.objects.filter(assign_to=user)

        # Tasks handed over to user
        handovers = LeaveHandover.objects.currently_assigned_to(user).filter(
            task_type=cls._get_task_type_name()
        )

        # Get handed over task IDs
        handed_over_task_ids = [h.original_task_id for h in handovers]

        # Include handed over tasks
        if handed_over_task_ids:
            handed_over_tasks = cls.objects.filter(id__in=handed_over_task_ids)
            # Combine using union
            return original_tasks.union(handed_over_tasks).order_by("-id")

        return original_tasks

    def get_current_assignee(self):
        """Get the current assignee (considering active handovers)"""
        today = timezone.now().date()

        # Check if there's an active handover for this task
        handover = LeaveHandover.objects.filter(
            task_type=self._get_task_type_name(),
            original_task_id=self.id,
            is_active=True,
            effective_start_date__lte=today,
            effective_end_date__gte=today,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED]
        ).first()

        if handover:
            return handover.new_assignee

        return self.assign_to

    def get_handover_info(self):
        """Get handover information for this task"""
        today = timezone.now().date()

        handover = LeaveHandover.objects.filter(
            task_type=self._get_task_type_name(),
            original_task_id=self.id,
            is_active=True,
            effective_start_date__lte=today,
            effective_end_date__gte=today,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED]
        ).select_related("leave_request", "original_assignee", "new_assignee").first()

        if handover:
            return {
                "is_handed_over": True,
                "original_assignee": handover.original_assignee,
                "current_assignee": handover.new_assignee,
                "handover_message": handover.message,
                "leave_request": handover.leave_request,
                "handover_end_date": handover.effective_end_date,
                "handover": handover,
            }

        return {"is_handed_over": False}

    @classmethod
    def _get_task_type_name(cls):
        """Get the task type name for handovers"""
        if "checklist" in cls.__name__.lower():
            return "checklist"
        elif "delegation" in cls.__name__.lower():
            return "delegation"
        elif "help" in cls.__name__.lower() or "ticket" in cls.__name__.lower():
            return "help_ticket"
        return "unknown"


# ---------------------------------------------------------------------------
# Delegation Reminder System
# ---------------------------------------------------------------------------

class DelegationReminder(models.Model):
    """
    Tracks automatic reminders for delegated tasks until completion.
    """
    leave_handover = models.ForeignKey(
        LeaveHandover, on_delete=models.CASCADE, related_name="reminders"
    )
    interval_days = models.PositiveIntegerField(
        default=2, help_text="Send reminder every N days"
    )
    next_run_at = models.DateTimeField(
        help_text="Next scheduled reminder time"
    )
    is_active = models.BooleanField(
        default=True, help_text="Set to False to stop reminders"
    )
    last_sent_at = models.DateTimeField(null=True, blank=True)
    total_sent = models.PositiveIntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["next_run_at", "is_active"]),
            models.Index(fields=["leave_handover"]),
        ]

    def __str__(self):
        return f"Reminder for {self.leave_handover} (every {self.interval_days}d, sent {self.total_sent}x)"

    def should_send_reminder(self):
        """Check if reminder should be sent now"""
        if not self.is_active:
            return False
        if timezone.now() < self.next_run_at:
            return False

        # Check if handover is still active
        if not self.leave_handover.is_currently_active:
            return False

        # Check if the original task is completed
        task_obj = self.leave_handover.get_task_object()
        if task_obj and hasattr(task_obj, "status"):
            if task_obj.status in ["Completed", "Closed", "Done"]:
                return False

        return True

    def mark_sent(self):
        """Mark reminder as sent and schedule next one"""
        self.last_sent_at = timezone.now()
        self.total_sent += 1
        self.next_run_at = timezone.now() + timedelta(days=self.interval_days)
        self.save(update_fields=["last_sent_at", "total_sent", "next_run_at", "updated_at"])

    def deactivate(self):
        """Stop sending reminders"""
        self.is_active = False
        self.save(update_fields=["is_active", "updated_at"])


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
    HANDOVER_EMAIL_SENT = "HANDOVER_EMAIL_SENT", "Handover Email Sent"
    REMINDER_EMAIL_SENT = "REMINDER_EMAIL_SENT", "Reminder Email Sent"


class LeaveDecisionAudit(models.Model):
    leave = models.ForeignKey(LeaveRequest, on_delete=models.CASCADE, related_name="audits")
    action = models.CharField(max_length=25, choices=DecisionAction.choices)
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
    def log(cls, leave: LeaveRequest, action: str, decided_by: Optional[UserType] = None, **extra) -> "LeaveDecisionAudit":
        return cls.objects.create(leave=leave, action=action, decided_by=decided_by, extra=extra or {})


# ---------------------------------------------------------------------------
# Utility Functions for Dashboard Integration
# ---------------------------------------------------------------------------

def get_handed_over_tasks_for_user(user: UserType) -> dict:
    """Get all tasks handed over to a user, grouped by type"""
    handovers = LeaveHandover.objects.currently_assigned_to(user).select_related(
        "leave_request", "original_assignee"
    )

    result = {
        "checklist": [],
        "delegation": [],
        "help_ticket": [],
    }

    for handover in handovers:
        task_obj = handover.get_task_object()
        if task_obj:
            task_data = {
                "task": task_obj,
                "handover": handover,
                "original_assignee": handover.original_assignee,
                "leave_request": handover.leave_request,
                "handover_message": handover.message,
                "task_url": handover.get_task_url(),
            }
            result[handover.task_type].append(task_data)

    return result


# ---------------------------------------------------------------------------
# Cleanup utility (for scheduler/management command)
# ---------------------------------------------------------------------------

def deactivate_expired_handovers() -> int:
    """
    Deactivate all expired LeaveHandover rows and stop their DelegationReminder rows.
    A handover is considered expired when:
      • effective_end_date is in the past (IST date), or
      • the linked leave is not in PENDING/APPROVED.
    Returns the number of handovers deactivated.
    """
    try:
        count = LeaveHandover.objects.deactivate_expired()
        logger.info("Expired handovers deactivated: %s", count)
        return count
    except Exception:
        logger.exception("Failed during deactivate_expired_handovers()")
        return 0


# ---------------------------------------------------------------------------
# Signals (emails + audits)
# ---------------------------------------------------------------------------

def _collect_admin_cc_emails(employee: UserType) -> List[str]:
    """
    Be resilient if multi-cc persistence is unavailable.
    """
    emails: List[str] = []
    try:
        rp, cc_users = LeaveRequest.resolve_routing_multi_for(employee)
        for u in cc_users:
            if u and getattr(u, "email", None):
                emails.append(u.email)
    except Exception:
        logger.exception("collect_admin_cc_emails: failed to resolve multi-cc")
        emails = []

    # de-duplicate while preserving order
    seen = set()
    ordered = []
    for e in emails:
        e_low = (e or "").strip().lower()
        if e_low and e_low not in seen:
            seen.add(e_low)
            ordered.append(e)
    return ordered


def _safe_send_request_email(leave: LeaveRequest) -> None:
    try:
        from apps.leave.services.notifications import send_leave_request_email
        # Manager
        manager_email = leave.reporting_person.email if (leave.reporting_person and leave.reporting_person.email) else None
        # Admin-managed default CCs (multi) + legacy single via resolver
        admin_cc_list = _collect_admin_cc_emails(leave.employee)
        # User-selected extra CCs
        extra_cc_emails = [u.email for u in leave.cc_users.all() if u.email]
        # Merge + normalize
        all_cc = list({*(e.strip().lower() for e in admin_cc_list + extra_cc_emails)})
        send_leave_request_email(leave, manager_email=manager_email, cc_list=all_cc)
        # ✅ Mark audit with kind="request" so downstream duplicate suppression works reliably
        LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT, kind="request")
    except Exception:
        logger.exception("Failed to send leave request email")


def _safe_send_handover_emails(leave: LeaveRequest) -> None:
    """Send separate handover emails to each assignee"""
    try:
        from apps.leave.services.notifications import send_handover_email
        handovers = LeaveHandover.objects.filter(leave_request=leave).select_related("new_assignee")

        # Group handovers by assignee to send one email per person
        assignee_handovers = {}
        for handover in handovers:
            assignee_id = handover.new_assignee.id
            if assignee_id not in assignee_handovers:
                assignee_handovers[assignee_id] = []
            assignee_handovers[assignee_id].append(handover)

        # Send email to each assignee
        for assignee_id, user_handovers in assignee_handovers.items():
            assignee = user_handovers[0].new_assignee
            send_handover_email(leave, assignee, user_handovers)
            LeaveDecisionAudit.log(leave, DecisionAction.HANDOVER_EMAIL_SENT, extra={"assignee_id": assignee_id})

            # Create delegation reminders if configured
            for handover in user_handovers:
                # Default to 2-day reminders for delegated tasks
                reminder_interval = 2
                next_run = timezone.now() + timedelta(days=reminder_interval)

                DelegationReminder.objects.create(
                    leave_handover=handover,
                    interval_days=reminder_interval,
                    next_run_at=next_run,
                    is_active=True,
                )

    except Exception:
        logger.exception("Failed to send handover emails")


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

    # Defer all side-effects until after the transaction commits to avoid SQLite locks
    def _after_commit():
        try:
            LeaveDecisionAudit.log(instance, DecisionAction.APPLIED)
        except Exception:
            logger.exception("Failed to log APPLIED for leave %s", getattr(instance, "id", None))
        try:
            _safe_send_request_email(instance)
        except Exception:
            logger.exception("Failed to queue/send request email for leave %s", getattr(instance, "id", None))
        try:
            if LeaveHandover.objects.filter(leave_request=instance).exists():
                _safe_send_handover_emails(instance)
        except Exception:
            logger.exception("Failed to queue/send handover emails for leave %s", getattr(instance, "id", None))

    try:
        transaction.on_commit(_after_commit)
    except Exception:
        # Fallback: execute immediately (still better than dropping the event)
        _after_commit()
