# FILE: apps/tasks/models.py
# PURPOSE: Enforce non-working-day task assignment rules, leave-aware validation,
#          and production-safe checklist lifecycle/delete architecture
# UPDATED: 2026-05-08

from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.timesince import timesince
from django.core.exceptions import ValidationError
from apps.settings.models import Holiday

User = get_user_model()


def is_holiday_or_sunday(date_val):
    """
    Returns True if the given date is Sunday or a configured holiday.
    Accepts date, datetime or anything with .date() / .weekday().
    """
    if not date_val:
        return False

    if hasattr(date_val, "date"):
        try:
            date_val = date_val.date()
        except Exception:
            pass

    return (
        hasattr(date_val, "weekday") and date_val.weekday() == 6
    ) or Holiday.objects.filter(date=date_val).exists()


def _validate_non_working_day(planned_value, *, label: str = "task") -> None:
    """
    Block assignment/creation on Sundays and configured holidays.
    """
    if not planned_value:
        return

    if is_holiday_or_sunday(planned_value):
        raise ValidationError(
            f"This is a holiday date or Sunday, you cannot add a {label} on this day."
        )


# ---------------------------------------------------------------------------
# Shared helpers for handover + notifications
# ---------------------------------------------------------------------------

def _active_handover_for(task_obj, task_type_name: str):
    """
    Find an active LeaveHandover row for this task without causing import cycles.
    """
    try:
        from apps.leave.models import LeaveHandover, LeaveStatus

        today = timezone.now().date()

        return (
            LeaveHandover.objects.filter(
                task_type=task_type_name,
                original_task_id=task_obj.id,
                is_active=True,
                effective_start_date__lte=today,
                effective_end_date__gte=today,
            )
            .filter(leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED])
            .select_related("original_assignee", "new_assignee", "leave_request")
            .first()
        )

    except Exception:
        return None


def _send_task_completion(original_assignee: User, delegate: User, task_obj, context: dict) -> None:
    """
    Fire-and-forget notification to original owner when delegate completes the task.

    NOTE:
    Per latest client rules, generic completion emails are disabled,
    and this helper is not called anywhere.
    """
    try:
        try:
            from apps.tasks.services.notifications import send_task_completion_email
        except Exception:
            from apps.leave.services.notifications import send_task_completion_email

        send_task_completion_email(original_assignee, delegate, task_obj, context)

    except Exception:
        # Do not raise from model layer.
        pass


# ---------------------------------------------------------------------------
# Leave window helper
# ---------------------------------------------------------------------------

def _is_on_leave_instant(user: User, dt) -> bool:
    """
    True if dt lies inside ANY leave window for this user.

    Uses apps.leave.services.tasks_integration when available.
    Falls back to a simple local check.

    Blocking statuses:
    - PENDING
    - APPROVED
    """
    if not user or not getattr(user, "id", None) or not dt:
        return False

    try:
        from apps.leave.services.tasks_integration import is_user_on_leave_at_instant
        return bool(is_user_on_leave_at_instant(user, dt))

    except Exception:
        try:
            from apps.leave.models import LeaveRequest

            qs = LeaveRequest.objects.filter(employee=user).only(
                "start_at",
                "end_at",
                "status",
            )

            ndt = (
                timezone.localtime(dt)
                if timezone.is_aware(dt)
                else timezone.make_aware(dt, timezone.get_current_timezone())
            )

            for lr in qs:
                s = timezone.localtime(lr.start_at)
                e = timezone.localtime(lr.end_at)

                if s <= ndt <= e and str(lr.status) in {"PENDING", "APPROVED"}:
                    return True

        except Exception:
            return False

    return False


# ---------------------------------------------------------------------------
# Checklist
# ---------------------------------------------------------------------------

class Checklist(models.Model):
    assign_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="checklists_assigned",
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="checklists",
    )
    planned_date = models.DateTimeField()

    STATUS_CHOICES = [
        ("Pending", "Pending"),
        ("Completed", "Completed"),
    ]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="Pending")
    completed_at = models.DateTimeField(null=True, blank=True)

    priority = models.CharField(
        max_length=10,
        choices=[
            ("Low", "Low"),
            ("Medium", "Medium"),
            ("High", "High"),
        ],
    )
    attachment_mandatory = models.BooleanField(default=False)

    # -----------------------------------------------------------------------
    # Leave / holiday skip flag
    # -----------------------------------------------------------------------
    # Meaning:
    #   This individual task occurrence was skipped due to leave/holiday.
    #
    # Important:
    #   This should NOT mean admin deleted the recurring task permanently.
    #   Permanent delete/archive is handled by is_deleted/is_active below.
    # -----------------------------------------------------------------------
    is_skipped_due_to_leave = models.BooleanField(default=False, db_index=True)

    # -----------------------------------------------------------------------
    # Checklist lifecycle / delete architecture
    # -----------------------------------------------------------------------
    # is_deleted:
    #   True means this task row or recurring task series was deleted/archived.
    #   Deleted recurring series must never regenerate.
    #
    # is_active:
    #   True means this row is eligible for active/action screens.
    #   False means keep in DB but hide from main checklist/action queue.
    #
    # deleted_at / deleted_by:
    #   Audit trail for who deleted/archived the task.
    #
    # delete_reason:
    #   Why the task was deleted/archived.
    #
    # skip_reason:
    #   Why the occurrence was skipped, e.g. "leave", "holiday", "sunday".
    # -----------------------------------------------------------------------
    is_deleted = models.BooleanField(default=False, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)

    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="deleted_checklist_tasks",
    )

    delete_reason = models.CharField(max_length=255, blank=True)
    skip_reason = models.CharField(max_length=50, blank=True)

    mode = models.CharField(
        max_length=10,
        choices=[
            ("Daily", "Daily"),
            ("Weekly", "Weekly"),
            ("Monthly", "Monthly"),
            ("Yearly", "Yearly"),
        ],
        blank=True,
        null=True,
    )
    frequency = models.PositiveIntegerField(default=1, blank=True, null=True)

    recurrence_end_date = models.DateField(
        null=True,
        blank=True,
        help_text="Stop creating new occurrences after this date.",
    )

    time_per_task_minutes = models.PositiveIntegerField(default=0, blank=True, null=True)
    remind_before_days = models.PositiveIntegerField(default=0, blank=True, null=True)

    message = models.TextField(blank=True)
    media_upload = models.FileField(upload_to="checklist_media/", blank=True, null=True)

    assign_pc = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="pc_checklists",
    )
    group_name = models.CharField(max_length=100, blank=True)
    notify_to = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="notify_checklists",
    )
    auditor = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="audit_checklists",
    )

    set_reminder = models.BooleanField(default=False)
    reminder_mode = models.CharField(
        max_length=10,
        choices=[
            ("Daily", "Daily"),
            ("Weekly", "Weekly"),
            ("Monthly", "Monthly"),
            ("Yearly", "Yearly"),
        ],
        blank=True,
        null=True,
    )
    reminder_frequency = models.PositiveIntegerField(default=1, blank=True, null=True)
    reminder_starting_time = models.TimeField(blank=True, null=True)

    checklist_auto_close = models.BooleanField(default=False)
    checklist_auto_close_days = models.PositiveIntegerField(default=0, blank=True, null=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)
    doer_file = models.FileField(upload_to="checklist_doer/", blank=True, null=True)
    doer_notes = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["assign_to", "task_name", "mode", "frequency", "group_name"]),
            models.Index(fields=["assign_to", "status", "planned_date"]),
            models.Index(fields=["status", "planned_date"]),
            models.Index(fields=["is_skipped_due_to_leave", "planned_date"]),
            models.Index(fields=["is_deleted", "is_active", "planned_date"]),
            models.Index(fields=["assign_to", "is_deleted", "is_active"]),
        ]

    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)

        try:
            from apps.leave.models import LeaveHandover

            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(
                task_type="checklist"
            )
            ids = [h.original_task_id for h in handovers]

            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by("-id")

        except Exception:
            pass

        return original_tasks

    def is_recurring(self):
        return bool(
            self.mode in ["Daily", "Weekly", "Monthly", "Yearly"]
            and self.frequency
            and int(self.frequency) > 0
        )

    @property
    def delay(self):
        end = self.completed_at or timezone.now()
        return timesince(self.planned_date, end)

    def clean(self):
        """
        Hard block for manual/task creation:
        - no checklist assignment on Sunday / configured holiday
        - no checklist assignment during assignee leave window

        Note:
        Recurring engine may shift recurring occurrences to next working day.
        Manual creation remains blocked.
        """
        super().clean()

        if self.planned_date:
            _validate_non_working_day(self.planned_date, label="checklist")

        if self.assign_to_id and self.planned_date:
            if _is_on_leave_instant(self.assign_to, self.planned_date):
                raise ValidationError("Assignee is on leave during the planned window.")

    def save(self, *args, **kwargs):
        old_status = None

        if self.pk:
            try:
                old_status = type(self).objects.only("status", "completed_at").get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        if self.status == "Completed" and not self.completed_at:
            self.completed_at = timezone.now()

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED:
        # No "Task Completed" email for checklist as per client requirement.

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class Delegation(models.Model):
    assign_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="delegations_assigned",
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="delegations",
    )

    cc_users = models.ManyToManyField(
        User,
        blank=True,
        related_name="delegations_cc",
        help_text="Optional users to keep in CC for this delegation.",
    )

    planned_date = models.DateTimeField()

    STATUS_CHOICES = [
        ("Pending", "Pending"),
        ("Completed", "Completed"),
    ]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="Pending")
    completed_at = models.DateTimeField(null=True, blank=True)

    priority = models.CharField(
        max_length=10,
        choices=[
            ("Low", "Low"),
            ("Medium", "Medium"),
            ("High", "High"),
        ],
    )
    attachment_mandatory = models.BooleanField(default=False)
    audio_recording = models.FileField(upload_to="delegation_audio/", blank=True, null=True)

    time_per_task_minutes = models.PositiveIntegerField(default=0, blank=True, null=True)

    # This occurrence was skipped due to leave.
    is_skipped_due_to_leave = models.BooleanField(default=False, db_index=True)

    # Kept for DB compatibility, but delegations are treated as one-time tasks.
    mode = models.CharField(
        max_length=10,
        choices=[
            ("Daily", "Daily"),
            ("Weekly", "Weekly"),
            ("Monthly", "Monthly"),
            ("Yearly", "Yearly"),
        ],
        blank=True,
        null=True,
        default=None,
    )
    frequency = models.PositiveIntegerField(default=None, blank=True, null=True)

    description = models.TextField(default="", blank=True)
    message = models.TextField(blank=True)

    doer_file = models.FileField(upload_to="delegation_doer/", blank=True, null=True)
    doer_notes = models.TextField(blank=True, null=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)

    set_reminder = models.BooleanField(default=False)
    reminder_time = models.DateTimeField(null=True, blank=True)
    reminder_sent_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["assign_to", "status", "planned_date"]),
            models.Index(fields=["is_skipped_due_to_leave", "planned_date"]),
            models.Index(fields=["status", "set_reminder", "reminder_time"]),
        ]

    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)

        try:
            from apps.leave.models import LeaveHandover

            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(
                task_type="delegation"
            )
            ids = [h.original_task_id for h in handovers]

            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by("-id")

        except Exception:
            pass

        return original_tasks

    def is_recurring(self):
        return False

    @property
    def reminder_status(self) -> str:
        if not self.set_reminder or not self.reminder_time:
            return "Not Set"

        if self.status == "Completed":
            return "Completed"

        if self.reminder_sent_at:
            return "Triggered"

        return "Active"

    def clean(self):
        """
        Hard block:
        - delegations are one-time only
        - no delegation assignment on Sunday / configured holiday
        - no delegation assignment during assignee leave window
        """
        self.mode = None
        self.frequency = None

        super().clean()

        if self.planned_date:
            _validate_non_working_day(self.planned_date, label="delegation")

        if self.assign_to_id and self.planned_date:
            if _is_on_leave_instant(self.assign_to, self.planned_date):
                raise ValidationError("Assignee is on leave during the planned window.")

    def save(self, *args, **kwargs):
        self.mode = None
        self.frequency = None

        old_status = None

        if self.pk:
            try:
                old_status = type(self).objects.only("status", "completed_at").get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        if self.status == "Completed" and not self.completed_at:
            self.completed_at = timezone.now()

        if not self.set_reminder:
            self.reminder_time = None

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED:
        # No emails on completion for delegation.

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Bulk upload
# ---------------------------------------------------------------------------

class BulkUpload(models.Model):
    FORM_CHOICES = [
        ("checklist", "Checklist"),
        ("delegation", "Delegation"),
    ]
    form_type = models.CharField(max_length=20, choices=FORM_CHOICES)
    csv_file = models.FileField(upload_to="bulk_uploads/")
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_form_type_display()} upload @ {self.uploaded_at:%Y-%m-%d}"


# ---------------------------------------------------------------------------
# FMS
# ---------------------------------------------------------------------------

class FMS(models.Model):
    assign_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="fms_assigned",
    )
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="fms_tasks",
    )
    planned_date = models.DateField()

    STATUS_CHOICES = [
        ("Pending", "Pending"),
        ("Completed", "Completed"),
    ]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="Pending")

    delay = models.IntegerField(default=0)
    doer_notes = models.TextField(blank=True, null=True)
    doer_file = models.FileField(upload_to="fms_doer/", blank=True, null=True)
    priority = models.CharField(
        max_length=10,
        choices=[
            ("Low", "Low"),
            ("Medium", "Medium"),
            ("High", "High"),
        ],
    )
    estimated_minutes = models.PositiveIntegerField(default=0)

    is_skipped_due_to_leave = models.BooleanField(default=False, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["assign_to", "status", "planned_date"]),
            models.Index(fields=["is_skipped_due_to_leave", "planned_date"]),
        ]

    def clean(self):
        super().clean()

        if self.planned_date:
            _validate_non_working_day(self.planned_date, label="task")

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Help Ticket
# ---------------------------------------------------------------------------

class HelpTicket(models.Model):
    assign_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="help_tickets_assigned",
    )
    title = models.CharField(max_length=200)
    description = models.TextField()
    assign_to = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="help_tickets",
    )
    planned_date = models.DateTimeField()
    priority = models.CharField(
        max_length=10,
        choices=[
            ("Low", "Low"),
            ("Medium", "Medium"),
            ("High", "High"),
        ],
        default="Low",
    )

    STATUS_CHOICES = [
        ("Open", "Open"),
        ("In Progress", "In Progress"),
        ("Closed", "Closed"),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="Open")

    estimated_minutes = models.PositiveIntegerField(default=0)
    media_upload = models.FileField(upload_to="help_ticket_media/", blank=True, null=True)

    resolved_at = models.DateTimeField(null=True, blank=True)
    resolved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="help_tickets_resolved",
    )
    resolved_notes = models.TextField(blank=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)

    is_skipped_due_to_leave = models.BooleanField(default=False, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["assign_to", "status", "planned_date"]),
            models.Index(fields=["is_skipped_due_to_leave", "planned_date"]),
        ]

    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)

        try:
            from apps.leave.models import LeaveHandover

            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(
                task_type="help_ticket"
            )
            ids = [h.original_task_id for h in handovers]

            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by("-id")

        except Exception:
            pass

        return original_tasks

    @property
    def delay(self):
        end = self.resolved_at or timezone.now()
        return timesince(self.planned_date, end)

    def clean(self):
        super().clean()

        _validate_non_working_day(self.planned_date, label="help ticket")

        if self.assign_to_id and self.planned_date:
            if _is_on_leave_instant(self.assign_to, self.planned_date):
                raise ValidationError("Assignee is on leave during the planned window.")

    def save(self, *args, **kwargs):
        old_status = None

        if self.pk:
            try:
                old_status = type(self).objects.only("status", "resolved_at").get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        if self.status == "Closed" and not self.resolved_at:
            self.resolved_at = timezone.now()

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED:
        # No emails on closing help tickets.

    def __str__(self):
        return f"{self.title} → {self.assign_to}"