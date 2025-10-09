# apps/tasks/models.py
from datetime import timedelta
from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.timesince import timesince
from apps.settings.models import Holiday

User = get_user_model()


def is_holiday_or_sunday(date_val):
    if not date_val:
        return False
    if hasattr(date_val, "date"):
        try:
            date_val = date_val.date()
        except Exception:
            pass
    return hasattr(date_val, "weekday") and date_val.weekday() == 6 or Holiday.objects.filter(date=date_val).exists()


# ---------------------------------------------------------------------------
# Shared helpers for handover + notifications
# ---------------------------------------------------------------------------

def _active_handover_for(task_obj, task_type_name: str):
    """
    Find an active LeaveHandover row for this task (if any) without causing import
    cycles by importing lazily.
    """
    try:
        from django.db.models import Q
        from apps.leave.models import LeaveHandover, LeaveStatus  # local import to avoid circulars
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
    """
    try:
        # Prefer the tasks.notifications service if present; fall back to leave.notifications
        try:
            from apps.tasks.services.notifications import send_task_completion_email  # type: ignore
        except Exception:
            from apps.leave.services.notifications import send_task_completion_email  # type: ignore
        send_task_completion_email(original_assignee, delegate, task_obj, context)
    except Exception:
        # Do not raise from model layer
        pass


# ---------------------------------------------------------------------------
# Checklist
# ---------------------------------------------------------------------------

class Checklist(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='checklists_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='checklists')
    planned_date = models.DateTimeField()

    STATUS_CHOICES = [('Pending', 'Pending'), ('Completed', 'Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    completed_at = models.DateTimeField(null=True, blank=True)

    priority = models.CharField(max_length=10, choices=[('Low', 'Low'), ('Medium', 'Medium'), ('High', 'High')])
    attachment_mandatory = models.BooleanField(default=False)

    mode = models.CharField(
        max_length=10,
        choices=[
            ('Daily', 'Daily'),
            ('Weekly', 'Weekly'),
            ('Monthly', 'Monthly'),
            ('Yearly', 'Yearly'),
        ],
        blank=True,
        null=True,
    )
    frequency = models.PositiveIntegerField(default=1, blank=True, null=True)

    recurrence_end_date = models.DateField(
        null=True,
        blank=True,
        help_text="Stop creating new occurrences after this date."
    )

    time_per_task_minutes = models.PositiveIntegerField(default=0, blank=True, null=True)
    remind_before_days = models.PositiveIntegerField(default=0, blank=True, null=True)

    message = models.TextField(blank=True)
    media_upload = models.FileField(upload_to='checklist_media/', blank=True, null=True)

    assign_pc = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='pc_checklists')
    group_name = models.CharField(max_length=100, blank=True)
    notify_to = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='notify_checklists')
    auditor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='audit_checklists')

    set_reminder = models.BooleanField(default=False)
    reminder_mode = models.CharField(
        max_length=10,
        choices=[
            ('Daily', 'Daily'),
            ('Weekly', 'Weekly'),
            ('Monthly', 'Monthly'),
            ('Yearly', 'Yearly'),
        ],
        blank=True,
        null=True,
    )
    reminder_frequency = models.PositiveIntegerField(default=1, blank=True, null=True)
    reminder_starting_time = models.TimeField(blank=True, null=True)

    checklist_auto_close = models.BooleanField(default=False)
    checklist_auto_close_days = models.PositiveIntegerField(default=0, blank=True, null=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)
    doer_file = models.FileField(upload_to='checklist_doer/', blank=True, null=True)
    doer_notes = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['assign_to', 'task_name', 'mode', 'frequency', 'group_name']),
            models.Index(fields=['assign_to', 'status', 'planned_date']),
            models.Index(fields=['status', 'planned_date']),
        ]

    # ---- Dashboard helper (no import of HandoverTaskMixin to avoid cycles)
    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)
        try:
            handovers = None
            # Lazy import
            from apps.leave.models import LeaveHandover  # type: ignore
            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(task_type='checklist')
            ids = [h.original_task_id for h in handovers]
            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by('-id')
        except Exception:
            pass
        return original_tasks

    def is_recurring(self):
        return bool(
            self.mode in ['Daily', 'Weekly', 'Monthly', 'Yearly'] and
            self.frequency and int(self.frequency) > 0
        )

    @property
    def delay(self):
        end = self.completed_at or timezone.now()
        return timesince(self.planned_date, end)

    def clean(self):
        super().clean()

    def save(self, *args, **kwargs):
        # detect status transition
        old_status = None
        if self.pk:
            try:
                old_status = type(self).objects.only('status', 'completed_at').get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        # auto set completion timestamp when moving to Completed
        if self.status == 'Completed' and not self.completed_at:
            self.completed_at = timezone.now()

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED: No emails on completion for checklist.
        # (Recurring creation & dashboard logic remain handled elsewhere.)

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class Delegation(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='delegations_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='delegations')

    planned_date = models.DateTimeField()

    STATUS_CHOICES = [('Pending', 'Pending'), ('Completed', 'Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')
    completed_at = models.DateTimeField(null=True, blank=True)

    priority = models.CharField(max_length=10, choices=[('Low', 'Low'), ('Medium', 'Medium'), ('High', 'High')])
    attachment_mandatory = models.BooleanField(default=False)
    audio_recording = models.FileField(upload_to='delegation_audio/', blank=True, null=True)

    time_per_task_minutes = models.PositiveIntegerField(default=0, blank=True, null=True)

    mode = models.CharField(
        max_length=10,
        choices=[
            ('Daily', 'Daily'),
            ('Weekly', 'Weekly'),
            ('Monthly', 'Monthly'),
            ('Yearly', 'Yearly'),
        ],
        blank=True,
        null=True,
        default=None,
    )
    frequency = models.PositiveIntegerField(default=None, blank=True, null=True)

    description = models.TextField(default="", blank=True)

    message = models.TextField(blank=True)

    doer_file = models.FileField(upload_to='delegation_doer/', blank=True, null=True)
    doer_notes = models.TextField(blank=True, null=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['assign_to', 'status', 'planned_date']),
        ]

    # ---- Dashboard helper (no mixin import)
    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)
        try:
            from apps.leave.models import LeaveHandover  # type: ignore
            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(task_type='delegation')
            ids = [h.original_task_id for h in handovers]
            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by('-id')
        except Exception:
            pass
        return original_tasks

    def is_recurring(self):
        return False

    def clean(self):
        self.mode = None
        self.frequency = None
        super().clean()

    def save(self, *args, **kwargs):
        # enforce non-recurring
        self.mode = None
        self.frequency = None

        old_status = None
        if self.pk:
            try:
                old_status = type(self).objects.only('status', 'completed_at').get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        # auto set completion timestamp
        if self.status == 'Completed' and not self.completed_at:
            self.completed_at = timezone.now()

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED: No emails on completion for delegation.

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Bulk upload
# ---------------------------------------------------------------------------

class BulkUpload(models.Model):
    FORM_CHOICES = [('checklist', 'Checklist'), ('delegation', 'Delegation')]
    form_type = models.CharField(max_length=20, choices=FORM_CHOICES)
    csv_file = models.FileField(upload_to='bulk_uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.get_form_type_display()} upload @ {self.uploaded_at:%Y-%m-%d}"


# ---------------------------------------------------------------------------
# FMS (not part of handover types)
# ---------------------------------------------------------------------------

class FMS(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='fms_assigned')
    task_name = models.CharField(max_length=200)
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='fms_tasks')
    planned_date = models.DateField()

    STATUS_CHOICES = [('Pending', 'Pending'), ('Completed', 'Completed')]
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')

    delay = models.IntegerField(default=0)
    doer_notes = models.TextField(blank=True, null=True)
    doer_file = models.FileField(upload_to='fms_doer/', blank=True, null=True)
    priority = models.CharField(max_length=10, choices=[('Low', 'Low'), ('Medium', 'Medium'), ('High', 'High')])
    estimated_minutes = models.PositiveIntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.task_name} → {self.assign_to}"


# ---------------------------------------------------------------------------
# Help Ticket
# ---------------------------------------------------------------------------

class HelpTicket(models.Model):
    assign_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='help_tickets_assigned')
    title = models.CharField(max_length=200)
    description = models.TextField()
    assign_to = models.ForeignKey(User, on_delete=models.CASCADE, related_name='help_tickets')
    planned_date = models.DateTimeField()
    priority = models.CharField(
        max_length=10,
        choices=[('Low', 'Low'), ('Medium', 'Medium'), ('High', 'High')],
        default='Low',
    )

    STATUS_CHOICES = [('Open', 'Open'), ('In Progress', 'In Progress'), ('Closed', 'Closed')]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Open')

    estimated_minutes = models.PositiveIntegerField(default=0)
    media_upload = models.FileField(upload_to='help_ticket_media/', blank=True, null=True)

    resolved_at = models.DateTimeField(null=True, blank=True)
    resolved_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name='help_tickets_resolved'
    )
    resolved_notes = models.TextField(blank=True)

    actual_duration_minutes = models.PositiveIntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['assign_to', 'status', 'planned_date']),
        ]

    # ---- Dashboard helper
    @classmethod
    def get_tasks_for_user(cls, user: User):
        original_tasks = cls.objects.filter(assign_to=user)
        try:
            from apps.leave.models import LeaveHandover  # type: ignore
            handovers = LeaveHandover.objects.currently_assigned_to(user).filter(task_type='help_ticket')
            ids = [h.original_task_id for h in handovers]
            if ids:
                ho_tasks = cls.objects.filter(id__in=ids)
                return original_tasks.union(ho_tasks).order_by('-id')
        except Exception:
            pass
        return original_tasks

    @property
    def delay(self):
        end = self.resolved_at or timezone.now()
        return timesince(self.planned_date, end)

    def clean(self):
        from django.core.exceptions import ValidationError
        if is_holiday_or_sunday(self.planned_date):
            raise ValidationError("This is a holiday date or Sunday, you cannot add a task on this day.")
        super().clean()

    def save(self, *args, **kwargs):
        old_status = None
        if self.pk:
            try:
                old_status = type(self).objects.only('status', 'resolved_at').get(pk=self.pk).status
            except type(self).DoesNotExist:
                old_status = None

        # if moving to Closed, stamp resolved_at
        if self.status == 'Closed' and not self.resolved_at:
            self.resolved_at = timezone.now()

        self.full_clean()
        super().save(*args, **kwargs)

        # COMPLETION EMAILS DISABLED: No emails on closing help tickets.

    def __str__(self):
        return f"{self.title} → {self.assign_to}"
