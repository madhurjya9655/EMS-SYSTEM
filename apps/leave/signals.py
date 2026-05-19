# FILE: apps/leave/signals.py
# PURPOSE: Leave module signals — workflow hooks, email dispatch, audit logging,
#          and leave-based task skip re-sync.
#
# BUSINESS RULE:
# - User applies leave -> tasks are skipped/hidden immediately.
# - Pending leave blocks tasks.
# - Approved leave blocks tasks.
# - Rejected leave does not block tasks.
# - Deleted leave does not block tasks.
# - If leave dates are changed, task skips move to the new leave period.
# - Historical leave records remain safe.
# - Emails, handovers, audits, and approver mapping logic must not break.

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List

from zoneinfo import ZoneInfo

from django.apps import apps as django_apps
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Q
from django.db.models.signals import post_save, pre_delete, pre_save
from django.dispatch import Signal, receiver
from django.utils import timezone

from .models import ApproverMapping, LeaveRequest, LeaveStatus

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# Active leave statuses that should block/hide/skip tasks.
ACTIVE_TASK_BLOCKING_STATUSES = {
    LeaveStatus.PENDING,
    LeaveStatus.APPROVED,
}

# Re-entrancy guard.
# This prevents receivers from being bound more than once.
if not hasattr(logging, "_leave_signals_bound"):
    logging._leave_signals_bound = False  # type: ignore[attr-defined]


# -------------------------------------------------------------------------
# Integration signals
# Other apps can subscribe to these if needed.
# -------------------------------------------------------------------------
leave_blocked = Signal()    # args: employee_id: int, dates: List[date], leave_id: int
leave_unblocked = Signal()  # args: employee_id: int, dates: List[date], leave_id: int


# -------------------------------------------------------------------------
# Date helpers
# -------------------------------------------------------------------------
def _ist_dates_covered(lr: LeaveRequest) -> List[date]:
    """
    Return IST calendar dates covered by this leave.

    Simple meaning:
    If leave is from 20 May to 22 May,
    return:
    [20 May, 21 May, 22 May]
    """
    try:
        return lr.block_dates()
    except Exception:
        try:
            s = timezone.localtime(lr.start_at, IST).date()
            e = timezone.localtime(lr.end_at, IST).date()
        except Exception:
            s = lr.start_at.date()
            e = lr.end_at.date()

        cur, last = min(s, e), max(s, e)
        out: List[date] = []

        while cur <= last:
            out.append(cur)
            cur = date.fromordinal(cur.toordinal() + 1)

        return out


def _leave_window_ist(lr: LeaveRequest):
    """
    Return normalized leave window in IST.

    Returns:
    start_ist, end_ist, start_date, end_date, is_half_day
    """
    start_at = getattr(lr, "start_at", None)
    end_at = getattr(lr, "end_at", None)

    if not start_at or not end_at:
        return None

    start_ist = timezone.localtime(start_at, IST)
    end_ist = timezone.localtime(end_at, IST)

    if end_ist < start_ist:
        start_ist, end_ist = end_ist, start_ist

    start_date = getattr(lr, "start_date", None) or start_ist.date()
    end_date = getattr(lr, "end_date", None) or end_ist.date()

    if end_date < start_date:
        start_date, end_date = end_date, start_date

    is_half_day = bool(getattr(lr, "is_half_day", False))

    return start_ist, end_ist, start_date, end_date, is_half_day


# -------------------------------------------------------------------------
# Email helpers
# -------------------------------------------------------------------------
def _dedupe_emails_preserve_order(emails: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []

    for e in emails or []:
        low = (e or "").strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        out.append(low)

    return out


def _collect_admin_cc_emails(employee) -> List[str]:
    """
    Admin-managed CC emails.

    Uses LeaveRequest.resolve_routing_multi_for(employee).
    """
    emails: List[str] = []

    try:
        _rp, cc_users = LeaveRequest.resolve_routing_multi_for(employee)

        for u in cc_users:
            if u and getattr(u, "email", None):
                emails.append(u.email)

    except Exception:
        logger.exception("signals._collect_admin_cc_emails: failed to resolve multi-cc")
        emails = []

    return _dedupe_emails_preserve_order(emails)


def _safe_send_request_email_and_audit(leave: LeaveRequest) -> None:
    """
    Send leave request email and write EMAIL_SENT(kind='request') audit.

    Runs after database commit.
    """
    try:
        from .models import DecisionAction, LeaveDecisionAudit
        from apps.leave.services.notifications import send_leave_request_email

        manager_email = (
            leave.reporting_person.email
            if leave.reporting_person and getattr(leave.reporting_person, "email", None)
            else None
        )

        admin_cc_list = _collect_admin_cc_emails(leave.employee)

        extra_cc_emails: List[str] = []

        try:
            extra_cc_emails = [
                u.email
                for u in leave.cc_users.all()
                if getattr(u, "email", None)
            ]
        except Exception:
            extra_cc_emails = []

        all_cc = _dedupe_emails_preserve_order(admin_cc_list + extra_cc_emails)

        send_leave_request_email(
            leave,
            manager_email=manager_email,
            cc_list=all_cc,
        )

        try:
            LeaveDecisionAudit.log(
                leave,
                DecisionAction.EMAIL_SENT,
                kind="request",
            )
        except Exception:
            logger.exception(
                "Failed to log EMAIL_SENT(kind=request) for leave %s",
                getattr(leave, "id", None),
            )

    except Exception:
        logger.exception(
            "Failed to send leave request email from signals for leave %s",
            getattr(leave, "id", None),
        )


def _safe_send_handover_emails_and_create_reminders(leave: LeaveRequest) -> None:
    """
    Send handover emails and create reminders.

    Runs after database commit.
    """
    try:
        from .models import DecisionAction, DelegationReminder, LeaveDecisionAudit, LeaveHandover
        from apps.leave.services.notifications import send_handover_email

        handovers = (
            LeaveHandover.objects
            .filter(leave_request=leave)
            .select_related("new_assignee")
        )

        by_assignee = {}

        for ho in handovers:
            if not getattr(ho, "new_assignee_id", None):
                continue

            by_assignee.setdefault(ho.new_assignee_id, []).append(ho)

        for assignee_id, items in by_assignee.items():
            try:
                assignee = items[0].new_assignee

                send_handover_email(
                    leave,
                    assignee,
                    items,
                )

                try:
                    LeaveDecisionAudit.log(
                        leave,
                        DecisionAction.HANDOVER_EMAIL_SENT,
                        extra={"assignee_id": assignee_id},
                    )
                except Exception:
                    logger.exception(
                        "Failed to log HANDOVER_EMAIL_SENT for leave %s",
                        getattr(leave, "id", None),
                    )

                for ho in items:
                    try:
                        DelegationReminder.objects.get_or_create(
                            leave_handover=ho,
                            defaults={
                                "interval_days": 2,
                                "next_run_at": timezone.now() + timezone.timedelta(days=2),
                                "is_active": True,
                            },
                        )
                    except Exception:
                        logger.exception(
                            "Failed to create/get reminder for handover %s",
                            getattr(ho, "id", None),
                        )

            except Exception:
                logger.exception(
                    "Failed sending handover email for assignee %s leave %s",
                    assignee_id,
                    getattr(leave, "id", None),
                )

    except Exception:
        logger.exception(
            "Failed to send handover emails from signals for leave %s",
            getattr(leave, "id", None),
        )


# -------------------------------------------------------------------------
# Task skip helpers
# -------------------------------------------------------------------------
def _get_task_models():
    """
    Load task models safely.

    Existing production models expected:
    - apps.tasks.Checklist
    - apps.tasks.Delegation
    - apps.tasks.HelpTicket
    - apps.tasks.FMS
    """
    Checklist = django_apps.get_model("tasks", "Checklist")
    Delegation = django_apps.get_model("tasks", "Delegation")
    HelpTicket = django_apps.get_model("tasks", "HelpTicket")
    FMS = django_apps.get_model("tasks", "FMS")

    return Checklist, Delegation, HelpTicket, FMS


def _normalize_handover_task_type(value) -> str:
    """
    Normalize handover task type.

    Example:
    'CHECKLIST' -> 'checklist'
    'checklist' -> 'checklist'
    """
    return str(value or "").strip().lower()


def _handover_exclusion_ids_for_leave(leave: LeaveRequest) -> Dict[str, List[int]]:
    """
    Return task IDs that should not be skipped because they were handed over.

    Simple meaning:
    If a task was delegated to another person during leave,
    do not hide/skip that task for the original assignee.
    """
    exclude_ids: Dict[str, List[int]] = {
        "checklist": [],
        "delegation": [],
        "help_ticket": [],
    }

    try:
        from .models import LeaveHandover

        handovers = (
            LeaveHandover.objects
            .filter(leave_request=leave, is_active=True)
            .only("task_type", "original_task_id")
        )

        for ho in handovers:
            task_type = _normalize_handover_task_type(getattr(ho, "task_type", ""))

            if task_type in exclude_ids:
                exclude_ids[task_type].append(ho.original_task_id)

    except Exception:
        logger.exception(
            "Could not resolve handover exclusions for leave %s",
            getattr(leave, "id", None),
        )

    return exclude_ids


def _restore_leave_skips_for_employee_id(employee_id: int) -> Dict[str, int]:
    """
    Restore all tasks previously skipped because of leave.

    Simple meaning:
    Bring back tasks before recalculating current leave periods.
    """
    counts = {
        "checklist": 0,
        "delegation": 0,
        "help_ticket": 0,
        "fms": 0,
    }

    if not employee_id:
        return counts

    try:
        Checklist, Delegation, HelpTicket, FMS = _get_task_models()

        counts["checklist"] = int(
            Checklist.objects
            .filter(assign_to_id=employee_id, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["delegation"] = int(
            Delegation.objects
            .filter(assign_to_id=employee_id, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["help_ticket"] = int(
            HelpTicket.objects
            .filter(assign_to_id=employee_id, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["fms"] = int(
            FMS.objects
            .filter(assign_to_id=employee_id, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        logger.info(
            "Restored leave-skipped tasks for employee %s counts=%s.",
            employee_id,
            counts,
        )

    except Exception:
        logger.exception(
            "Failed to restore leave-skipped tasks for employee %s.",
            employee_id,
        )

    return counts


def _apply_task_skips_for_leave(leave: LeaveRequest, *, exclude_handover: bool = True) -> Dict[str, int]:
    """
    Apply task skips for one active leave.

    Active means:
    - PENDING
    - APPROVED

    Inactive means:
    - REJECTED
    - CANCELLED if your project has it
    - Deleted leave
    """
    counts = {
        "checklist": 0,
        "delegation": 0,
        "help_ticket": 0,
        "fms": 0,
    }

    try:
        leave_status = getattr(leave, "status", None)

        if leave_status not in ACTIVE_TASK_BLOCKING_STATUSES:
            logger.info(
                "Task auto-skip ignored for leave %s because status=%s is not active.",
                getattr(leave, "id", None),
                leave_status,
            )
            return counts

        if not getattr(leave, "employee_id", None):
            logger.warning(
                "Task auto-skip ignored for leave %s because employee_id is missing.",
                getattr(leave, "id", None),
            )
            return counts

        window = _leave_window_ist(leave)

        if not window:
            logger.warning(
                "Task auto-skip ignored for leave %s because start_at/end_at is missing.",
                getattr(leave, "id", None),
            )
            return counts

        start_ist, end_ist, start_date, end_date, is_half_day = window

        Checklist, Delegation, HelpTicket, FMS = _get_task_models()

        exclude_ids = {
            "checklist": [],
            "delegation": [],
            "help_ticket": [],
        }

        if exclude_handover:
            exclude_ids = _handover_exclusion_ids_for_leave(leave)

        if is_half_day:
            datetime_window = Q(planned_date__gte=start_ist) & Q(planned_date__lt=end_ist)
        else:
            datetime_window = Q(planned_date__date__gte=start_date) & Q(planned_date__date__lte=end_date)

        checklist_qs = (
            Checklist.objects
            .filter(
                assign_to_id=leave.employee_id,
                status="Pending",
                is_skipped_due_to_leave=False,
            )
            .filter(datetime_window)
        )

        if exclude_ids["checklist"]:
            checklist_qs = checklist_qs.exclude(id__in=exclude_ids["checklist"])

        counts["checklist"] = int(
            checklist_qs.update(is_skipped_due_to_leave=True)
        )

        delegation_qs = (
            Delegation.objects
            .filter(
                assign_to_id=leave.employee_id,
                status="Pending",
                is_skipped_due_to_leave=False,
            )
            .filter(datetime_window)
        )

        if exclude_ids["delegation"]:
            delegation_qs = delegation_qs.exclude(id__in=exclude_ids["delegation"])

        counts["delegation"] = int(
            delegation_qs.update(is_skipped_due_to_leave=True)
        )

        help_ticket_qs = (
            HelpTicket.objects
            .filter(
                assign_to_id=leave.employee_id,
                is_skipped_due_to_leave=False,
            )
            .exclude(status__in=["Closed", "COMPLETED", "Completed", "Done"])
            .filter(datetime_window)
        )

        if exclude_ids["help_ticket"]:
            help_ticket_qs = help_ticket_qs.exclude(id__in=exclude_ids["help_ticket"])

        counts["help_ticket"] = int(
            help_ticket_qs.update(is_skipped_due_to_leave=True)
        )

        # FMS planned_date is DateField in your existing logic.
        # DateField cannot support exact half-day time windows.
        # So FMS is skipped only for full-day leave.
        if not is_half_day:
            counts["fms"] = int(
                FMS.objects
                .filter(
                    assign_to_id=leave.employee_id,
                    planned_date__gte=start_date,
                    planned_date__lte=end_date,
                    is_skipped_due_to_leave=False,
                )
                .update(is_skipped_due_to_leave=True)
            )

        logger.info(
            "Task auto-skip applied for leave %s employee=%s status=%s half_day=%s counts=%s.",
            getattr(leave, "id", None),
            getattr(leave, "employee_id", None),
            leave_status,
            is_half_day,
            counts,
        )

    except Exception:
        logger.exception(
            "Task auto-skip failed for leave %s.",
            getattr(leave, "id", None),
        )

    return counts


def _resync_leave_task_skips_for_employee_id(
    employee_id: int,
    *,
    exclude_handover: bool = True,
) -> Dict[str, object]:
    """
    Recalculate leave-based task skipping for one employee.

    Simple explanation:
    1. First bring back all tasks skipped because of leave.
    2. Then check all active leaves.
    3. Then skip tasks again for those active leave periods.

    This handles:
    - leave apply
    - manager approval
    - manager rejection
    - user delete
    - admin delete
    - leave date change
    - overlapping leaves
    """
    result: Dict[str, object] = {
        "restored": {
            "checklist": 0,
            "delegation": 0,
            "help_ticket": 0,
            "fms": 0,
        },
        "applied": {},
        "active_leave_ids": [],
    }

    if not employee_id:
        return result

    try:
        restored_counts = _restore_leave_skips_for_employee_id(employee_id)
        result["restored"] = restored_counts

        active_leaves = (
            LeaveRequest.objects
            .filter(
                employee_id=employee_id,
                status__in=ACTIVE_TASK_BLOCKING_STATUSES,
            )
            .select_related("employee")
            .order_by("start_at", "id")
        )

        applied = {}
        active_leave_ids = []

        for active_leave in active_leaves:
            skip_counts = _apply_task_skips_for_leave(
                active_leave,
                exclude_handover=exclude_handover,
            )

            applied[active_leave.id] = skip_counts
            active_leave_ids.append(active_leave.id)

        result["applied"] = applied
        result["active_leave_ids"] = active_leave_ids

        logger.info(
            "Re-synced leave task skips for employee %s active_leave_ids=%s result=%s.",
            employee_id,
            active_leave_ids,
            result,
        )

    except Exception:
        logger.exception(
            "Failed to re-sync leave task skips for employee %s.",
            employee_id,
        )

    return result


def _resync_leave_task_skips_after_commit(
    *,
    employee_id: int,
    reason: str,
    leave_id=None,
) -> None:
    """
    Schedule task skip re-sync after database commit.

    This avoids changing tasks before the leave save/delete is safely committed.
    """
    if not employee_id:
        return

    def _after_commit():
        result = _resync_leave_task_skips_for_employee_id(
            employee_id,
            exclude_handover=True,
        )

        logger.info(
            "Leave task skip re-sync completed reason=%s leave=%s employee=%s result=%s.",
            reason,
            leave_id,
            employee_id,
            result,
        )

    try:
        transaction.on_commit(_after_commit)
    except Exception:
        logger.exception(
            "transaction.on_commit failed for leave task skip re-sync reason=%s leave=%s employee=%s.",
            reason,
            leave_id,
            employee_id,
        )
        _after_commit()


# -------------------------------------------------------------------------
# Signal bindings
# -------------------------------------------------------------------------
if not logging._leave_signals_bound:  # type: ignore[attr-defined]
    logging._leave_signals_bound = True  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------
    # Track previous leave values before save.
    # ---------------------------------------------------------------------
    @receiver(pre_save, sender=LeaveRequest)
    def _stash_prev_leave_state(sender, instance: LeaveRequest, **kwargs):
        if not instance.pk:
            instance._prev_status = None  # type: ignore[attr-defined]
            instance._prev_employee_id = None  # type: ignore[attr-defined]
            instance._prev_start_at = None  # type: ignore[attr-defined]
            instance._prev_end_at = None  # type: ignore[attr-defined]
            instance._prev_is_half_day = None  # type: ignore[attr-defined]
            return

        try:
            prev = (
                LeaveRequest.objects
                .only("status", "employee_id", "start_at", "end_at", "is_half_day")
                .get(pk=instance.pk)
            )

            instance._prev_status = prev.status  # type: ignore[attr-defined]
            instance._prev_employee_id = prev.employee_id  # type: ignore[attr-defined]
            instance._prev_start_at = prev.start_at  # type: ignore[attr-defined]
            instance._prev_end_at = prev.end_at  # type: ignore[attr-defined]
            instance._prev_is_half_day = prev.is_half_day  # type: ignore[attr-defined]

        except LeaveRequest.DoesNotExist:
            instance._prev_status = None  # type: ignore[attr-defined]
            instance._prev_employee_id = None  # type: ignore[attr-defined]
            instance._prev_start_at = None  # type: ignore[attr-defined]
            instance._prev_end_at = None  # type: ignore[attr-defined]
            instance._prev_is_half_day = None  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------
    # Main leave workflow hook.
    #
    # Handles:
    # - leave created
    # - leave date changed
    # - leave status changed
    # - leave approved
    # - leave rejected
    # - employee changed
    # ---------------------------------------------------------------------
    @receiver(post_save, sender=LeaveRequest)
    def _leave_post_save(sender, instance: LeaveRequest, created: bool, **kwargs):
        lr = instance

        prev_status = getattr(lr, "_prev_status", None)
        prev_employee_id = getattr(lr, "_prev_employee_id", None)
        prev_start_at = getattr(lr, "_prev_start_at", None)
        prev_end_at = getattr(lr, "_prev_end_at", None)
        prev_is_half_day = getattr(lr, "_prev_is_half_day", None)

        current_employee_id = getattr(lr, "employee_id", None)

        status_changed = prev_status != lr.status
        employee_changed = bool(prev_employee_id and prev_employee_id != current_employee_id)
        date_changed = (
            prev_start_at != getattr(lr, "start_at", None)
            or prev_end_at != getattr(lr, "end_at", None)
            or prev_is_half_day != getattr(lr, "is_half_day", None)
        )

        # Lazy import to avoid cycles.
        try:
            from apps.leave.services.task_handover import apply_handover_for_leave
        except Exception:
            apply_handover_for_leave = None  # type: ignore

        if created:
            # 1. Notify integration listeners that leave blocks dates immediately.
            try:
                leave_blocked.send(
                    sender=LeaveRequest,
                    employee_id=lr.employee_id,
                    dates=_ist_dates_covered(lr),
                    leave_id=lr.id,
                )
            except Exception:
                logger.exception(
                    "Failed emitting leave_blocked for leave %s.",
                    getattr(lr, "id", None),
                )

            # 2. Apply handover immediately for dashboard correctness.
            if apply_handover_for_leave:
                try:
                    apply_handover_for_leave(lr)
                except Exception:
                    logger.exception(
                        "Leave %s failed to apply handover on create.",
                        getattr(lr, "id", None),
                    )

            # 3. Re-sync task skips immediately after commit.
            #    This is what hides/skips tasks right after leave apply.
            _resync_leave_task_skips_after_commit(
                employee_id=current_employee_id,
                reason="leave_created",
                leave_id=lr.id,
            )

            # 4. After commit: audits + emails.
            def _after_create_commit():
                try:
                    from .models import DecisionAction, LeaveDecisionAudit

                    LeaveDecisionAudit.log(
                        lr,
                        DecisionAction.APPLIED,
                    )

                except Exception:
                    logger.exception(
                        "Failed logging APPLIED for leave %s.",
                        getattr(lr, "id", None),
                    )

                _safe_send_request_email_and_audit(lr)

                try:
                    if lr.handovers.exists():
                        _safe_send_handover_emails_and_create_reminders(lr)
                except Exception:
                    logger.exception(
                        "Post-commit handover email phase failed for leave %s.",
                        getattr(lr, "id", None),
                    )

            try:
                transaction.on_commit(_after_create_commit)
            except Exception:
                logger.exception(
                    "transaction.on_commit failed for create email/audit phase leave %s.",
                    getattr(lr, "id", None),
                )
                _after_create_commit()

            return

        # If employee changed, restore/re-sync old employee too.
        if employee_changed:
            _resync_leave_task_skips_after_commit(
                employee_id=prev_employee_id,
                reason="leave_employee_changed_old_employee",
                leave_id=lr.id,
            )

        # Always re-sync current employee after relevant leave update.
        # This covers:
        # - approval
        # - rejection
        # - date change
        # - half-day change
        # - employee change
        if status_changed or date_changed or employee_changed:
            _resync_leave_task_skips_after_commit(
                employee_id=current_employee_id,
                reason="leave_updated",
                leave_id=lr.id,
            )

        if status_changed and lr.status == LeaveStatus.APPROVED:
            if apply_handover_for_leave:
                try:
                    apply_handover_for_leave(lr)
                except Exception:
                    logger.exception(
                        "Leave %s failed to apply handover on approval.",
                        getattr(lr, "id", None),
                    )

        if status_changed and lr.status == LeaveStatus.REJECTED:
            try:
                leave_unblocked.send(
                    sender=LeaveRequest,
                    employee_id=lr.employee_id,
                    dates=_ist_dates_covered(lr),
                    leave_id=lr.id,
                )
            except Exception:
                logger.exception(
                    "Failed emitting leave_unblocked for leave %s.",
                    getattr(lr, "id", None),
                )

    # ---------------------------------------------------------------------
    # Delete hook.
    #
    # Handles:
    # - user deletes pending leave
    # - admin deletes leave
    # ---------------------------------------------------------------------
    @receiver(pre_delete, sender=LeaveRequest)
    def _leave_pre_delete(sender, instance: LeaveRequest, **kwargs):
        leave_id = getattr(instance, "id", None)
        employee_id = getattr(instance, "employee_id", None)

        try:
            leave_unblocked.send(
                sender=LeaveRequest,
                employee_id=employee_id,
                dates=_ist_dates_covered(instance),
                leave_id=leave_id,
            )
        except Exception:
            logger.exception(
                "Failed emitting leave_unblocked during delete for leave %s.",
                leave_id,
            )

        _resync_leave_task_skips_after_commit(
            employee_id=employee_id,
            reason="leave_deleted",
            leave_id=leave_id,
        )

    # ---------------------------------------------------------------------
    # When Admin edits ApproverMapping:
    # retarget PENDING leaves and resend request email.
    # ---------------------------------------------------------------------
    @receiver(post_save, sender=ApproverMapping)
    def _on_mapping_changed(sender, instance: ApproverMapping, created: bool, **kwargs):
        try:
            new_rp = instance.reporting_person
            new_cc = instance.cc_person

            pending = LeaveRequest.objects.filter(
                employee=instance.employee,
                status=LeaveStatus.PENDING,
            )

            to_update = pending.filter(
                ~Q(reporting_person=new_rp) | ~Q(cc_person=new_cc)
            )

            if not to_update.exists():
                return

            from apps.leave.services.notifications import send_leave_request_email

            for lr in to_update:
                lr.reporting_person = new_rp
                lr.cc_person = new_cc

                lr.save(
                    update_fields=[
                        "reporting_person",
                        "cc_person",
                        "updated_at",
                    ]
                )

                send_leave_request_email(
                    lr,
                    manager_email=(new_rp.email or None) if new_rp else None,
                    cc_list=[new_cc.email] if getattr(new_cc, "email", None) else [],
                    force=True,
                )

                logger.info(
                    "Rerouted and resent leave %s to %s cc=%s after ApproverMapping change.",
                    getattr(lr, "id", None),
                    getattr(new_rp, "email", "-"),
                    getattr(new_cc, "email", "-"),
                )

        except Exception:
            logger.exception("Failed handling ApproverMapping change.")