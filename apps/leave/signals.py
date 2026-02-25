# File: apps/leave/signals.py
from __future__ import annotations

import logging
from datetime import date
from typing import List

import pytz
from django.db import transaction
from django.db.models import Q
from django.db.models.signals import post_save, pre_save
from django.dispatch import Signal, receiver
from django.utils import timezone

from .models import LeaveRequest, LeaveStatus, ApproverMapping

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Re-entrancy guard (prevents duplicate bindings)
if not hasattr(logging, "_leave_signals_bound"):
    logging._leave_signals_bound = False  # type: ignore[attr-defined]

# -------------------------------------------------------------------------
# Integration signals (other apps can subscribe)
# -------------------------------------------------------------------------
leave_blocked = Signal()    # args: employee_id: int, dates: List[date], leave_id: int
leave_unblocked = Signal()  # args: employee_id: int, dates: List[date], leave_id: int


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def _ist_dates_covered(lr: LeaveRequest) -> List[date]:
    """Return IST calendar dates covered by this leave (inclusive)."""
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
    Admin-managed CC emails (multi + legacy fallback).
    Uses LeaveRequest.resolve_routing_multi_for (robust).
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
    Send request email + write EMAIL_SENT(kind='request') audit.
    Runs after commit.
    """
    try:
        from .models import LeaveDecisionAudit, DecisionAction  # local import avoids cycles
        from apps.leave.services.notifications import send_leave_request_email

        manager_email = (
            leave.reporting_person.email
            if (leave.reporting_person and getattr(leave.reporting_person, "email", None))
            else None
        )

        admin_cc_list = _collect_admin_cc_emails(leave.employee)

        extra_cc_emails: List[str] = []
        try:
            extra_cc_emails = [u.email for u in leave.cc_users.all() if getattr(u, "email", None)]
        except Exception:
            extra_cc_emails = []

        all_cc = _dedupe_emails_preserve_order(admin_cc_list + extra_cc_emails)

        send_leave_request_email(leave, manager_email=manager_email, cc_list=all_cc)

        # mark kind=request for duplicate suppression logic downstream
        try:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT, kind="request")
        except Exception:
            logger.exception("Failed to log EMAIL_SENT(kind=request) for leave %s", leave.id)

    except Exception:
        logger.exception("Failed to send leave request email (signals) for leave %s", getattr(leave, "id", None))


def _safe_send_handover_emails_and_create_reminders(leave: LeaveRequest) -> None:
    """
    If there are handovers, send one email per assignee and create reminders.
    Runs after commit.
    """
    try:
        from .models import LeaveHandover, DelegationReminder, LeaveDecisionAudit, DecisionAction
        from apps.leave.services.notifications import send_handover_email

        handovers = LeaveHandover.objects.filter(leave_request=leave).select_related("new_assignee")

        by_assignee = {}
        for ho in handovers:
            if not getattr(ho, "new_assignee_id", None):
                continue
            by_assignee.setdefault(ho.new_assignee_id, []).append(ho)

        for assignee_id, items in by_assignee.items():
            try:
                assignee = items[0].new_assignee
                send_handover_email(leave, assignee, items)

                try:
                    LeaveDecisionAudit.log(
                        leave,
                        DecisionAction.HANDOVER_EMAIL_SENT,
                        extra={"assignee_id": assignee_id},
                    )
                except Exception:
                    logger.exception("Failed to log HANDOVER_EMAIL_SENT for leave %s", leave.id)

                # Ensure reminders exist (default every 2 days)
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
                        logger.exception("Failed to create/get reminder for handover %s", getattr(ho, "id", None))

            except Exception:
                logger.exception("Failed sending handover email for assignee %s (leave %s)", assignee_id, leave.id)

    except Exception:
        logger.exception("Failed to send handover emails (signals) for leave %s", getattr(leave, "id", None))


# Only bind receivers once
if not logging._leave_signals_bound:  # type: ignore[attr-defined]
    logging._leave_signals_bound = True  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------
    # Track previous status to detect transitions cleanly
    # ---------------------------------------------------------------------
    @receiver(pre_save, sender=LeaveRequest)
    def _stash_prev_status(sender, instance: LeaveRequest, **kwargs):
        if not instance.pk:
            instance._prev_status = None  # type: ignore[attr-defined]
            return
        try:
            prev = LeaveRequest.objects.only("status").get(pk=instance.pk)
            instance._prev_status = prev.status  # type: ignore[attr-defined]
        except LeaveRequest.DoesNotExist:
            instance._prev_status = None  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------
    # Main workflow hooks
    #   • On create: emit leave_blocked + log APPLIED + send request email + handover email (after commit)
    #   • On approve: re-apply handover (idempotent safety)
    #   • On reject: emit leave_unblocked
    # ---------------------------------------------------------------------
    @receiver(post_save, sender=LeaveRequest)
    def _leave_post_save(sender, instance: LeaveRequest, created: bool, **kwargs):
        lr = instance

        # Lazy import to avoid cycles
        try:
            from apps.leave.services.task_handover import apply_handover_for_leave  # noqa
        except Exception:
            apply_handover_for_leave = None  # type: ignore

        if created:
            # 1) Block dates immediately
            try:
                leave_blocked.send(
                    sender=LeaveRequest,
                    employee_id=lr.employee_id,
                    dates=_ist_dates_covered(lr),
                    leave_id=lr.id,
                )
            except Exception:
                logger.exception("Failed emitting leave_blocked for leave #%s", lr.id)

            # 2) Apply handover immediately (dashboard correctness)
            if apply_handover_for_leave:
                try:
                    apply_handover_for_leave(lr)
                except Exception:
                    logger.exception("Leave #%s: failed to apply handover on create", lr.id)

            # 3) After commit: audits + emails (prevents sqlite locks)
            def _after_commit():
                try:
                    from .models import LeaveDecisionAudit, DecisionAction
                    LeaveDecisionAudit.log(lr, DecisionAction.APPLIED)
                except Exception:
                    logger.exception("Failed logging APPLIED for leave #%s", lr.id)

                _safe_send_request_email_and_audit(lr)

                try:
                    # Only attempt if any handovers exist
                    if lr.handovers.exists():
                        _safe_send_handover_emails_and_create_reminders(lr)
                except Exception:
                    logger.exception("Post-commit handover email phase failed for leave #%s", lr.id)

            try:
                transaction.on_commit(_after_commit)
            except Exception:
                # Fallback if on_commit not available
                _after_commit()

            return

        # Updates: check transitions
        prev_status = getattr(lr, "_prev_status", None)

        if prev_status != lr.status and lr.status == LeaveStatus.APPROVED:
            if apply_handover_for_leave:
                try:
                    apply_handover_for_leave(lr)
                except Exception:
                    logger.exception("Leave #%s: failed to apply handover on approval", lr.id)

        if prev_status != lr.status and lr.status == LeaveStatus.REJECTED:
            try:
                leave_unblocked.send(
                    sender=LeaveRequest,
                    employee_id=lr.employee_id,
                    dates=_ist_dates_covered(lr),
                    leave_id=lr.id,
                )
            except Exception:
                logger.exception("Failed emitting leave_unblocked for leave #%s", lr.id)

    # ---------------------------------------------------------------------
    # When Admin edits ApproverMapping: retarget PENDING leaves & resend
    # ---------------------------------------------------------------------
    @receiver(post_save, sender=ApproverMapping)
    def _on_mapping_changed(sender, instance: ApproverMapping, created: bool, **kwargs):
        try:
            new_rp = instance.reporting_person
            new_cc = instance.cc_person

            pending = LeaveRequest.objects.filter(employee=instance.employee, status=LeaveStatus.PENDING)
            to_update = pending.filter(~Q(reporting_person=new_rp) | ~Q(cc_person=new_cc))

            if not to_update.exists():
                return

            from apps.leave.services.notifications import send_leave_request_email  # noqa

            for lr in to_update:
                lr.reporting_person = new_rp
                lr.cc_person = new_cc
                lr.save(update_fields=["reporting_person", "cc_person", "updated_at"])

                send_leave_request_email(
                    lr,
                    manager_email=(new_rp.email or None) if new_rp else None,
                    cc_list=[new_cc.email] if getattr(new_cc, "email", None) else [],
                    force=True,
                )

                logger.info(
                    "Rerouted & resent leave #%s to %s (cc=%s) after ApproverMapping change.",
                    lr.id, getattr(new_rp, "email", "-"), getattr(new_cc, "email", "-"),
                )
        except Exception:
            logger.exception("Failed handling ApproverMapping change.")