# apps/leave/signals.py
from __future__ import annotations

import logging
from datetime import date
from typing import List

from django.db.models import Q
from django.db.models.signals import post_save, pre_save
from django.dispatch import Signal, receiver
from django.utils import timezone
from zoneinfo import ZoneInfo

from .models import LeaveRequest, LeaveStatus, ApproverMapping

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# -------------------------------------------------------------------------
# Integration signals (other apps can subscribe)
# -------------------------------------------------------------------------
# Fired when a leave is created (pending) -> block these dates for employee
leave_blocked = Signal()    # args: employee_id: int, dates: List[date], leave_id: int
# Fired when a leave is rejected -> unblock these dates
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
        out: List[date] = []
        cur, last = min(s, e), max(s, e)
        while cur <= last:
            out.append(cur)
            cur = date.fromordinal(cur.toordinal() + 1)
        return out


# -------------------------------------------------------------------------
# Track previous status to detect transitions cleanly
# -------------------------------------------------------------------------
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


# -------------------------------------------------------------------------
# Main workflow hooks (email sending is centralized in models/services)
#   • On create: only emit "blocked" signal (emails sent from models.py -> services)
#   • On reject: emit "unblocked"
# -------------------------------------------------------------------------
@receiver(post_save, sender=LeaveRequest)
def _leave_post_save(sender, instance: LeaveRequest, created: bool, **kwargs):
    lr = instance

    if created:
        # Block dates immediately for task assignment
        try:
            leave_blocked.send(
                sender=LeaveRequest,
                employee_id=lr.employee_id,
                dates=_ist_dates_covered(lr),
                leave_id=lr.id,
            )
        except Exception:
            logger.exception("Failed emitting leave_blocked for leave #%s", lr.id)
        return

    # If status changed to REJECTED, unblock dates
    prev_status = getattr(lr, "_prev_status", None)
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


# -------------------------------------------------------------------------
# When Admin edits ApproverMapping: retarget PENDING leaves & resend
# -------------------------------------------------------------------------
@receiver(post_save, sender=ApproverMapping)
def _on_mapping_changed(sender, instance: ApproverMapping, created: bool, **kwargs):
    """
    If RP/CC changed for an employee:
      • Update any PENDING LeaveRequest.routing fields to the new RP/CC
      • Resend the request email to the new RP (with CC)
    """
    try:
        new_rp = instance.reporting_person
        new_cc = instance.cc_person

        pending = LeaveRequest.objects.filter(employee=instance.employee, status=LeaveStatus.PENDING)
        to_update = pending.filter(~Q(reporting_person=new_rp) | ~Q(cc_person=new_cc))

        if not to_update.exists():
            return

        # Lazy import to avoid circulars
        from apps.leave.services.notifications import send_leave_request_email

        for lr in to_update:
            lr.reporting_person = new_rp
            lr.cc_person = new_cc
            lr.save(update_fields=["reporting_person", "cc_person", "updated_at"])

            # Resend to the new recipients (service handles subject/body & throttling)
            send_leave_request_email(
                lr,
                manager_email=(new_rp.email or None),
                cc_list=[new_cc.email] if getattr(new_cc, "email", None) else [],
            )

            logger.info(
                "Rerouted & resent leave #%s to %s (cc=%s) after ApproverMapping change.",
                lr.id, getattr(new_rp, "email", "-"), getattr(new_cc, "email", "-"),
            )
    except Exception:
        logger.exception("Failed handling ApproverMapping change.")
