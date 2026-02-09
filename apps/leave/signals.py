# File: apps/leave/signals.py
from __future__ import annotations

import logging
from datetime import date
from typing import List

from django.db.models import Q
from django.db.models.signals import post_save, pre_save
from django.dispatch import Signal, receiver
from django.utils import timezone
import pytz  # ✅ align tz impl with models/forms

from .models import LeaveRequest, LeaveStatus, ApproverMapping

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Re-entrancy guard: this module may be imported multiple times by different
# app configs; ensure handlers are only connected once.
if not hasattr(logging, "_leave_signals_bound"):
    logging._leave_signals_bound = False  # type: ignore[attr-defined]

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
        # prefer model's own helper if present
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
    #   • On create: emit "blocked" signal + apply handover immediately
    #   • On approve: (idempotently) apply handover again (safety)
    #   • On reject: emit "unblocked"
    #   NOTE: actual emails are handled in services/models; this file coordinates state.
    # ---------------------------------------------------------------------
    @receiver(post_save, sender=LeaveRequest)
    def _leave_post_save(sender, instance: LeaveRequest, created: bool, **kwargs):
        lr = instance

        # Lazy import avoids circular imports during app loading/migrations
        try:
            from apps.leave.services.task_handover import apply_handover_for_leave  # noqa: WPS433
        except Exception as e:  # pragma: no cover
            logger.debug("Could not import apply_handover_for_leave (ok during startup): %s", e)
            apply_handover_for_leave = None  # type: ignore

        if created:
            # 1) Block dates immediately for task assignment and recurrence logic
            try:
                leave_blocked.send(
                    sender=LeaveRequest,
                    employee_id=lr.employee_id,
                    dates=_ist_dates_covered(lr),
                    leave_id=lr.id,
                )
            except Exception:
                logger.exception("Failed emitting leave_blocked for leave #%s", lr.id)

            # 2) Immediately apply handover so dashboards reflect instantly
            if apply_handover_for_leave:
                try:
                    moved = apply_handover_for_leave(lr)
                    if moved:
                        logger.info("Leave #%s: applied handover on create; tasks moved=%s", lr.id, moved)
                except Exception:
                    logger.exception("Leave #%s: failed to apply handover on create", lr.id)
            return

        # For updates: check status transitions
        prev_status = getattr(lr, "_prev_status", None)

        # If status changed to APPROVED, re-apply handover (idempotent safety)
        if prev_status != lr.status and lr.status == LeaveStatus.APPROVED:
            if apply_handover_for_leave:
                try:
                    moved = apply_handover_for_leave(lr)
                    if moved:
                        logger.info("Leave #%s: applied handover on approval; tasks moved=%s", lr.id, moved)
                except Exception:
                    logger.exception("Leave #%s: failed to apply handover on approval", lr.id)

        # If status changed to REJECTED, unblock dates
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
            from apps.leave.services.notifications import send_leave_request_email  # noqa: WPS433

            for lr in to_update:
                lr.reporting_person = new_rp
                lr.cc_person = new_cc
                lr.save(update_fields=["reporting_person", "cc_person", "updated_at"])

                # Resend to the new recipients (bypass recent-duplicate suppression)
                send_leave_request_email(
                    lr,
                    manager_email=(new_rp.email or None),
                    cc_list=[new_cc.email] if getattr(new_cc, "email", None) else [],
                    force=True,  # ensure resend after mapping changes
                )

                logger.info(
                    "Rerouted & resent leave #%s to %s (cc=%s) after ApproverMapping change.",
                    lr.id, getattr(new_rp, "email", "-"), getattr(new_cc, "email", "-"),
                )
        except Exception:
            logger.exception("Failed handling ApproverMapping change.")
