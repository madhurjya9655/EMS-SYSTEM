# apps/leave/management/commands/backfill_leave_data.py
from __future__ import annotations

import math
from datetime import timedelta
from typing import Iterable, Optional, Tuple

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

try:
    # Django ships pytz by default; use it for consistency with the app
    import pytz
except Exception:  # pragma: no cover
    pytz = None  # type: ignore

IST = pytz.timezone("Asia/Kolkata") if pytz else timezone.get_current_timezone()


def chunked(qs, size: int) -> Iterable[list]:
    buf = []
    for obj in qs.iterator(chunk_size=size):
        buf.append(obj)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _ist_date(dt) -> Optional[timezone.datetime.date]:
    if not dt:
        return None
    try:
        return timezone.localtime(dt, IST).date()
    except Exception:
        try:
            return dt.date()
        except Exception:
            return None


def _compute_blocked_days(leave) -> int:
    """
    Inclusive IST calendar days covered by [start_at, end_at].
    Half-day -> still counted as 1 calendar day in `blocked_days` (matches UI/email spec).
    """
    try:
        s = timezone.localtime(leave.start_at, IST).date()
        # treat end as inclusive by subtracting a microsecond
        e = timezone.localtime(leave.end_at - timedelta(microseconds=1), IST).date()
    except Exception:
        s = leave.start_at.date()
        e = leave.end_at.date()
    if e < s:
        s, e = e, s
    return (e - s).days + 1


class Command(BaseCommand):
    help = (
        "Safely backfill/normalize LeaveRequest data: blocked_days, snapshots, date-only fields, "
        "decided_at, and minimal audits for historical rows.\n\n"
        "Idempotent & chunked; supports dry-run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--only",
            choices=["all", "blocked", "snapshots", "dates", "decided", "audits"],
            default="all",
            help="Limit work to a subset (default: all).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="How many rows to process per DB batch (default: 500).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Compute and log changes without writing to the database.",
        )

    def handle(self, *args, **opts):
        only = opts["only"]
        batch_size = int(opts["batch_size"])
        dry = bool(opts["dry_run"])

        LeaveRequest = apps.get_model("leave", "LeaveRequest")
        LeaveDecisionAudit = apps.get_model("leave", "LeaveDecisionAudit")
        DecisionAction = apps.get_model("leave", "DecisionAction")
        LeaveStatus = apps.get_model("leave", "LeaveStatus")

        total = LeaveRequest.objects.count()
        if total == 0:
            self.stdout.write(self.style.WARNING("No LeaveRequest rows found. Nothing to do."))
            return

        self.stdout.write(self.style.NOTICE(f"Found {total} LeaveRequest rows. Processing in batches of {batch_size} (dry_run={dry})"))

        touched_blocked = 0
        touched_snap = 0
        touched_dates = 0
        touched_decided_at = 0
        audits_created = 0

        def want(section: str) -> bool:
            return only == "all" or only == section

        processed = 0
        for batch in chunked(LeaveRequest.objects.all().order_by("id"), batch_size):
            with transaction.atomic():
                for lr in batch:
                    processed += 1

                    # 1) blocked_days
                    if want("blocked"):
                        try:
                            new_bd = _compute_blocked_days(lr)
                            old_bd = int(getattr(lr, "blocked_days", 0) or 0)
                            if new_bd != old_bd:
                                if not dry:
                                    lr.blocked_days = new_bd
                                    lr.save(update_fields=["blocked_days", "updated_at"] if hasattr(lr, "updated_at") else ["blocked_days"])
                                touched_blocked += 1
                        except Exception:
                            # keep going; log in verbose mode
                            self.stderr.write(f"[blocked] failed for leave id={lr.id}")

                    # 2) snapshot fields (employee_name/email/designation)
                    if want("snapshots"):
                        try:
                            missing = not (lr.employee_name and lr.employee_email)
                            if missing:
                                if not dry:
                                    # call the model's snapshot helper if present
                                    if hasattr(lr, "_snapshot_employee_details"):
                                        lr._snapshot_employee_details()  # type: ignore[attr-defined]
                                        lr.save(update_fields=["employee_name", "employee_email", "employee_designation"])
                                    else:
                                        # fallback: best effort
                                        emp = lr.employee
                                        lr.employee_email = (getattr(emp, "email", "") or "").strip()
                                        name = (getattr(emp, "get_full_name", lambda: "")() or "").strip()
                                        lr.employee_name = name or (emp.username or "")
                                        # designation snapshot if profile has it
                                        try:
                                            prof = getattr(emp, "profile", None)
                                            if prof and getattr(prof, "designation", None):
                                                lr.employee_designation = prof.designation
                                        except Exception:
                                            pass
                                        lr.save(update_fields=["employee_name", "employee_email", "employee_designation"])
                                touched_snap += 1
                        except Exception:
                            self.stderr.write(f"[snapshots] failed for leave id={lr.id}")

                    # 3) legacy date-only fields start_date/end_date
                    if want("dates"):
                        try:
                            sd = _ist_date(lr.start_at)
                            ed = _ist_date(lr.end_at - timedelta(microseconds=1))
                            need = (getattr(lr, "start_date", None) != sd) or (getattr(lr, "end_date", None) != ed)
                            if need and sd and ed:
                                if not dry:
                                    lr.start_date = sd
                                    lr.end_date = ed
                                    lr.save(update_fields=["start_date", "end_date"])
                                touched_dates += 1
                        except Exception:
                            self.stderr.write(f"[dates] failed for leave id={lr.id}")

                    # 4) decided_at for decided rows
                    if want("decided"):
                        try:
                            if lr.status in (getattr(LeaveStatus, "APPROVED", "APPROVED"), getattr(LeaveStatus, "REJECTED", "REJECTED")) and not lr.decided_at:
                                if not dry:
                                    lr.decided_at = timezone.now()
                                    lr.save(update_fields=["decided_at"])
                                touched_decided_at += 1
                        except Exception:
                            self.stderr.write(f"[decided] failed for leave id={lr.id}")

                    # 5) minimal audits (APPLIED once; decision if decided and missing)
                    if want("audits") and LeaveDecisionAudit and DecisionAction:
                        try:
                            has_any = LeaveDecisionAudit.objects.filter(leave=lr).exists()
                            if not has_any:
                                if not dry:
                                    LeaveDecisionAudit.objects.create(
                                        leave=lr,
                                        action=getattr(DecisionAction, "APPLIED", "APPLIED"),
                                        decided_by=getattr(lr, "employee", None),
                                        extra={
                                            "start_at_ist": timezone.localtime(lr.start_at, IST).strftime("%Y-%m-%d %H:%M IST"),
                                            "end_at_ist": timezone.localtime(lr.end_at, IST).strftime("%Y-%m-%d %H:%M IST"),
                                        },
                                    )
                                audits_created += 1

                            # decision audit if decided & not present
                            if lr.status in (getattr(LeaveStatus, "APPROVED", "APPROVED"), getattr(LeaveStatus, "REJECTED", "REJECTED")):
                                want_action = getattr(DecisionAction, "APPROVED", "APPROVED") if lr.status == getattr(LeaveStatus, "APPROVED", "APPROVED") else getattr(DecisionAction, "REJECTED", "REJECTED")
                                has_decision = LeaveDecisionAudit.objects.filter(leave=lr, action=want_action).exists()
                                if not has_decision:
                                    if not dry:
                                        LeaveDecisionAudit.objects.create(
                                            leave=lr,
                                            action=want_action,
                                            decided_by=getattr(lr, "approver", None),
                                            extra={"decision_comment": getattr(lr, "decision_comment", "")},
                                        )
                                    audits_created += 1
                        except Exception:
                            self.stderr.write(f"[audits] failed for leave id={lr.id}")

            # End batch
            self.stdout.write(f"Processed {processed}/{total}")

        # Summary
        self.stdout.write(self.style.SUCCESS("=== Backfill Summary ==="))
        if want("blocked"):
            self.stdout.write(f"blocked_days updated : {touched_blocked}")
        if want("snapshots"):
            self.stdout.write(f"employee snapshots   : {touched_snap}")
        if want("dates"):
            self.stdout.write(f"start/end dates set  : {touched_dates}")
        if want("decided"):
            self.stdout.write(f"decided_at backfilled: {touched_decided_at}")
        if want("audits"):
            self.stdout.write(f"audits created       : {audits_created}")
        self.stdout.write(self.style.SUCCESS("Done."))
