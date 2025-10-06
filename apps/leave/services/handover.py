# apps/leave/services/handover.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List, Iterable, Optional
from datetime import datetime

import pytz
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone

from apps.leave.models import LeaveRequest, LeaveHandover

IST = pytz.timezone("Asia/Kolkata")


@dataclass
class HandoverItemDTO:
    task_name: str
    task_type: str
    task_id: int | None
    task_url: str | None
    message: str | None


def _format_ist(dt: datetime | None) -> str:
    """Return compact IST date like '08 Feb 2025'."""
    if not dt:
        return ""
    if timezone.is_aware(dt):
        local = dt.astimezone(IST)
    else:
        local = IST.localize(dt)
    return local.strftime("%d %b %Y")


def _safe_full_name(user) -> str:
    try:
        return (user.get_full_name() or user.username or "").strip()
    except Exception:
        return getattr(user, "username", "") or str(user)


def _duration_days(leave: LeaveRequest) -> float:
    """
    Prefer the model's computed blocked_days (supports half-day as 0.5),
    otherwise compute from ist_dates().
    """
    try:
        if getattr(leave, "blocked_days", None):
            return float(leave.blocked_days)
    except Exception:
        pass
    try:
        days = leave.ist_dates()
        if not days:
            return 0.0
        return 0.5 if (leave.is_half_day and len(days) == 1) else float(len(days))
    except Exception:
        return 0.0


def _items_from_handovers(handovers: Iterable[LeaveHandover]) -> List[HandoverItemDTO]:
    items: List[HandoverItemDTO] = []
    for ho in handovers:
        # Try model helpers first; fall back to generic label
        try:
            task_title = ho.get_task_title()
        except Exception:
            task_title = f"{ho.get_task_type_display()} #{ho.original_task_id}"
        try:
            task_url = ho.get_task_url()
        except Exception:
            task_url = None

        items.append(
            HandoverItemDTO(
                task_name=task_title,
                task_type=str(ho.task_type),
                task_id=int(ho.original_task_id) if ho.original_task_id is not None else None,
                task_url=task_url or None,
                message=(ho.message or None),
            )
        )
    return items


# -----------------------------------------------------------------------------#
# Apply handover (safe, best-effort)                                           #
# -----------------------------------------------------------------------------#
def _try_apply_via_model_method(ho: LeaveHandover) -> bool:
    """
    If your LeaveHandover model implements a first-class method to do the move,
    call it. We check common method names defensively.
    """
    for meth in ("apply", "apply_handover", "perform", "execute"):
        fn = getattr(ho, meth, None)
        if callable(fn):
            try:
                res = fn()  # should perform reassignment internally
                return bool(res) if res is not None else True
            except Exception:
                import logging
                logging.getLogger(__name__).exception("Handover.%s failed (id=%s)", meth, getattr(ho, "id", None))
    return False


def _try_apply_by_reassigning_task_object(ho: LeaveHandover) -> bool:
    """
    Generic fallback: obtain the task object (if model exposes it) and try to
    set a likely assignee/owner field to the new assignee. This is deliberately
    conservative and wrapped in try/except to never raise.
    """
    import logging
    log = logging.getLogger(__name__)

    task_obj = None
    # Prefer explicit helper if model provides it
    for getter in ("get_task_object", "fetch_task_object", "task_object"):
        fn = getattr(ho, getter, None)
        try:
            task_obj = fn() if callable(fn) else (fn if fn else None)
        except Exception:
            task_obj = None
        if task_obj is not None:
            break
    if task_obj is None:
        # Last-ditch: if model exposes app/model/PK, you could look it up here.
        # We keep it noop to avoid coupling to other apps.
        return False

    # Likely assignee field names in external apps
    candidate_fields = (
        "assignee",
        "assigned_to",
        "owner",
        "user",
        "agent",
        "responsible",
        "handler",
        "staff",
    )
    new_user = getattr(ho, "new_assignee", None)
    if not new_user:
        return False

    for field in candidate_fields:
        if hasattr(task_obj, field):
            try:
                setattr(task_obj, field, new_user)
                # Save with update_fields when possible
                try:
                    task_obj.save(update_fields=[field])
                except Exception:
                    task_obj.save()
                return True
            except Exception:
                log.exception("Failed to set %s on %r for handover id=%s", field, task_obj, getattr(ho, "id", None))

    return False


def apply_handover_for_leave(leave: LeaveRequest) -> int:
    """
    Apply all *active* handovers for the given leave. Returns the count of
    handovers successfully applied (best-effort). Never raises.
    """
    import logging
    log = logging.getLogger(__name__)

    moved = 0
    try:
        handovers = (
            LeaveHandover.objects.filter(leave_request=leave, is_active=True)
            .select_related("new_assignee", "original_assignee")
            .order_by("id")
        )
        now = timezone.now()

        for ho in handovers:
            applied = False

            # 1) Prefer model-native method if available
            try:
                applied = _try_apply_via_model_method(ho)
            except Exception:
                log.exception("Model-method apply failed for handover id=%s", getattr(ho, "id", None))

            # 2) Generic reassignment fallback if still not applied
            if not applied:
                try:
                    applied = _try_apply_by_reassigning_task_object(ho)
                except Exception:
                    log.exception("Generic reassignment failed for handover id=%s", getattr(ho, "id", None))

            # 3) Mark as applied if any approach worked
            if applied:
                try:
                    # Common bookkeeping fields (tolerant if missing)
                    if hasattr(ho, "applied_at"):
                        ho.applied_at = now
                    if hasattr(ho, "is_applied"):
                        ho.is_applied = True
                    # keep is_active=True for the effective period; do not auto-deactivate here
                    ho.save(update_fields=[f for f in ("applied_at", "is_applied", "updated_at") if hasattr(ho, f)])
                except Exception:
                    # Save fallback
                    try:
                        ho.save()
                    except Exception:
                        log.exception("Failed to save post-apply state for handover id=%s", getattr(ho, "id", None))
                moved += 1

    except Exception:
        log.exception("apply_handover_for_leave failed for leave id=%s", getattr(leave, "id", None))

    return moved


# -----------------------------------------------------------------------------#
# Email notifications                                                           #
# -----------------------------------------------------------------------------#
def send_handover_email(
    leave: LeaveRequest,
    assignee,
    handovers: Iterable[LeaveHandover],
    site_url: Optional[str] = None,
) -> None:
    """
    Render and send the "leave_handover" email to a specific assignee, for all
    of their handovers tied to a single LeaveRequest.

    Templates used:
      templates/email/leave_handover.html
      templates/email/leave_handover.txt
    """
    try:
        # Resolve site base URL
        base_url = (site_url or getattr(settings, "SITE_URL", "") or "").rstrip("/")

        # Dates (prefer snapshots on LeaveRequest; they are IST-date aligned)
        start_dt = getattr(leave, "start_at", None)
        end_dt = getattr(leave, "end_at", None)
        start_at_ist = _format_ist(start_dt)
        end_at_ist = _format_ist(end_dt)

        items = _items_from_handovers(handovers)

        ctx = {
            "assignee_name": _safe_full_name(assignee),
            "employee_name": _safe_full_name(leave.employee),
            "employee_email": (getattr(leave, "employee_email", None) or getattr(leave.employee, "email", "") or ""),
            "leave_type": getattr(leave.leave_type, "name", str(leave.leave_type)),
            "start_at_ist": start_at_ist,
            "end_at_ist": end_at_ist,
            "duration_days": _duration_days(leave),
            "is_half_day": bool(getattr(leave, "is_half_day", False)),
            "handover_message": (getattr(leave, "handover_message", None) or ""),  # form may store message separately
            "handovers": [asdict(i) for i in items],
            "site_url": base_url,
        }

        subject = f"Task Handover: {_safe_full_name(leave.employee)} → You ({ctx['start_at_ist']} to {ctx['end_at_ist']})"

        text_body = render_to_string("email/leave_handover.txt", ctx)
        html_body = render_to_string("email/leave_handover.html", ctx)

        to_email = getattr(assignee, "email", None)
        if not to_email:
            return  # no recipient address — silently skip

        from_email = (
            getattr(settings, "DEFAULT_FROM_EMAIL", None)
            or getattr(settings, "SERVER_EMAIL", None)
            or "no-reply@example.com"
        )

        msg = EmailMultiAlternatives(subject, text_body, from_email, [to_email])
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=True)

    except Exception:
        # Never raise to caller — mirror your defensive pattern
        import logging
        logging.getLogger(__name__).exception("Failed sending handover email for leave id=%s", getattr(leave, "id", None))
