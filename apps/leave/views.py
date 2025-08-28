from __future__ import annotations

import logging
from typing import Dict, Optional

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_POST

from apps.users.permissions import has_permission
from .forms import LeaveRequestForm
from .models import LeaveRequest, LeaveStatus

logger = logging.getLogger(__name__)


# ---- helpers ---------------------------------------------------------------

def _employee_header(user) -> Dict[str, Optional[str]]:
    """
    Pull employee display info from Profile (imported from the sheet).
    Returns: dict with name, email, designation, photo_url (optional)
    """
    name = getattr(user, "get_full_name", lambda: "")() or user.get_username()
    email = (user.email or "").strip()
    designation = ""
    photo_url: Optional[str] = None

    try:
        from apps.users.models import Profile  # type: ignore
        prof = Profile.objects.select_related("team_leader").filter(user=user).first()
        if prof:
            designation = (getattr(prof, "designation", "") or "").strip()
            photo = getattr(prof, "photo", None)
            if photo and getattr(photo, "url", None):
                photo_url = photo.url
    except Exception:
        logger.exception("Failed to load Profile for user id=%s", getattr(user, "pk", None))

    return {"name": name, "email": email, "designation": designation, "photo_url": photo_url}


def _can_manage(request_user, leave: LeaveRequest) -> bool:
    """Allow decision if the current user is the assigned manager or a superuser."""
    if not request_user.is_authenticated:
        return False
    if request_user.is_superuser:
        return True
    return leave.manager_id == request_user.id


# ---- views -----------------------------------------------------------------

@has_permission("leave_apply")
@login_required
def apply_leave(request):
    """
    Employee applies for leave.
    - Shows employee header (photo/name/designation/email) from Profile.
    - On POST: sets employee, PENDING status; model handles snapshots/emails/signals.
    """
    header = _employee_header(request.user)

    if request.method == "POST":
        form = LeaveRequestForm(request.POST, request.FILES)
        if form.is_valid():
            lr: LeaveRequest = form.save(commit=False)
            lr.employee = request.user
            lr.status = LeaveStatus.PENDING  # explicit; model guards too

            # Prefer Profile.team_leader if available (model will also auto-pick if absent)
            try:
                if not lr.manager:
                    from apps.users.models import Profile  # type: ignore
                    prof = Profile.objects.select_related("team_leader").filter(user=request.user).first()
                    if prof and getattr(prof, "team_leader_id", None):
                        lr.manager = prof.team_leader
            except Exception:
                logger.exception("While setting manager from profile")

            lr.save()  # triggers snapshot + email via post_save signal
            messages.success(request, "Leave application submitted.")
            return redirect("leave:my_leaves")
        else:
            messages.error(request, "Please fix the errors below.")
    else:
        form = LeaveRequestForm()

    return render(
        request,
        "leave/apply.html",  # new Bootstrap template we'll add
        {
            "form": form,
            "employee_header": header,
        },
    )


@has_permission("leave_list")
@login_required
def my_leaves(request):
    """
    Show current user's leave requests with status badges and approver when decided.
    """
    leaves = (
        LeaveRequest.objects.filter(employee=request.user)
        .select_related("leave_type", "approver", "manager")
        .order_by("-applied_at")
    )
    return render(
        request,
        "leave/my_leaves.html",
        {
            "leaves": leaves,
        },
    )


@has_permission("leave_pending_manager")
@login_required
def manager_pending(request):
    """
    Manager's queue: show PENDING leaves where current user is the manager.
    """
    leaves = (
        LeaveRequest.objects.filter(manager=request.user, status=LeaveStatus.PENDING)
        .select_related("employee", "leave_type")
        .order_by("start_at")
    )
    return render(
        request,
        "leave/manager_pending.html",
        {"leaves": leaves},
    )


@has_permission("leave_pending_manager")
@login_required
@require_POST
@transaction.atomic
def manager_decide_approve(request, pk: int):
    """
    Approve exactly once. Sets approver, decided_at; post_save will send notifications & task integration.
    """
    leave = get_object_or_404(LeaveRequest.objects.select_for_update(), pk=pk)
    if not _can_manage(request.user, leave):
        messages.error(request, "You are not allowed to approve this leave.")
        return redirect("leave:manager_pending")

    if leave.is_decided:
        messages.info(request, "This leave has already been decided.")
        return redirect("leave:manager_pending")

    leave.status = LeaveStatus.APPROVED
    leave.approver = request.user
    leave.decided_at = timezone.now()
    leave.decision_comment = (request.POST.get("decision_comment") or "").strip()
    leave.save(update_fields=["status", "approver", "decided_at", "decision_comment", "updated_at"])

    messages.success(request, "Leave approved.")
    return redirect("leave:manager_pending")


@has_permission("leave_pending_manager")
@login_required
@require_POST
@transaction.atomic
def manager_decide_reject(request, pk: int):
    """
    Reject exactly once. Sets approver, decided_at; post_save will send notifications.
    """
    leave = get_object_or_404(LeaveRequest.objects.select_for_update(), pk=pk)
    if not _can_manage(request.user, leave):
        messages.error(request, "You are not allowed to reject this leave.")
        return redirect("leave:manager_pending")

    if leave.is_decided:
        messages.info(request, "This leave has already been decided.")
        return redirect("leave:manager_pending")

    comment = (request.POST.get("decision_comment") or "").strip()
    if not comment:
        # encourage a reason on rejection
        comment = "Rejected by manager."

    leave.status = LeaveStatus.REJECTED
    leave.approver = request.user
    leave.decided_at = timezone.now()
    leave.decision_comment = comment
    leave.save(update_fields=["status", "approver", "decided_at", "decision_comment", "updated_at"])

    messages.success(request, "Leave rejected.")
    return redirect("leave:manager_pending")
