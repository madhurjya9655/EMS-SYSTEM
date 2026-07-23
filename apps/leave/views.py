# D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\leave\views.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta, date, time as dtime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from django import forms
from django.apps import apps as django_apps
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import ManyToManyField, Q
from django.db.utils import OperationalError
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views import View
from django.views.decorators.http import require_GET, require_POST

from apps.users.permissions import has_permission
from apps.users.routing import recipients_for_leave
from .forms import LeaveEmailSettingsForm, LeaveRequestForm
from .models import (
    ApproverMapping,
    CCConfiguration,
    HandoverTaskType,
    LeaveEmailSettings,
    LeaveHandover,
    LeaveRequest,
    LeaveStatus,
    LeaveType,
)
from .utils import (
    get_admin_leave_balance_rows,
    get_employee_leave_balance_summary,
)

try:
    from .services.notifications import (
        send_handover_email,
        send_leave_decision_email,
        send_leave_request_email,
    )
except Exception:
    send_handover_email = None
    send_leave_decision_email = None
    send_leave_request_email = None

try:
    from .models import DecisionAction, LeaveDecisionAudit
except Exception:
    LeaveDecisionAudit = None
    DecisionAction = None

try:
    from .tasks import send_handover_emails_async, send_leave_emails_async
except Exception:
    send_leave_emails_async = None
    send_handover_emails_async = None

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

TOKEN_SALT = "leave-action-v1"
TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7

ALLOWED_LEAVE_TYPE_NAMES = {
    "Compensatory Off",
    "Casual Leave",
    "Maternity Leave",
}

# Leave counts as active for deduction/blocking as soon as it is applied.
ACTIVE_DEDUCTION_STATUSES = {
    LeaveStatus.PENDING,
    LeaveStatus.APPROVED,
}

WORK_START_IST = dtime(9, 30)
WORK_END_IST = dtime(18, 0)

PROFILE_PHOTO_FIELD_CANDIDATES = (
    "photo",
    "profile_photo",
    "profile_image",
    "avatar",
    "image",
    "picture",
)

MAX_UPLOAD_BYTES = 10 * 1024 * 1024


def now_ist():
    return timezone.localtime(timezone.now(), IST)


def _within_apply_window_ist(dt=None) -> bool:
    cur = timezone.localtime(dt or timezone.now(), IST).time()
    return (cur >= WORK_START_IST) and (cur <= WORK_END_IST)


def _model_has_field(model, name: str) -> bool:
    try:
        model._meta.get_field(name)
        return True
    except Exception:
        return False


def _detect_profile_photo_field(Profile) -> Optional[str]:
    for fname in PROFILE_PHOTO_FIELD_CANDIDATES:
        if _model_has_field(Profile, fname):
            return fname
    return None


def _get_profile_photo_url_safe(user) -> Optional[str]:
    try:
        Profile = django_apps.get_model("users", "Profile")
        if not Profile:
            return None
        prof = Profile.objects.filter(user=user).first()
        if not prof:
            return None
        fname = _detect_profile_photo_field(Profile)
        if not fname:
            return None
        fileobj = getattr(prof, fname, None)
        return getattr(fileobj, "url", None)
    except Exception:
        return None


def _employee_header(user) -> Dict[str, Optional[str]]:
    header = {
        "name": (getattr(user, "get_full_name", lambda: "")() or user.username or "").strip(),
        "email": (user.email or "").strip(),
        "designation": "",
        "department": "",
        "photo_url": _get_profile_photo_url_safe(user),
    }

    try:
        Profile = django_apps.get_model("users", "Profile")
    except Exception:
        return header

    try:
        qs = Profile.objects.filter(user=user)
        if _model_has_field(Profile, "team_leader"):
            qs = qs.select_related("team_leader")
        prof = qs.first()
        if not prof:
            return header

        if _model_has_field(Profile, "designation"):
            header["designation"] = (getattr(prof, "designation", "") or "").strip()

        if _model_has_field(Profile, "department"):
            header["department"] = (getattr(prof, "department", "") or "").strip()

        return header
    except Exception:
        logger.exception("Failed to load Profile for user id=%s", getattr(user, "id", None))
        return header


def _routing_for_leave(leave: LeaveRequest) -> Tuple[str, List[str]]:
    emp_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip().lower()
    r = recipients_for_leave(emp_email)
    manager_email = (r.get("to") or "").strip().lower()
    cc_list = [e.strip().lower() for e in (r.get("cc") or []) if e]
    return manager_email, cc_list


def _role_for_email(leave: LeaveRequest, email: str) -> Optional[str]:
    if not email:
        return None
    email = email.strip().lower()
    manager_email, cc_list = _routing_for_leave(leave)
    if email == manager_email:
        return "manager"
    if email in cc_list:
        return "cc"
    return None


def _can_manage(request_user, leave: LeaveRequest) -> bool:
    if not getattr(request_user, "is_authenticated", False):
        return False
    if getattr(request_user, "is_superuser", False):
        return True
    return leave.reporting_person_id == getattr(request_user, "id", None)


def _safe_next_url(request: HttpRequest, default_name: str) -> str:
    nxt = (request.GET.get("next") or request.POST.get("next") or "").strip()
    if nxt.startswith("/"):
        return nxt
    try:
        return reverse(default_name)
    except Exception:
        return "/"


def _client_ip(request: HttpRequest) -> Optional[str]:
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


def _auto_skip_tasks_for_leave(leave: LeaveRequest, *, exclude_handover: bool = True) -> Dict[str, int]:
    """
    Soft-skip already-created tasks that fall inside a PENDING or APPROVED leave window.

    New production rule:
    - Leave blocks tasks immediately after apply.
    - PENDING + APPROVED leave blocks tasks.
    - REJECTED leave does not block tasks.
    - Deleted leave does not block tasks.
    - Full-day leave blocks tasks on covered IST dates.
    - Half-day leave blocks tasks inside exact leave time window.
    - Never hard-delete tasks.
    - Only set is_skipped_due_to_leave=True.
    """
    counts = {"checklist": 0, "delegation": 0, "help_ticket": 0, "fms": 0}

    try:
        leave_status = getattr(leave, "status", None)

        if leave_status not in ACTIVE_DEDUCTION_STATUSES:
            logger.info(
                "Auto-skip ignored for leave %s because status=%s is not active.",
                getattr(leave, "id", None),
                leave_status,
            )
            return counts

        if not getattr(leave, "employee_id", None):
            logger.warning(
                "Auto-skip ignored for leave %s because employee_id is missing.",
                getattr(leave, "id", None),
            )
            return counts

        start_at = getattr(leave, "start_at", None)
        end_at = getattr(leave, "end_at", None)

        if not (start_at and end_at):
            logger.warning(
                "Auto-skip ignored for leave %s because start_at/end_at missing.",
                getattr(leave, "id", None),
            )
            return counts

        start_ist = timezone.localtime(start_at, IST)
        end_ist = timezone.localtime(end_at, IST)

        if end_ist < start_ist:
            start_ist, end_ist = end_ist, start_ist

        start_date = getattr(leave, "start_date", None) or start_ist.date()
        end_date = getattr(leave, "end_date", None) or end_ist.date()

        if end_date < start_date:
            start_date, end_date = end_date, start_date

        is_half_day = bool(getattr(leave, "is_half_day", False))

        from apps.tasks.models import Checklist, Delegation, FMS, HelpTicket

        exclude_ids = {"checklist": [], "delegation": [], "help_ticket": []}

        if exclude_handover:
            try:
                handovers = (
                    LeaveHandover.objects
                    .filter(leave_request=leave, is_active=True)
                    .only("task_type", "original_task_id")
                )

                for ho in handovers:
                    task_type = str(getattr(ho, "task_type", "") or "")
                    if task_type in exclude_ids:
                        exclude_ids[task_type].append(ho.original_task_id)

            except Exception:
                logger.exception(
                    "Could not resolve handover exclusions for leave %s.",
                    getattr(leave, "id", None),
                )

        if is_half_day:
            datetime_window = Q(planned_date__gte=start_ist) & Q(planned_date__lt=end_ist)
        else:
            datetime_window = Q(planned_date__date__gte=start_date) & Q(planned_date__date__lte=end_date)

        checklist_qs = (
            Checklist.objects
            .filter(
                assign_to=leave.employee,
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
                assign_to=leave.employee,
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
                assign_to=leave.employee,
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

        try:
            if not is_half_day:
                counts["fms"] = int(
                    FMS.objects
                    .filter(
                        assign_to=leave.employee,
                        planned_date__gte=start_date,
                        planned_date__lte=end_date,
                        is_skipped_due_to_leave=False,
                    )
                    .update(is_skipped_due_to_leave=True)
                )
        except Exception:
            logger.exception(
                "FMS auto-skip failed for leave %s.",
                getattr(leave, "id", None),
            )

        logger.info(
            "Auto-skip applied for leave %s status=%s half_day=%s counts=%s.",
            getattr(leave, "id", None),
            leave_status,
            is_half_day,
            counts,
        )

    except Exception:
        logger.exception(
            "Auto-skip failed for leave %s.",
            getattr(leave, "id", None),
        )

    return counts

def _restore_leave_skips_for_employee(employee) -> Dict[str, int]:
    """
    Restore tasks previously skipped due to leave for one employee.

    Simple meaning:
    - Set is_skipped_due_to_leave=False
    - Only for this employee
    - This prepares system for fresh recalculation
    """
    counts = {"checklist": 0, "delegation": 0, "help_ticket": 0, "fms": 0}

    try:
        if not employee:
            return counts

        from apps.tasks.models import Checklist, Delegation, FMS, HelpTicket

        counts["checklist"] = int(
            Checklist.objects
            .filter(assign_to=employee, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["delegation"] = int(
            Delegation.objects
            .filter(assign_to=employee, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["help_ticket"] = int(
            HelpTicket.objects
            .filter(assign_to=employee, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        counts["fms"] = int(
            FMS.objects
            .filter(assign_to=employee, is_skipped_due_to_leave=True)
            .update(is_skipped_due_to_leave=False)
        )

        logger.info(
            "Restored leave-skipped tasks for employee %s counts=%s.",
            getattr(employee, "id", None),
            counts,
        )

    except Exception:
        logger.exception(
            "Failed to restore leave-skipped tasks for employee %s.",
            getattr(employee, "id", None),
        )

    return counts


def _resync_leave_task_skips_for_employee(employee, *, exclude_handover: bool = True) -> Dict[str, object]:
    """
    Recalculate leave-based task skipping for one employee.

    Simple meaning:
    Step 1: Bring back all tasks skipped because of leave.
    Step 2: Look at current active leaves.
    Step 3: Skip tasks again for active leave periods.

    Active leaves:
    - PENDING
    - APPROVED

    Inactive leaves:
    - REJECTED
    - deleted
    """
    result = {
        "restored": {"checklist": 0, "delegation": 0, "help_ticket": 0, "fms": 0},
        "applied": {},
        "active_leave_ids": [],
    }

    try:
        if not employee:
            return result

        restored_counts = _restore_leave_skips_for_employee(employee)
        result["restored"] = restored_counts

        active_leaves = (
            LeaveRequest.objects
            .filter(
                employee=employee,
                status__in=ACTIVE_DEDUCTION_STATUSES,
            )
            .select_related("employee")
            .order_by("start_at", "id")
        )

        for active_leave in active_leaves:
            skip_counts = _auto_skip_tasks_for_leave(
                active_leave,
                exclude_handover=exclude_handover,
            )
            result["applied"][active_leave.id] = skip_counts
            result["active_leave_ids"].append(active_leave.id)

        logger.info(
            "Re-synced leave task skips for employee %s active_leave_ids=%s result=%s.",
            getattr(employee, "id", None),
            result["active_leave_ids"],
            result,
        )

    except Exception:
        logger.exception(
            "Failed to re-sync leave task skips for employee %s.",
            getattr(employee, "id", None),
        )

    return result

def _datespan_ist(start_dt, end_dt) -> List[date]:
    if not (start_dt and end_dt):
        return []
    s = timezone.localtime(start_dt, IST).date()
    e = timezone.localtime(end_dt, IST).date()
    if e < s:
        s, e = e, s
    out: List[date] = []
    cur = s
    while cur <= e:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _blocked_days_in_year_ist(leave: LeaveRequest, year: int) -> float:
    """
    Deduction helper for leave-type balance rows.

    BUSINESS RULE:
    - Full-day leave deducts from balance
    - Half-day leave does NOT deduct from yearly 24 balance
    - Deduct on apply, so PENDING + APPROVED count
    - REJECTED / deleted should not count
    """
    if leave.status not in ACTIVE_DEDUCTION_STATUSES:
        return 0.0

    span = _datespan_ist(leave.start_at, leave.end_at)
    days_in_year = [d for d in span if d.year == year]
    if not days_in_year:
        return 0.0

    if leave.is_half_day:
        return 0.0

    return float(len(set(days_in_year)))


def _blocked_days_total_ist(leave: LeaveRequest) -> float:
    """
    Display helper only.

    This shows how many calendar days are blocked by the leave entry itself.
    Half-day is displayed as 0.5 for user clarity, even though it does NOT
    deduct from yearly paid leave quota.
    """
    span = _datespan_ist(leave.start_at, leave.end_at)
    if not span:
        return 0.0
    if leave.is_half_day and len(set(span)) == 1:
        return 0.5
    return float(len(set(span)))


@dataclass
class BalanceRow:
    type_name: str
    default_days: int
    used_days: float
    remaining_days: float


def _leave_balances_for_user(user) -> Tuple[List[BalanceRow], float]:
    """
    Per-leave-type display rows used on dashboard.

    CURRENT BUSINESS RULE PRESERVED:
    - Deduct on apply.
    - PENDING + APPROVED count.
    - Half-day does NOT deduct from yearly quota.

    IMPORTANT:
    Master annual available balance comes from:
        get_employee_leave_balance_summary(user)

    This function is kept only for old template compatibility where the dashboard
    expects "balances" rows by leave type.

    Do not use this function as source of truth for final available leave.
    """
    year = now_ist().year
    rows: List[BalanceRow] = []

    types = list(
        LeaveType.objects
        .filter(name__in=ALLOWED_LEAVE_TYPE_NAMES)
        .order_by("name")
    )

    active_leaves = (
        LeaveRequest.objects
        .filter(
            employee=user,
            status__in=ACTIVE_DEDUCTION_STATUSES,
        )
        .select_related("leave_type")
        .only(
            "start_at",
            "end_at",
            "is_half_day",
            "leave_type_id",
            "status",
        )
    )

    used_by_type: Dict[int, float] = {}

    for lr in active_leaves:
        if not lr.leave_type_id:
            continue

        used = _blocked_days_in_year_ist(lr, year)

        if used <= 0:
            continue

        used_by_type[lr.leave_type_id] = used_by_type.get(lr.leave_type_id, 0.0) + used

    total_remaining = 0.0

    for lt in types:
        used = used_by_type.get(lt.id, 0.0)
        remaining = max(float(lt.default_days) - used, 0.0)
        total_remaining += remaining

        rows.append(
            BalanceRow(
                type_name=lt.name,
                default_days=lt.default_days,
                used_days=used,
                remaining_days=remaining,
            )
        )

    return rows, total_remaining

@has_permission("leave_list")
@login_required
def dashboard(request: HttpRequest) -> HttpResponse:
    header = _employee_header(request.user)
    now = now_ist()

    is_admin_or_manager = (
        getattr(request.user, "is_superuser", False)
        or getattr(request.user, "is_staff", False)
    )

    viewed_user = request.user
    all_users: List = []
    selected_user_id: str = ""

    if is_admin_or_manager:
        User = get_user_model()

        all_users = list(
            User.objects
            .filter(is_active=True)
            .only("id", "first_name", "last_name", "username", "email")
            .order_by("first_name", "last_name", "username")
        )

        raw_uid = (request.GET.get("user_id") or "").strip()
        selected_user_id = raw_uid

        if raw_uid:
            try:
                viewed_user = get_object_or_404(
                    User.objects.only(
                        "id",
                        "first_name",
                        "last_name",
                        "username",
                        "email",
                    ),
                    pk=int(raw_uid),
                    is_active=True,
                )
            except (ValueError, TypeError):
                viewed_user = request.user
                selected_user_id = ""

    is_viewing_other = viewed_user.pk != request.user.pk

    leaves = (
        LeaveRequest.objects
        .filter(employee=viewed_user)
        .select_related("leave_type", "approver", "reporting_person")
        .order_by("-applied_at", "-id")
    )

    for lr in leaves:
        try:
            if getattr(lr, "blocked_days", None) in (None, 0):
                setattr(lr, "blocked_days", _blocked_days_total_ist(lr))
        except Exception:
            setattr(lr, "blocked_days", _blocked_days_total_ist(lr))

    pending_count = approved_count = rejected_count = 0

    for lr in leaves:
        if lr.status == LeaveStatus.PENDING:
            pending_count += 1
        elif lr.status == LeaveStatus.APPROVED:
            approved_count += 1
        elif lr.status == LeaveStatus.REJECTED:
            rejected_count += 1

    # Old per-leave-type rows retained for backward template compatibility.
    # Do NOT treat this as source of truth for final available balance.
    balances, _legacy_total_remaining = _leave_balances_for_user(viewed_user)

    # Source of truth for dashboard available leave.
    # This uses EmployeeLeaveBalance + carry_forward_adjustment.
    annual_leave_balance = get_employee_leave_balance_summary(viewed_user)

    available_leave_balance = annual_leave_balance.remaining_paid_leaves
    total_paid_leaves = annual_leave_balance.total_paid_leaves
    paid_leaves_taken = annual_leave_balance.paid_leaves_taken
    unpaid_leaves = annual_leave_balance.unpaid_leaves
    carry_forward_adjustment = annual_leave_balance.carry_forward_adjustment

    return render(
        request,
        "leave/dashboard.html",
        {
            "employee_header": header,
            "leaves": leaves,

            # Existing context keys retained.
            "balances": balances,
            "annual_leave_balance": annual_leave_balance,

            # New explicit balance keys for templates.
            "available_leave_balance": available_leave_balance,
            "total_paid_leaves": total_paid_leaves,
            "paid_leaves_taken": paid_leaves_taken,
            "unpaid_leaves": unpaid_leaves,
            "carry_forward_adjustment": carry_forward_adjustment,

            "now_ist": now,
            "is_admin_or_manager": is_admin_or_manager,
            "is_viewing_other": is_viewing_other,
            "viewed_user": viewed_user,
            "all_users": all_users,
            "selected_user_id": selected_user_id,
            "pending_count": pending_count,
            "approved_count": approved_count,
            "rejected_count": rejected_count,
        },
    )

@has_permission("leave_apply")
@login_required
def apply_leave(request: HttpRequest) -> HttpResponse:
    header = _employee_header(request.user)
    now = now_ist()
    can_apply_now = None

    if request.method == "POST":
        form = LeaveRequestForm(request.POST, request.FILES, user=request.user)
        try:
            if form.is_valid():
                max_attempts = 5
                base_sleep = 0.25
                handovers_created: List[LeaveHandover] = []

                for attempt in range(1, max_attempts + 1):
                    try:
                        with transaction.atomic():
                            try:
                                lr = form.save(commit=True)
                            except ValidationError as ve:
                                for msg in getattr(ve, "messages", []) or [str(ve)]:
                                    messages.error(request, msg)
                                break

                            cd = form.cleaned_data
                            delegate_to = cd.get("delegate_to")
                            ho_msg = (cd.get("handover_message") or "").strip()

                            def _to_int_list(vals):
                                out: List[int] = []
                                for v in (vals or []):
                                    try:
                                        out.append(int(v))
                                    except (TypeError, ValueError):
                                        continue
                                return out

                            cl_ids = _to_int_list(cd.get("handover_checklist"))
                            dg_ids = _to_int_list(cd.get("handover_delegation"))
                            ht_ids = _to_int_list(cd.get("handover_help_ticket"))

                            if delegate_to and (cl_ids or dg_ids or ht_ids):
                                handovers = []
                                ef_start = getattr(lr, "start_date", None) or timezone.localtime(lr.start_at, IST).date()
                                ef_end = getattr(lr, "end_date", None) or timezone.localtime(lr.end_at, IST).date()

                                for tid in cl_ids:
                                    handovers.append(
                                        LeaveHandover(
                                            leave_request=lr,
                                            original_assignee=request.user,
                                            new_assignee=delegate_to,
                                            task_type=HandoverTaskType.CHECKLIST,
                                            original_task_id=tid,
                                            message=ho_msg,
                                            effective_start_date=ef_start,
                                            effective_end_date=ef_end,
                                            is_active=True,
                                        )
                                    )
                                for tid in dg_ids:
                                    handovers.append(
                                        LeaveHandover(
                                            leave_request=lr,
                                            original_assignee=request.user,
                                            new_assignee=delegate_to,
                                            task_type=HandoverTaskType.DELEGATION,
                                            original_task_id=tid,
                                            message=ho_msg,
                                            effective_start_date=ef_start,
                                            effective_end_date=ef_end,
                                            is_active=True,
                                        )
                                    )
                                for tid in ht_ids:
                                    handovers.append(
                                        LeaveHandover(
                                            leave_request=lr,
                                            original_assignee=request.user,
                                            new_assignee=delegate_to,
                                            task_type=HandoverTaskType.HELP_TICKET,
                                            original_task_id=tid,
                                            message=ho_msg,
                                            effective_start_date=ef_start,
                                            effective_end_date=ef_end,
                                            is_active=True,
                                        )
                                    )

                                if handovers:
                                    handovers_created = LeaveHandover.objects.bulk_create(handovers, ignore_conflicts=True)

                            skip_counts = _auto_skip_tasks_for_leave(lr, exclude_handover=True)
                            logger.info(
                            "Leave %s applied with status=%s. Immediate task auto-skip counts=%s.",
                            getattr(lr, "status", None),
                            lr.id,
                            skip_counts,
                               )

                            def _apply_and_send_noop():
                                try:
                                    logger.info(
                                        "Post-commit: relying on model/signals to apply handover & send emails for leave %s",
                                        lr.id,
                                    )
                                except Exception as e:
                                    logger.error("Post-commit hook (noop) failed for leave %s: %s", lr.id, e)

                            transaction.on_commit(_apply_and_send_noop)

                        if handovers_created:
                            messages.success(
                                request,
                                f"Leave application submitted with {len(handovers_created)} task handovers. Email notifications are being sent.",
                            )
                        else:
                            messages.success(
                                request,
                                "Leave application submitted successfully. Email notifications are being sent.",
                            )
                        return redirect("leave:dashboard")

                    except OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_attempts:
                            sleep_s = base_sleep * (2 ** (attempt - 1))
                            logger.warning(
                                "SQLite busy on apply_leave (attempt %s/%s); retrying in %.2fs.",
                                attempt,
                                max_attempts,
                                sleep_s,
                            )
                            time.sleep(sleep_s)
                            continue
                        logger.exception("apply_leave failed due to OperationalError on attempt %s", attempt)
                        messages.error(request, "Database is busy. Please try again.")
                        break

                    except ValidationError as ve:
                        for msg in getattr(ve, "messages", []) or [str(ve)]:
                            messages.error(request, msg)
                        break

                    except Exception as e:
                        logger.exception("apply_leave failed to create leave and/or handover: %s", e)
                        messages.error(
                            request,
                            "Could not submit the leave due to an unexpected error. Please review the form and try again.",
                        )
                        break

            else:
                try:
                    logger.error("LeaveRequestForm invalid: %s", form.errors.as_json())
                except Exception:
                    logger.error("LeaveRequestForm invalid (errors could not be serialized).")
                messages.error(request, "Please fix the errors below.")
        except Exception:
            logger.exception("apply_leave crashed during validation stage")
            messages.error(request, "Something went wrong while validating your request. Please try again.")
    else:
        form = LeaveRequestForm(user=request.user)

    return render(
        request,
        "leave/apply_leave.html",
        {
            "form": form,
            "employee_header": header,
            "now_ist": now,
            "can_apply_now": can_apply_now,
        },
    )


@has_permission("leave_list")
@login_required
def my_leaves(request: HttpRequest) -> HttpResponse:
    leaves = (
        LeaveRequest.objects
        .filter(employee=request.user)
        .select_related("leave_type", "approver")
        .order_by("-applied_at", "-id")
    )

    annual_leave_balance = get_employee_leave_balance_summary(request.user)

    return render(
        request,
        "leave/my_leaves.html",
        {
            "leaves": leaves,
            "annual_leave_balance": annual_leave_balance,

            # Explicit balance keys for template safety.
            "available_leave_balance": annual_leave_balance.remaining_paid_leaves,
            "total_paid_leaves": annual_leave_balance.total_paid_leaves,
            "paid_leaves_taken": annual_leave_balance.paid_leaves_taken,
            "unpaid_leaves": annual_leave_balance.unpaid_leaves,
            "carry_forward_adjustment": annual_leave_balance.carry_forward_adjustment,
        },
    )

@has_permission("leave_pending_manager")
@login_required
def manager_pending(request: HttpRequest) -> HttpResponse:
    leaves = (
        LeaveRequest.objects.filter(reporting_person=request.user, status=LeaveStatus.PENDING)
        .select_related("employee", "leave_type")
        .order_by("start_at")
    )

    managed_user_ids = list(
        ApproverMapping.objects.filter(reporting_person=request.user)
        .values_list("employee_id", flat=True)
    )

    pending_employee_ids = list({leave.employee_id for leave in leaves})
    all_employee_ids = list(set(managed_user_ids) | set(pending_employee_ids))

    User = get_user_model()
    users = User.objects.filter(id__in=all_employee_ids).order_by(
        "first_name", "last_name", "username"
    )
    employee_leave_balances = get_admin_leave_balance_rows(users=users)

    ctx = {
        "leaves": leaves,
        "employee_leave_balances": employee_leave_balances,
        "next_url": reverse("leave:manager_pending"),
    }
    return render(request, "leave/pending_leaves.html", ctx)


def _build_approval_context(leave: LeaveRequest) -> Dict[str, object]:
    emp = leave.employee
    designation = ""

    try:
        Profile = django_apps.get_model("users", "Profile")

        if Profile:
            prof = Profile.objects.filter(user=emp).first()

            if prof and getattr(prof, "designation", None):
                designation = prof.designation or ""

    except Exception:
        designation = ""

    annual_leave_balance = get_employee_leave_balance_summary(
        emp,
        leave.start_date,
    )

    return {
        "leave": leave,
        "employee_full_name": (
            getattr(emp, "get_full_name", lambda: "")()
            or emp.username
            or ""
        ).strip(),
        "employee_designation": designation,
        "employee_email": (emp.email or "").strip(),
        "leave_type_name": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "from_ist": timezone.localtime(leave.start_at, IST),
        "to_ist": timezone.localtime(leave.end_at, IST),
        "is_half_day": bool(leave.is_half_day),
        "reason": leave.reason or "",
        "attachment": getattr(leave, "attachment", None),

        # Source of truth for manager approval page.
        "annual_leave_balance": annual_leave_balance,
        "available_leave_balance": annual_leave_balance.remaining_paid_leaves,
        "total_paid_leaves": annual_leave_balance.total_paid_leaves,
        "paid_leaves_taken": annual_leave_balance.paid_leaves_taken,
        "unpaid_leaves": annual_leave_balance.unpaid_leaves,
        "carry_forward_adjustment": annual_leave_balance.carry_forward_adjustment,
    }


@has_permission("leave_pending_manager")
@login_required
@require_GET
def approval_page(request: HttpRequest, pk: int) -> HttpResponse:
    leave = get_object_or_404(LeaveRequest, pk=pk)
    if not _can_manage(request.user, leave):
        return HttpResponseForbidden("You are not allowed to view this approval page.")
    ctx = _build_approval_context(leave)
    ctx["next_url"] = _safe_next_url(request, "leave:manager_pending")
    return render(request, "leave/approve.html", ctx)


@has_permission("leave_pending_manager")
@login_required
@require_POST
@transaction.atomic
def manager_decide_approve(request: HttpRequest, pk: int) -> HttpResponse:
    leave = get_object_or_404(
        LeaveRequest.objects.select_for_update(),
        pk=pk,
    )

    if not _can_manage(request.user, leave):
        messages.error(request, "Only the assigned Reporting Person can approve this leave.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    if leave.is_decided:
        messages.info(request, "This leave has already been decided.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    comment = (request.POST.get("decision_comment") or "").strip()

    try:
        employee_id = getattr(leave, "employee_id", None)
        leave_id = getattr(leave, "id", None)

        leave.approve(
            by_user=request.user,
            comment=comment,
        )

        def _after_approval_commit():
            logger.info(
                "Leave %s approved by user %s for employee %s. Task skip re-sync is handled by leave signals.",
                leave_id,
                getattr(request.user, "id", None),
                employee_id,
            )

        transaction.on_commit(_after_approval_commit)

        messages.success(request, "Leave approved.")

    except ValidationError as e:
        for msg in e.messages:
            messages.error(request, msg)

    except Exception:
        logger.exception("Approve failed for leave %s", getattr(leave, "pk", None))
        messages.error(request, "Could not approve the leave. Please try again.")

    return redirect(_safe_next_url(request, "leave:manager_pending"))

@has_permission("leave_pending_manager")
@login_required
@require_POST
@transaction.atomic
def manager_decide_reject(request: HttpRequest, pk: int) -> HttpResponse:
    leave = get_object_or_404(LeaveRequest.objects.select_for_update(), pk=pk)
    if not _can_manage(request.user, leave):
        messages.error(request, "Only the assigned Reporting Person can reject this leave.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    if leave.is_decided:
        messages.info(request, "This leave has already been decided.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    comment = (request.POST.get("decision_comment") or "").strip() or "Rejected by manager."
    try:
        employee = leave.employee

        leave.reject(by_user=request.user, comment=comment)

        def _after_reject_commit():
            try:
                resync_counts = _resync_leave_task_skips_for_employee(
                    employee,
                    exclude_handover=True,
                )
                logger.info(
                    "Leave %s rejected. Task skip state re-synced for employee %s result=%s.",
                    leave.id,
                    getattr(employee, "id", None),
                    resync_counts,
                )
            except Exception:
                logger.exception(
                    "Post-reject task skip re-sync failed for leave %s.",
                    getattr(leave, "id", None),
                )

        transaction.on_commit(_after_reject_commit)

        messages.success(request, "Leave rejected.")
    except ValidationError as e:
        for msg in e.messages:
            messages.error(request, msg)
    except Exception:
        logger.exception("Reject failed for leave %s", leave.pk)
        messages.error(request, "Could not reject the leave. Please try again.")
    return redirect(_safe_next_url(request, "leave:manager_pending"))


@has_permission("leave_list")
@login_required
@require_POST
@transaction.atomic
def delete_leave(request: HttpRequest, pk: int) -> HttpResponse:
    leave = get_object_or_404(
        LeaveRequest.objects.select_for_update(),
        pk=pk,
        employee=request.user,
    )
    if leave.status != LeaveStatus.PENDING:
        messages.error(request, "Only pending leave requests can be deleted.")
        return redirect("leave:dashboard")

    leave_type = leave.leave_type.name
    start_date = timezone.localtime(leave.start_at, IST).strftime("%B %d, %Y")

    try:
        employee = leave.employee
        leave_id = leave.id

        leave.delete()

        def _after_delete_commit():
            try:
                resync_counts = _resync_leave_task_skips_for_employee(
                    employee,
                    exclude_handover=True,
                )
                logger.info(
                    "Leave %s deleted. Task skip state re-synced for employee %s result=%s.",
                    leave_id,
                    getattr(employee, "id", None),
                    resync_counts,
                )
            except Exception:
                logger.exception(
                    "Post-delete task skip re-sync failed for leave %s.",
                    leave_id,
                )

        transaction.on_commit(_after_delete_commit)

        messages.success(request, f"Successfully deleted {leave_type} leave request for {start_date}.")
    except Exception:
        logger.exception("Failed to delete leave request %s", pk)
        messages.error(request, "Failed to delete leave request. Please try again.")

    return redirect("leave:dashboard")


@has_permission("leave_list")
@login_required
@require_POST
@transaction.atomic
def bulk_delete_leaves(request: HttpRequest) -> HttpResponse:
    leave_ids = request.POST.getlist("leave_ids")

    if not leave_ids:
        messages.error(request, "No leave requests selected for deletion.")
        return redirect("leave:dashboard")

    try:
        leave_ids = [
            int(id_str)
            for id_str in leave_ids
            if str(id_str).isdigit()
        ]

        if not leave_ids:
            messages.error(request, "Invalid leave request IDs provided.")
            return redirect("leave:dashboard")

        leaves_to_delete = (
            LeaveRequest.objects
            .select_for_update()
            .filter(
                pk__in=leave_ids,
                employee=request.user,
                status=LeaveStatus.PENDING,
            )
        )

        deleted_count = leaves_to_delete.count()

        if deleted_count == 0:
            messages.warning(
                request,
                "No eligible leave requests found for deletion. Only pending requests can be deleted.",
            )
            return redirect("leave:dashboard")

        deleted_leave_ids = list(
            leaves_to_delete.values_list("id", flat=True)
        )

        affected_employee_ids = list(
            leaves_to_delete
            .values_list("employee_id", flat=True)
            .distinct()
        )

        leaves_to_delete.delete()

        def _after_bulk_delete_commit():
            logger.info(
                "Bulk leave delete completed. deleted_leave_ids=%s affected_employee_ids=%s deleted_count=%s. Task skip re-sync is handled by leave signals.",
                deleted_leave_ids,
                affected_employee_ids,
                deleted_count,
            )

        transaction.on_commit(_after_bulk_delete_commit)

        if deleted_count == 1:
            messages.success(request, "Successfully deleted 1 leave request.")
        else:
            messages.success(
                request,
                f"Successfully deleted {deleted_count} leave requests.",
            )

        total_requested = len(leave_ids)

        if deleted_count < total_requested:
            skipped = total_requested - deleted_count
            messages.info(
                request,
                f"{skipped} request(s) were skipped because only pending requests can be deleted.",
            )

    except ValueError:
        messages.error(request, "Invalid leave request IDs provided.")

    except Exception:
        logger.exception("Failed to bulk delete leave requests")
        messages.error(
            request,
            "Failed to delete leave requests. Please try again.",
        )

    return redirect("leave:dashboard")

class TokenDecisionView(View):
    template_confirm = "leave/email_decision_confirm.html"
    template_done = "leave/email_decision_done.html"
    template_used = "leave/email_token_used.html"
    template_error = "leave/email_decision_error.html"

    def _decode(self, raw_token: str):
        try:
            payload = signing.loads(
                raw_token,
                salt=TOKEN_SALT,
                max_age=TOKEN_MAX_AGE_SECONDS,
            )
            return payload
        except signing.BadSignature:
            raise ValueError("Invalid or expired token.")

    def _load(self, token: str):
        payload = self._decode(token)

        leave_id = int(payload.get("leave_id") or 0)

        actor_email = (
            payload.get("rp_email")
            or payload.get("actor_email")
            or payload.get("manager_email")
            or payload.get("approver_email")
            or ""
        ).strip().lower()

        leave = get_object_or_404(
            LeaveRequest.objects.select_related(
                "employee",
                "reporting_person",
                "approver",
                "cc_person",
                "leave_type",
            ).prefetch_related(
                "cc_users",
            ),
            pk=leave_id,
        )

        return payload, actor_email, leave

    def _token_hash(self, token: str) -> str:
        if not LeaveDecisionAudit:
            return ""
        return LeaveDecisionAudit.hash_token(token)

    def _token_already_used(self, leave: LeaveRequest, token_hash: str) -> bool:
        if not LeaveDecisionAudit:
            return False

        return LeaveDecisionAudit.objects.filter(
            leave=leave,
            token_hash=token_hash,
            token_used=True,
        ).exists()

    def _norm_email(self, value) -> str:
        return (value or "").strip().lower()

    def _token_actor_user_id(self, payload):
        raw_user_id = (
            payload.get("rp_user_id")
            or payload.get("actor_user_id")
            or payload.get("manager_user_id")
            or payload.get("approver_user_id")
            or payload.get("user_id")
        )

        if not raw_user_id:
            return None

        try:
            return int(raw_user_id)
        except (TypeError, ValueError):
            return None

    def _user_matches_token_actor(self, user, payload, actor_email: str) -> bool:
        if not user:
            return False

        user_email = self._norm_email(getattr(user, "email", ""))

        if actor_email and user_email and user_email == actor_email:
            return True

        actor_user_id = self._token_actor_user_id(payload)

        if actor_user_id and int(getattr(user, "id", 0) or 0) == actor_user_id:
            return True

        return False

    def _get_authorized_decider_user(self, request: HttpRequest, leave: LeaveRequest, payload, actor_email: str):
        """
        Public email approval authorization.

        Valid approvers:
        1. Logged-in superuser
        2. Logged-in reporting person
        3. Token actor email/user id matches leave.reporting_person
        4. Token actor email/user id matches leave.approver, if already assigned
        5. Token actor email/user id matches current ApproverMapping.reporting_person
        6. Backward compatibility: old _role_for_email() says manager

        IMPORTANT:
        This intentionally does NOT depend only on _role_for_email(),
        because _role_for_email() uses routing lookup and can return None even when
        leave.reporting_person is correctly stored in DB.
        """

        if request.user.is_authenticated:
            if getattr(request.user, "is_superuser", False):
                return request.user

            if leave.reporting_person_id == getattr(request.user, "id", None):
                return request.user

        if self._user_matches_token_actor(getattr(leave, "reporting_person", None), payload, actor_email):
            return leave.reporting_person

        if self._user_matches_token_actor(getattr(leave, "approver", None), payload, actor_email):
            return leave.approver

        try:
            mapping = (
                ApproverMapping.objects
                .select_related("reporting_person", "cc_person")
                .prefetch_related("default_cc_users")
                .filter(employee=leave.employee)
                .first()
            )

            if mapping:
                if self._user_matches_token_actor(getattr(mapping, "reporting_person", None), payload, actor_email):
                    return mapping.reporting_person

                # Keep this only for projects where cc_person is treated as approval authority.
                if self._user_matches_token_actor(getattr(mapping, "cc_person", None), payload, actor_email):
                    return mapping.cc_person

                try:
                    default_cc_user = mapping.default_cc_users.filter(
                        email__iexact=actor_email,
                        is_active=True,
                    ).first()

                    if default_cc_user:
                        return default_cc_user

                except Exception:
                    logger.exception(
                        "Failed checking default_cc_users for leave %s.",
                        getattr(leave, "pk", None),
                    )

        except Exception:
            logger.exception(
                "Failed checking ApproverMapping for leave %s.",
                getattr(leave, "pk", None),
            )

        if _role_for_email(leave, actor_email) == "manager":
            try:
                User = get_user_model()
                actor_user = User.objects.filter(
                    email__iexact=actor_email,
                    is_active=True,
                ).first()

                if actor_user:
                    return actor_user

            except Exception:
                logger.exception(
                    "Failed loading fallback token actor user for leave %s.",
                    getattr(leave, "pk", None),
                )

            return leave.reporting_person

        return None

    def _is_allowed(self, request: HttpRequest, leave: LeaveRequest, payload, actor_email: str) -> bool:
        return self._get_authorized_decider_user(
            request=request,
            leave=leave,
            payload=payload,
            actor_email=actor_email,
        ) is not None

    def get(self, request: HttpRequest, token: str) -> HttpResponse:
        try:
            payload, actor_email, leave = self._load(token)
        except ValueError as e:
            return render(
                request,
                self.template_error,
                {"message": str(e)},
                status=400,
            )

        token_hash = self._token_hash(token)

        if self._token_already_used(leave, token_hash) or leave.is_decided:
            return render(
                request,
                self.template_used,
                {"leave": leave},
            )

        allowed = self._is_allowed(
            request=request,
            leave=leave,
            payload=payload,
            actor_email=actor_email,
        )

        try:
            if LeaveDecisionAudit:
                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(
                        getattr(DecisionAction, "TOKEN_OPENED", "TOKEN_OPENED")
                        if DecisionAction
                        else "TOKEN_OPENED"
                    ),
                    decided_by=request.user if request.user.is_authenticated else None,
                    token_hash=token_hash,
                    token_manager_email=actor_email,
                    token_used=False,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={
                        "hint_action": (request.GET.get("a") or "").upper(),
                        "allowed": allowed,
                        "actor_email": actor_email,
                        "leave_reporting_person_id": leave.reporting_person_id,
                        "leave_reporting_person_email": (
                            leave.reporting_person.email
                            if leave.reporting_person
                            else ""
                        ),
                    },
                )
        except Exception:
            logger.exception(
                "Failed to audit TOKEN_OPENED for leave %s",
                leave.pk,
            )

        hint_action = (request.GET.get("a") or "").upper()

        if hint_action not in ("APPROVED", "REJECTED"):
            hint_action = ""

        ctx = {
            "leave": leave,
            "token": token,
            "allowed": allowed,
            "hint_action": hint_action,
        }

        return render(
            request,
            self.template_confirm,
            ctx,
        )

    @transaction.atomic
    def post(self, request: HttpRequest, token: str) -> HttpResponse:
        raw_action = (request.POST.get("action") or "").strip().lower()

        if raw_action in ("approve", "approved"):
            new_status = LeaveStatus.APPROVED
        elif raw_action in ("reject", "rejected"):
            new_status = LeaveStatus.REJECTED
        else:
            return HttpResponseBadRequest("Invalid action.")

        try:
            payload, actor_email, leave = self._load(token)
        except ValueError as e:
            return render(
                request,
                self.template_error,
                {"message": str(e)},
                status=400,
            )

        if leave.is_decided:
            return render(
                request,
                self.template_used,
                {"leave": leave},
            )

        token_hash = self._token_hash(token)

        if self._token_already_used(leave, token_hash):
            return render(
                request,
                self.template_used,
                {"leave": leave},
            )

        decider_user = self._get_authorized_decider_user(
            request=request,
            leave=leave,
            payload=payload,
            actor_email=actor_email,
        )

        if decider_user is None:
            raise PermissionDenied("You are not authorized to decide this leave via link.")

        try:
            employee = leave.employee

            if new_status == LeaveStatus.APPROVED:
                leave.approve(
                    by_user=decider_user,
                    comment="Email decision: APPROVED by approval authority.",
                )
            else:
                leave.reject(
                    by_user=decider_user,
                    comment="Email decision: REJECTED by approval authority.",
                )

            def _after_token_decision_commit():
                try:
                    resync_counts = _resync_leave_task_skips_for_employee(
                        employee,
                        exclude_handover=True,
                    )

                    logger.info(
                        "Email decision completed for leave %s new_status=%s. "
                        "Task skip state re-synced for employee %s result=%s.",
                        leave.id,
                        new_status,
                        getattr(employee, "id", None),
                        resync_counts,
                    )

                except Exception:
                    logger.exception(
                        "Post-email-decision task skip re-sync failed for leave %s.",
                        getattr(leave, "id", None),
                    )

            transaction.on_commit(_after_token_decision_commit)

        except ValidationError as e:
            msg = (
                next(iter(e.messages), "Action blocked.")
                if getattr(e, "messages", None)
                else "Action blocked."
            )

            return render(
                request,
                self.template_error,
                {"message": msg},
                status=400,
            )

        except Exception:
            logger.exception(
                "Token decision failed for leave %s",
                leave.pk,
            )

            return render(
                request,
                self.template_error,
                {"message": "Could not complete the action."},
                status=400,
            )

        try:
            if LeaveDecisionAudit:
                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(
                        getattr(DecisionAction, "APPROVED", "APPROVED")
                        if new_status == LeaveStatus.APPROVED
                        else getattr(DecisionAction, "REJECTED", "REJECTED")
                    ),
                    decided_by=decider_user,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={
                        "source": "email_token",
                        "actor_email": actor_email,
                    },
                )

                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(
                        getattr(DecisionAction, "TOKEN_APPROVE", "TOKEN_APPROVE")
                        if new_status == LeaveStatus.APPROVED
                        else getattr(DecisionAction, "TOKEN_REJECT", "TOKEN_REJECT")
                    ),
                    decided_by=decider_user,
                    token_hash=token_hash,
                    token_manager_email=actor_email,
                    token_used=True,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={
                        "source": "email_token",
                        "actor_email": actor_email,
                    },
                )

        except Exception:
            logger.exception(
                "Failed to write decision audits for leave %s",
                leave.pk,
            )

        messages.success(
            request,
            f"Leave for {leave.employee.get_full_name() or leave.employee.username} "
            f"has been {leave.get_status_display()}.",
        )

        return render(
            request,
            self.template_done,
            {"leave": leave},
        )


@has_permission("leave_list")
@login_required
@require_POST
def upload_photo(request: HttpRequest) -> HttpResponse:
    file = request.FILES.get("photo")
    if not file:
        messages.error(request, "Please choose an image file to upload.")
        return redirect("leave:dashboard")

    ctype = (getattr(file, "content_type", "") or "").lower()
    if not ctype.startswith("image/"):
        messages.error(request, "Only image files are allowed.")
        return redirect("leave:dashboard")
    if getattr(file, "size", 0) > MAX_UPLOAD_BYTES:
        messages.error(request, "Image too large. Maximum allowed is 10 MB.")
        return redirect("leave:dashboard")

    try:
        Profile = django_apps.get_model("users", "Profile")
        if not Profile:
            messages.error(request, "Profile model is not available.")
            return redirect("leave:dashboard")

        field_name = _detect_profile_photo_field(Profile)
        if not field_name:
            messages.error(
                request,
                "Profile photo field is not configured on users.Profile. "
                "Add an ImageField named one of: "
                + ", ".join(PROFILE_PHOTO_FIELD_CANDIDATES),
            )
            return redirect("leave:dashboard")

        prof, _ = Profile.objects.get_or_create(user=request.user)
        setattr(prof, field_name, file)
        try:
            prof.save(update_fields=[field_name])
        except Exception:
            prof.save()

        messages.success(request, "Profile photo updated.")
    except Exception as e:
        logger.exception("Photo upload failed: %s", e)
        messages.error(
            request,
            "Could not save photo. Ensure MEDIA_ROOT is writable and Pillow is installed.",
        )

    return redirect("leave:dashboard")


@has_permission("leave_pending_manager")
@login_required
def manager_widget(request: HttpRequest) -> HttpResponse:
    leaves = (
        LeaveRequest.objects.filter(reporting_person=request.user, status=LeaveStatus.PENDING)
        .select_related("employee", "leave_type")
        .order_by("start_at")[:10]
    )
    rows = []
    for lr in leaves:
        start = timezone.localtime(lr.start_at, IST).strftime("%b %d, %I:%M %p")
        rows.append(
            f"<tr><td>{lr.employee.get_full_name() or lr.employee.username}</td>"
            f"<td>{lr.leave_type.name}</td>"
            f"<td>{start}</td>"
            f"<td>"
            f"<a class='btn btn-sm btn-outline-primary' href='{reverse('leave:approval_page', args=[lr.id])}'>Open</a> "
            f"<form method='post' action='{reverse('leave:manager_decide_approve', args=[lr.id])}?next={_safe_next_url(request, 'leave:manager_pending')}' style='display:inline'>{_csrf_input(request)}"
            f"<button class='btn btn-sm btn-success'>Approve</button></form> "
            f"<form method='post' action='{reverse('leave:manager_decide_reject', args=[lr.id])}?next={_safe_next_url(request, 'leave:manager_pending')}' style='display:inline'>{_csrf_input(request)}"
            f"<button class='btn btn-sm btn-danger'>Reject</button></form>"
            f"</td></tr>"
        )
    html = (
        "<div class='card'><div class='card-body'>"
        "<h6 class='mb-2'>Pending Leaves</h6>"
        "<div class='table-responsive'><table class='table table-sm'><thead>"
        "<tr><th>Employee</th><th>Type</th><th>Start (IST)</th><th>Action</th></tr>"
        "</thead><tbody>"
        + ("".join(rows) if rows else "<tr><td colspan='4' class='text-muted'>No pending leaves.</td></tr>")
        + "</tbody></table></div></div></div>"
    )
    return HttpResponse(html)


def _csrf_input(request: HttpRequest) -> str:
    from django.middleware.csrf import get_token

    try:
        token = get_token(request)
        return f"<input type='hidden' name='csrfmiddlewaretoken' value='{token}'>"
    except Exception:
        return ""


def _user_label(u) -> str:
    if not u:
        return "—"
    name = (getattr(u, "get_full_name", lambda: "")() or u.username or "").strip()
    email = (getattr(u, "email", "") or "").strip()
    return f"{name} ({email})" if email else name


@login_required
def approver_mapping_edit(request: HttpRequest, user_id: int) -> HttpResponse:
    User = get_user_model()
    employee = get_object_or_404(User, pk=user_id)
    mapping = (
        ApproverMapping.objects.select_related("employee", "reporting_person", "cc_person").filter(employee=employee).first()
    )

    ctx = {
        "employee": employee,
        "employee_obj": employee,
        "mapping": mapping,
        "reporting_label": _user_label(getattr(mapping, "reporting_person", None)) if mapping else "—",
        "cc_label": _user_label(getattr(mapping, "cc_person", None)) if mapping else "—",
        "next_url": _safe_next_url(request, "recruitment:employee_list"),
    }
    return render(request, "leave/approver_mapping_edit.html", ctx)


@login_required
@require_POST
@transaction.atomic
def approver_mapping_save(request: HttpRequest) -> HttpResponse:
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Only administrators can modify approver mappings.")

    try:
        employee_id = int(request.POST.get("employee_id") or 0)
    except Exception:
        return HttpResponseBadRequest("Invalid employee id.")

    rp_id = request.POST.get("reporting_person_id") or ""
    cc_id = request.POST.get("cc_person_id") or ""
    next_url = _safe_next_url(request, "recruitment:employee_list")

    User = get_user_model()
    employee = get_object_or_404(User, pk=employee_id)

    reporting_person = None
    if rp_id.strip():
        reporting_person = get_object_or_404(User, pk=int(rp_id))

    cc_person = None
    if cc_id.strip():
        cc_person = get_object_or_404(User, pk=int(cc_id))

    mapping, _created = ApproverMapping.objects.select_for_update().get_or_create(employee=employee)
    mapping.reporting_person = reporting_person
    mapping.cc_person = cc_person
    mapping.save()

    messages.success(request, "Approver mapping saved.")
    return redirect(next_url)


@login_required
def approver_mapping_edit_field(request: HttpRequest, user_id: int, field: str) -> HttpResponse:
    field = (field or "").strip().lower()
    if field not in ("reporting", "cc"):
        return HttpResponseBadRequest("Unknown field.")

    User = get_user_model()
    employee = get_object_or_404(User, pk=user_id)

    mapping = (
        ApproverMapping.objects.select_related("employee", "reporting_person", "cc_person").filter(employee=employee).first()
    )

    users_qs = (
        User.objects.filter(is_active=True)
        .exclude(email__isnull=True)
        .exclude(email__exact="")
        .only("id", "first_name", "last_name", "username", "email")
        .order_by("first_name", "last_name", "username")
    )

    options = list(users_qs)

    selected_id = None
    if mapping:
        selected_id = mapping.reporting_person_id if field == "reporting" else mapping.cc_person_id

    next_url = _safe_next_url(request, "recruitment:employee_list")
    back_url = reverse("leave:approver_mapping_edit", args=[employee.id])
    if not next_url or next_url == "/":
        next_url = back_url

    if request.method == "POST":
        if not getattr(request.user, "is_superuser", False):
            return HttpResponseForbidden("Only administrators can modify approver mappings.")

        if field == "reporting":
            chosen = (request.POST.get("reporting_person_id") or request.POST.get("chosen_id") or "").strip()
            if not chosen:
                messages.error(request, "Reporting person is required.")
                return redirect(request.path + f"?next={next_url}")
            rp = get_object_or_404(User, pk=int(chosen))
        else:
            chosen = (request.POST.get("cc_person_id") or request.POST.get("chosen_id") or "").strip()
            rp = None

        mapping, _ = ApproverMapping.objects.select_for_update().get_or_create(employee=employee)
        if field == "reporting":
            mapping.reporting_person = rp
        else:
            mapping.cc_person = None if not chosen else get_object_or_404(User, pk=int(chosen))
        mapping.save()

        messages.success(request, "Approver mapping updated.")
        return redirect(back_url + (f"?next={next_url}" if next_url else ""))

    ctx = {
        "employee": employee,
        "employee_obj": employee,
        "mapping": mapping,
        "options": options,
        "selected_id": selected_id,
        "field": field,
        "next_url": next_url,
    }
    return render(request, "leave/approver_mapping_field_edit.html", ctx)


@login_required
def approver_mapping_edit_reporting(request: HttpRequest, user_id: int) -> HttpResponse:
    return approver_mapping_edit_field(request, user_id, "reporting")


@login_required
def approver_mapping_edit_cc(request: HttpRequest, user_id: int) -> HttpResponse:
    return approver_mapping_edit_field(request, user_id, "cc")


class _CCAddForm(forms.Form):
    user = forms.ModelChoiceField(
        queryset=get_user_model().objects.filter(is_active=True).order_by("first_name", "last_name", "username")
    )

    def clean_user(self):
        u = self.cleaned_data["user"]
        if not getattr(u, "email", ""):
            raise forms.ValidationError("Selected user must have an email.")
        return u


@login_required
def cc_config(request: HttpRequest) -> HttpResponse:
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")
    add_form = _CCAddForm()
    rows = list(
        CCConfiguration.objects.select_related("user").order_by(
            "sort_order", "department", "user__first_name", "user__last_name"
        )
    )

    if request.method == "POST":
        updated = 0
        with transaction.atomic():
            for obj in rows:
                dep = request.POST.get(f"row-{obj.id}-department", "")
                is_active = bool(request.POST.get(f"row-{obj.id}-is_active"))
                try:
                    sort_val = int(request.POST.get(f"row-{obj.id}-sort_order", "0"))
                except Exception:
                    sort_val = 0
                changed = False
                if obj.department != dep:
                    obj.department = dep
                    changed = True
                if obj.is_active != is_active:
                    obj.is_active = is_active
                    changed = True
                if obj.sort_order != sort_val:
                    obj.sort_order = sort_val
                    changed = True
                if changed:
                    obj.save(update_fields=["department", "is_active", "sort_order", "updated_at"])
                    updated += 1
        messages.success(request, f"Saved {updated} row(s).")
        return redirect("leave:cc_config")

    return render(request, "leave/cc_config.html", {"rows": rows, "add_form": add_form})


@login_required
@require_POST
@transaction.atomic
def cc_config_add(request: HttpRequest) -> HttpResponse:
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")
    form = _CCAddForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Please select a valid user.")
        return redirect("leave:cc_config")
    u = form.cleaned_data["user"]
    obj, created = CCConfiguration.objects.get_or_create(
        user=u, defaults={"department": "", "is_active": True, "sort_order": 0}
    )
    if created:
        messages.success(request, "User added to CC options.")
    else:
        messages.info(request, "User already exists in CC options.")
    return redirect("leave:cc_config")


@login_required
@require_POST
@transaction.atomic
def cc_config_remove(request: HttpRequest, pk: int) -> HttpResponse:
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")
    try:
        obj = CCConfiguration.objects.get(pk=pk)
        obj.delete()
        messages.success(request, "Removed from CC options.")
    except CCConfiguration.DoesNotExist:
        messages.info(request, "Entry not found.")
    return redirect("leave:cc_config")


@login_required
def cc_assign(request: HttpRequest) -> HttpResponse:
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")

    User = get_user_model()

    active_cc = list(
        CCConfiguration.objects.filter(is_active=True)
        .select_related("user")
        .order_by("sort_order", "department", "user__first_name", "user__last_name")
    )

    choices: List[Tuple[int, str]] = []
    for opt in active_cc:
        label = opt.display_name or (opt.user.get_full_name() or opt.user.username)
        if opt.department:
            label = f"{label} — {opt.department}"
        choices.append((opt.user.id, label))

    employees = list(
        User.objects.filter(is_active=True)
        .only("id", "first_name", "last_name", "username", "email")
        .order_by("first_name", "last_name", "username")
    )

    mappings = {
        m.employee_id: m
        for m in ApproverMapping.objects.select_related("employee", "cc_person").filter(employee__in=employees)
    }

    has_m2m = hasattr(ApproverMapping, "default_cc_users") and (
        isinstance(getattr(ApproverMapping, "default_cc_users"), ManyToManyField)
        or hasattr(getattr(ApproverMapping, "default_cc_users"), "through")
    )

    if request.method == "POST":
        updated = 0
        with transaction.atomic():
            for emp in employees:
                mapping = mappings.get(emp.id)

                ids_param = f"row-{emp.id}-cc_user_ids"
                clear_param = f"row-{emp.id}-clear"

                if ids_param not in request.POST and clear_param not in request.POST:
                    continue

                if mapping is None:
                    mapping = ApproverMapping.objects.create(employee=emp)
                    mappings[emp.id] = mapping

                if request.POST.get(clear_param) == "1":
                    if has_m2m:
                        mapping.default_cc_users.set([])
                    else:
                        mapping.cc_person = None
                        mapping.save(update_fields=["cc_person", "updated_at"])
                    updated += 1
                    continue

                selected_ids: List[int] = []
                for raw in request.POST.getlist(ids_param):
                    try:
                        selected_ids.append(int(raw))
                    except (TypeError, ValueError):
                        pass

                if has_m2m:
                    mapping.default_cc_users.set(selected_ids)
                    updated += 1
                else:
                    if selected_ids:
                        first_id = selected_ids[0]
                        if mapping.cc_person_id != first_id:
                            mapping.cc_person_id = first_id
                            mapping.save(update_fields=["cc_person", "updated_at"])
                            updated += 1
                    else:
                        if mapping.cc_person_id is not None:
                            mapping.cc_person = None
                            mapping.save(update_fields=["cc_person", "updated_at"])
                            updated += 1

        messages.success(request, f"Updated {updated} employee(s).")
        return redirect("leave:cc_assign")

    id_to_label = dict(choices)
    rows = []
    for emp in employees:
        mapping = mappings.get(emp.id)
        current_ids: List[int] = []
        current_labels: List[str] = []

        if mapping:
            if has_m2m:
                try:
                    current_ids = list(mapping.default_cc_users.values_list("id", flat=True))
                except Exception:
                    current_ids = []
            else:
                if mapping.cc_person_id:
                    current_ids = [mapping.cc_person_id]

        for cid in current_ids:
            lbl = id_to_label.get(cid)
            if not lbl:
                try:
                    u = User.objects.filter(id=cid).only("first_name", "last_name", "username").first()
                    if u:
                        lbl = u.get_full_name() or u.username
                except Exception:
                    lbl = None
            if lbl:
                current_labels.append(lbl)

        rows.append(
            {
                "id": emp.id,
                "name": (emp.get_full_name() or emp.username),
                "email": emp.email,
                "current_cc_ids": current_ids,
                "current_cc_labels": current_labels,
            }
        )

    ctx = {
        "rows": rows,
        "cc_choices": choices,
    }
    return render(request, "leave/cc_assign.html", ctx)


@has_permission("leave_list")
@login_required
def my_handovers_widget(request: HttpRequest) -> HttpResponse:
    from .models import LeaveHandover, LeaveStatus

    today = timezone.localdate()
    handovers = (
        LeaveHandover.objects.select_related("leave_request", "original_assignee")
        .filter(
            new_assignee=request.user,
            is_active=True,
            effective_start_date__lte=today,
            effective_end_date__gte=today,
            leave_request__status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
        )
        .order_by("effective_end_date", "id")
    )

    rows = []
    for ho in handovers:
        title = ho.get_task_title()
        href = ho.get_task_url() or "#"
        rows.append(
            f"<tr>"
            f"<td><a href='{href}'>{title}</a></td>"
            f"<td class='text-nowrap'>{ho.get_task_type_display()}</td>"
            f"<td class='text-nowrap'>{ho.original_assignee.get_full_name() or ho.original_assignee.username}</td>"
            f"<td class='text-nowrap'>{ho.effective_end_date or ''}</td>"
            f"</tr>"
        )

    html = (
        "<div class='card mt-3'><div class='card-body'>"
        "<h6 class='mb-2'>Tasks handed over to you</h6>"
        "<div class='table-responsive'><table class='table table-sm align-middle'>"
        "<thead><tr><th>Task</th><th>Type</th><th>Original Owner</th><th>Handover Ends</th></tr></thead>"
        "<tbody>"
        + ("".join(rows) if rows else "<tr><td colspan='4' class='text-muted'>No active handovers.</td></tr>")
        + "</tbody></table></div></div></div>"
    )
    return HttpResponse(html)

@login_required
def leave_email_settings(request: HttpRequest) -> HttpResponse:
    """
    Allow superusers to manage global Leave TO and CC recipients.

    Saving keeps the recipient rule active immediately. No server restart is
    required because the notification service reads these database settings
    whenever a Leave email is sent.
    """
    if not getattr(request.user, "is_superuser", False):
        raise PermissionDenied("Only superusers can update Leave email settings.")

    settings_obj = LeaveEmailSettings.get_solo()

    if request.method == "POST":
        form = LeaveEmailSettingsForm(request.POST, instance=settings_obj)

        if form.is_valid():
            with transaction.atomic():
                settings_obj = form.save(commit=False)
                settings_obj.is_active = True
                settings_obj.save()
                form.save_m2m()

            messages.success(
                request,
                "Leave TO and CC recipients updated successfully. Changes are active immediately.",
            )
            return redirect("leave:leave_email_settings")
    else:
        form = LeaveEmailSettingsForm(instance=settings_obj)

    return render(
        request,
        "leave/admin/leave_email_settings.html",
        {
            "form": form,
            "email_settings": settings_obj,
            "settings_obj": settings_obj,
        },
    )