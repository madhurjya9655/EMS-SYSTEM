# apps/leave/views.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import timedelta, date
from zoneinfo import ZoneInfo

from django.apps import apps as django_apps
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views import View
from django.views.decorators.http import require_GET, require_POST

from apps.users.permissions import has_permission
from apps.users.routing import recipients_for_leave
from .forms import LeaveRequestForm
from .models import (
    LeaveRequest,
    LeaveStatus,
    LeaveType,
    ApproverMapping,
    LeaveHandover,
    HandoverTaskType,
)

# Import notification services
try:
    from .services.notifications import send_leave_decision_email, send_leave_request_email, send_handover_email
except Exception:
    send_leave_decision_email = None
    send_leave_request_email = None
    send_handover_email = None

# Import audit models
try:
    from .models import LeaveDecisionAudit, DecisionAction
except Exception:
    LeaveDecisionAudit = None
    DecisionAction = None

# Import Celery tasks
try:
    from .tasks import send_leave_emails_async, send_handover_emails_async
except Exception:
    send_leave_emails_async = None
    send_handover_emails_async = None

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
TOKEN_SALT = "leave-action-v1"
TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7  # 7 days


# -----------------------------------------------------------------------------#
# Helpers                                                                      #
# -----------------------------------------------------------------------------#
def now_ist():
    """Return current time localized to IST."""
    return timezone.localtime(timezone.now(), IST)


def _model_has_field(model, name: str) -> bool:
    try:
        model._meta.get_field(name)
        return True
    except Exception:
        return False


def _employee_header(user) -> Dict[str, Optional[str]]:
    """
    Build employee header safely without assuming Profile fields exist.
    Returns: name, email, designation, department, photo_url
    """
    header = {
        "name": (getattr(user, "get_full_name", lambda: "")() or user.username or "").strip(),
        "email": (user.email or "").strip(),
        "designation": "",
        "department": "",
        "photo_url": None,
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

        if _model_has_field(Profile, "photo"):
            photo = getattr(prof, "photo", None)
            if photo and getattr(photo, "url", None):
                header["photo_url"] = photo.url
        return header
    except Exception:
        logger.exception("Failed to load Profile for user id=%s", getattr(user, "id", None))
        return header


def _routing_for_leave(leave: LeaveRequest) -> Tuple[str, List[str]]:
    """
    Resolve routing from admin-controlled mapping.
    returns (manager_email, cc_list) — all lowercased.
    """
    emp_email = (leave.employee_email or getattr(leave.employee, "email", "") or "").strip().lower()
    r = recipients_for_leave(emp_email)  # {"to": manager, "cc": [..]}
    manager_email = (r.get("to") or "").strip().lower()
    cc_list = [e.strip().lower() for e in (r.get("cc") or []) if e]
    return manager_email, cc_list


def _role_for_email(leave: LeaveRequest, email: str) -> Optional[str]:
    """
    Classify an email for a given leave as "manager" | "cc" | None,
    based on the *current* admin mapping (tokens should honor latest mapping).
    """
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
    """
    Decision is restricted to:
      • superuser, OR
      • the assigned reporting_person.
    (CC is notify-only by spec.)
    """
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


# ---- blocked days math (IST, inclusive) -------------------------------------#
def _datespan_ist(start_dt, end_dt) -> List[date]:
    """Inclusive list of IST dates between start_dt and end_dt (order agnostic)."""
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
    span = _datespan_ist(leave.start_at, leave.end_at)
    days_in_year = [d for d in span if d.year == year]
    if not days_in_year:
        return 0.0
    if leave.is_half_day and len(set(days_in_year)) == 1:
        return 0.5
    return float(len(set(days_in_year)))


def _blocked_days_total_ist(leave: LeaveRequest) -> float:
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
    Build leave balance summary per type for current IST calendar year.
    Returns (rows, total_remaining).
    """
    year = now_ist().year
    rows: List[BalanceRow] = []
    types = list(LeaveType.objects.all().order_by("name"))

    approved = (
        LeaveRequest.objects.filter(employee=user, status=LeaveStatus.APPROVED)
        .select_related("leave_type")
        .only("start_at", "end_at", "is_half_day", "leave_type_id")
    )
    used_by_type: Dict[int, float] = {}
    for lr in approved:
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


# -----------------------------------------------------------------------------#
# Views                                                                         #
# -----------------------------------------------------------------------------#
@has_permission("leave_list")
@login_required
def dashboard(request: HttpRequest) -> HttpResponse:
    header = _employee_header(request.user)
    now = now_ist()

    leaves = (
        LeaveRequest.objects.filter(employee=request.user)
        .select_related("leave_type", "approver", "reporting_person")
        .order_by("-applied_at")
    )

    for lr in leaves:
        try:
            if getattr(lr, "blocked_days", None) in (None, 0):
                setattr(lr, "blocked_days", _blocked_days_total_ist(lr))
        except Exception:
            setattr(lr, "blocked_days", _blocked_days_total_ist(lr))

    balances, _ = _leave_balances_for_user(request.user)

    return render(
        request,
        "leave/dashboard.html",
        {
            "employee_header": header,
            "leaves": leaves,
            "balances": balances,
            "now_ist": now,
        },
    )


@has_permission("leave_apply")
@login_required
def apply_leave(request: HttpRequest) -> HttpResponse:
    """Employee applies for leave with CC and handover functionality."""
    header = _employee_header(request.user)
    now = now_ist()

    if request.method == "POST":
        form = LeaveRequestForm(request.POST, request.FILES, user=request.user)
        if form.is_valid():
            try:
                with transaction.atomic():
                    # Save the leave request first
                    lr = form.save(commit=True)

                    # Handle task handovers
                    cd = form.cleaned_data
                    delegate_to = cd.get("delegate_to")
                    ho_msg = (cd.get("handover_message") or "").strip()

                    # Collect task selections (IDs come as strings from MultiChoiceField)
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

                    handovers_created = []
                    if delegate_to and (cl_ids or dg_ids or ht_ids):
                        # Create handover records
                        handovers = []
                        ef_start = lr.start_date
                        ef_end = lr.end_date
                        
                        # Create handovers for each task type
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

                # Send emails after successful database commit using Celery
                def _send_emails():
                    try:
                        # Send leave request emails
                        if send_leave_emails_async:
                            send_leave_emails_async.delay(lr.id)
                        elif send_leave_request_email:
                            _send_leave_emails_sync(lr)
                        
                        # Send handover emails if any handovers were created
                        if handovers_created and send_handover_emails_async:
                            handover_ids = [h.id for h in handovers_created if h.id]
                            if handover_ids:
                                send_handover_emails_async.delay(lr.id, handover_ids)
                        elif handovers_created and send_handover_email:
                            _send_handover_emails_sync(lr, handovers_created)
                            
                    except Exception as e:
                        logger.error(f"Failed to send emails for leave {lr.id}: {e}")

                # Schedule emails after commit
                transaction.on_commit(_send_emails)

                if handovers_created:
                    messages.success(request, f"Leave application submitted with {len(handovers_created)} task handovers. Email notifications are being sent.")
                else:
                    messages.success(request, "Leave application submitted successfully. Email notifications are being sent.")

                return redirect("leave:dashboard")
                
            except Exception as e:
                logger.exception("apply_leave failed to create leave and/or handover")
                messages.error(request, f"Could not submit the leave: {str(e)}. Please try again.")
        else:
            messages.error(request, "Please fix the errors below.")
    else:
        form = LeaveRequestForm(user=request.user)

    return render(
        request,
        "leave/apply_leave.html",
        {
            "form": form,
            "employee_header": header,
            "now_ist": now,
        },
    )


def _send_leave_emails_sync(leave: LeaveRequest):
    """Send leave request emails synchronously (fallback)."""
    try:
        # Get CC users from the saved leave request
        cc_emails = [user.email for user in leave.cc_users.all() if user.email]
        
        # Get routing info
        manager_email = None
        admin_cc_list = []
        
        if leave.reporting_person and leave.reporting_person.email:
            manager_email = leave.reporting_person.email
        
        if leave.cc_person and leave.cc_person.email:
            admin_cc_list.append(leave.cc_person.email)
        
        # Combine admin CC and user-selected CC
        all_cc = list(set(admin_cc_list + cc_emails))
        
        # Send leave request email
        send_leave_request_email(
            leave,
            manager_email=manager_email,
            cc_list=all_cc
        )
        
        # Log email sent
        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT)
            
        logger.info(f"Sent leave request email for leave {leave.id}")
        
    except Exception as e:
        logger.error(f"Failed to send leave emails sync for leave {leave.id}: {e}")


def _send_handover_emails_sync(leave: LeaveRequest, handovers: List[LeaveHandover]):
    """Send handover emails synchronously (fallback)."""
    try:
        # Group handovers by assignee to send one email per person
        assignee_handovers = {}
        for handover in handovers:
            assignee_id = handover.new_assignee.id
            if assignee_id not in assignee_handovers:
                assignee_handovers[assignee_id] = []
            assignee_handovers[assignee_id].append(handover)
        
        # Send email to each assignee
        for assignee_id, user_handovers in assignee_handovers.items():
            try:
                assignee = user_handovers[0].new_assignee
                send_handover_email(leave, assignee, user_handovers)
                
                # Log handover email sent
                if LeaveDecisionAudit and DecisionAction:
                    LeaveDecisionAudit.log(leave, DecisionAction.HANDOVER_EMAIL_SENT, extra={'assignee_id': assignee_id})
                    
            except Exception as e:
                logger.error(f"Failed to send handover email to assignee {assignee_id}: {e}")
                
    except Exception as e:
        logger.error(f"Failed to send handover emails sync for leave {leave.id}: {e}")


@has_permission("leave_list")
@login_required
def my_leaves(request: HttpRequest) -> HttpResponse:
    return redirect("leave:dashboard")


@has_permission("leave_pending_manager")
@login_required
def manager_pending(request: HttpRequest) -> HttpResponse:
    """Manager queue: list PENDING leaves assigned to the logged-in reporting person."""
    leaves = (
        LeaveRequest.objects.filter(reporting_person=request.user, status=LeaveStatus.PENDING)
        .select_related("employee", "leave_type")
        .order_by("start_at")
    )
    return render(
        request,
        "leave/manager_pending.html",
        {"leaves": leaves},
    )


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

    return {
        "leave": leave,
        "employee_full_name": (getattr(emp, "get_full_name", lambda: "")() or emp.username or "").strip(),
        "employee_designation": designation,
        "employee_email": (emp.email or "").strip(),
        "leave_type_name": getattr(leave.leave_type, "name", str(leave.leave_type)),
        "from_ist": timezone.localtime(leave.start_at, IST),
        "to_ist": timezone.localtime(leave.end_at, IST),
        "is_half_day": bool(leave.is_half_day),
        "reason": leave.reason or "",
        "attachment": getattr(leave, "attachment", None),
    }


@has_permission("leave_pending_manager")
@login_required
@require_GET
def approval_page(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Friendly approval page that shows details and exposes Approve/Reject forms.
    Only the assigned Reporting Person (or superuser) can decide.
    """
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
    leave = get_object_or_404(LeaveRequest.objects.select_for_update(), pk=pk)
    if not _can_manage(request.user, leave):
        messages.error(request, "Only the assigned Reporting Person can approve this leave.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    if leave.is_decided:
        messages.info(request, "This leave has already been decided.")
        return redirect(_safe_next_url(request, "leave:manager_pending"))

    comment = (request.POST.get("decision_comment") or "").strip()
    try:
        leave.approve(by_user=request.user, comment=comment)
        messages.success(request, "Leave approved.")
    except ValidationError as e:
        for msg in e.messages:
            messages.error(request, msg)
    except Exception:
        logger.exception("Approve failed for leave %s", leave.pk)
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
        leave.reject(by_user=request.user, comment=comment)
        messages.success(request, "Leave rejected.")
    except ValidationError as e:
        for msg in e.messages:
            messages.error(request, msg)
    except Exception:
        logger.exception("Reject failed for leave %s", leave.pk)
        messages.error(request, "Could not reject the leave. Please try again.")
    return redirect(_safe_next_url(request, "leave:manager_pending"))


# -----------------------------------------------------------------------------#
# Delete functionality                                                          #
# -----------------------------------------------------------------------------#
@has_permission("leave_list")
@login_required
@require_POST
@transaction.atomic
def delete_leave(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Delete a single leave request (only PENDING requests by the owner).
    """
    leave = get_object_or_404(
        LeaveRequest.objects.select_for_update(),
        pk=pk,
        employee=request.user
    )
    
    if leave.status != LeaveStatus.PENDING:
        messages.error(request, "Only pending leave requests can be deleted.")
        return redirect("leave:dashboard")
    
    leave_type = leave.leave_type.name
    start_date = leave.start_at.strftime("%B %d, %Y")
    
    try:
        leave.delete()
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
    """
    Delete multiple leave requests (only PENDING requests by the owner).
    """
    leave_ids = request.POST.getlist('leave_ids')
    
    if not leave_ids:
        messages.error(request, "No leave requests selected for deletion.")
        return redirect("leave:dashboard")
    
    try:
        leave_ids = [int(id_str) for id_str in leave_ids if id_str.isdigit()]
        
        if not leave_ids:
            messages.error(request, "Invalid leave request IDs provided.")
            return redirect("leave:dashboard")
        
        leaves_to_delete = LeaveRequest.objects.select_for_update().filter(
            pk__in=leave_ids,
            employee=request.user,
            status=LeaveStatus.PENDING
        )
        
        deleted_count = leaves_to_delete.count()
        if deleted_count == 0:
            messages.warning(request, "No eligible leave requests found for deletion. Only pending requests can be deleted.")
            return redirect("leave:dashboard")
        
        leaves_to_delete.delete()
        
        if deleted_count == 1:
            messages.success(request, "Successfully deleted 1 leave request.")
        else:
            messages.success(request, f"Successfully deleted {deleted_count} leave requests.")
            
        total_requested = len(leave_ids)
        if deleted_count < total_requested:
            skipped = total_requested - deleted_count
            messages.info(request, f"{skipped} request(s) were skipped (only pending requests can be deleted).")
    
    except ValueError:
        messages.error(request, "Invalid leave request IDs provided.")
    except Exception:
        logger.exception("Failed to bulk delete leave requests")
        messages.error(request, "Failed to delete leave requests. Please try again.")
    
    return redirect("leave:dashboard")


# -----------------------------------------------------------------------------#
# One-click Token Decision (CONFIRMATION + POST)                                #
# -----------------------------------------------------------------------------#
class TokenDecisionView(View):
    """
    GET  -> show confirmation screen (log TOKEN_OPENED)
    POST -> approve/reject (enforce 10:00 IST via model validation), notify employee
    Only the Reporting Person (per current admin mapping) or a superuser may decide.
    CC is notify-only.
    """
    template_confirm = "leave/email_decision_confirm.html"
    template_done = "leave/email_decision_done.html"
    template_used = "leave/email_token_used.html"
    template_error = "leave/email_decision_error.html"

    def _decode(self, raw_token: str):
        try:
            payload = signing.loads(raw_token, salt=TOKEN_SALT, max_age=TOKEN_MAX_AGE_SECONDS)
            return payload
        except signing.BadSignature:
            raise ValueError("Invalid or expired token.")

    def _load(self, token: str):
        payload = self._decode(token)
        leave_id = int(payload.get("leave_id") or 0)
        actor_email = (payload.get("rp_email") or payload.get("actor_email") or payload.get("manager_email") or "").strip().lower()
        leave = get_object_or_404(
            LeaveRequest.objects.select_related("employee", "reporting_person", "leave_type"),
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
        return LeaveDecisionAudit.objects.filter(leave=leave, token_hash=token_hash, token_used=True).exists()

    def get(self, request: HttpRequest, token: str) -> HttpResponse:
        try:
            _payload, actor_email, leave = self._load(token)
        except ValueError as e:
            return render(request, self.template_error, {"message": str(e)}, status=400)

        token_hash = self._token_hash(token)
        if self._token_already_used(leave, token_hash) or leave.is_decided:
            return render(request, self.template_used, {"leave": leave})

        # Only current RP (per latest mapping) may act via email link.
        allowed = False
        role = _role_for_email(leave, actor_email)
        if role == "manager":
            allowed = True
        if request.user.is_authenticated and getattr(request.user, "is_superuser", False):
            allowed = True
        if request.user.is_authenticated and leave.reporting_person_id == getattr(request.user, "id", None):
            allowed = True

        # Audit token open (soft-fail)
        try:
            if LeaveDecisionAudit:
                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(getattr(DecisionAction, "TOKEN_OPENED", "TOKEN_OPENED") if DecisionAction else "TOKEN_OPENED"),
                    decided_by=request.user if request.user.is_authenticated else None,
                    token_hash=token_hash,
                    token_manager_email=actor_email,
                    token_used=False,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={"hint_action": (request.GET.get("a") or "").upper()},
                )
        except Exception:
            logger.exception("Failed to audit TOKEN_OPENED for leave %s", leave.pk)

        hint_action = (request.GET.get("a") or "").upper()
        if hint_action not in ("APPROVED", "REJECTED"):
            hint_action = ""

        ctx = {"leave": leave, "token": token, "allowed": allowed, "hint_action": hint_action}
        return render(request, self.template_confirm, ctx)

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
            _payload, actor_email, leave = self._load(token)
        except ValueError as e:
            return render(request, self.template_error, {"message": str(e)}, status=400)

        if leave.is_decided:
            return render(request, self.template_used, {"leave": leave})

        token_hash = self._token_hash(token)
        if self._token_already_used(leave, token_hash):
            return render(request, self.template_used, {"leave": leave})

        # Authorization
        if not (
            (request.user.is_authenticated and (request.user.is_superuser or leave.reporting_person_id == getattr(request.user, "id", None)))
            or (_role_for_email(leave, actor_email) == "manager")
        ):
            raise PermissionDenied("Only the assigned Reporting Person can decide this leave.")

        # Resolve approver user
        decider_user = request.user if request.user.is_authenticated else None
        if decider_user is None:
            try:
                User = get_user_model()
                decider_user = User.objects.filter(email__iexact=actor_email).first()
            except Exception:
                decider_user = None
        if decider_user is None:
            decider_user = leave.reporting_person

        # Decide
        try:
            if new_status == LeaveStatus.APPROVED:
                leave.approve(by_user=decider_user, comment="Email decision: APPROVED by Reporting Person.")
            else:
                leave.reject(by_user=decider_user, comment="Email decision: REJECTED by Reporting Person.")
        except ValidationError as e:
            msg = next(iter(e.messages), "Action blocked.") if getattr(e, "messages", None) else "Action blocked."
            return render(request, self.template_error, {"message": msg}, status=400)
        except Exception:
            logger.exception("Token decision failed for leave %s", leave.pk)
            return render(request, self.template_error, {"message": "Could not complete the action."}, status=400)

        # Audits
        try:
            if LeaveDecisionAudit:
                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(getattr(DecisionAction, "APPROVED", "APPROVED") if new_status == LeaveStatus.APPROVED else getattr(DecisionAction, "REJECTED", "REJECTED")),
                    decided_by=leave.approver,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={},
                )
                LeaveDecisionAudit.objects.create(
                    leave=leave,
                    action=(getattr(DecisionAction, "TOKEN_APPROVE", "TOKEN_APPROVE") if new_status == LeaveStatus.APPROVED else getattr(DecisionAction, "TOKEN_REJECT", "TOKEN_REJECT")),
                    decided_by=leave.approver,
                    token_hash=token_hash,
                    token_manager_email=actor_email,
                    token_used=True,
                    ip_address=_client_ip(request),
                    user_agent=(request.META.get("HTTP_USER_AGENT") or ""),
                    extra={},
                )
        except Exception:
            logger.exception("Failed to write decision audits for leave %s", leave.pk)

        messages.success(request, f"Leave for {leave.employee.get_full_name() or leave.employee.username} has been {leave.get_status_display()}.")
        return render(request, self.template_done, {"leave": leave})


# -----------------------------------------------------------------------------#
# Profile Photo Upload                                                          #
# -----------------------------------------------------------------------------#
@has_permission("leave_list")
@login_required
@require_POST
def upload_photo(request: HttpRequest) -> HttpResponse:
    file = request.FILES.get("photo")
    if not file:
        messages.error(request, "Please choose an image file to upload.")
        return redirect("leave:dashboard")

    try:
        Profile = django_apps.get_model("users", "Profile")
        if not Profile:
            messages.error(request, "Profile model is not available.")
            return redirect("leave:dashboard")

        prof, _ = Profile.objects.get_or_create(user=request.user)
        if _model_has_field(Profile, "photo"):
            setattr(prof, "photo", file)
            prof.save(update_fields=["photo"])
            messages.success(request, "Profile photo updated.")
        else:
            messages.error(request, "Profile photo field is not configured.")
    except Exception as e:
        logger.exception("Photo upload failed: %s", e)
        messages.error(request, "Could not save photo. Please try again.")

    return redirect("leave:dashboard")


# -----------------------------------------------------------------------------#
# Optional lightweight widget for manager dashboards                            #
# -----------------------------------------------------------------------------#
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
        start = timezone.localtime(lr.start_at, IST).strftime("%b  %d, %I:%M %p")
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
    

# -----------------------------------------------------------------------------#
# Approver Mapping – editor (summary + dedicated field pages)                   #
# -----------------------------------------------------------------------------#
def _user_label(u) -> str:
    if not u:
        return "—"
    name = (getattr(u, "get_full_name", lambda: "")() or u.username or "").strip()
    email = (getattr(u, "email", "") or "").strip()
    return f"{name} ({email})" if email else name


@login_required
def approver_mapping_edit(request: HttpRequest, user_id: int) -> HttpResponse:
    """
    Read-only summary with Edit buttons that open dedicated field pages.
    Only superusers can save changes (on field pages).
    """
    User = get_user_model()
    employee = get_object_or_404(User, pk=user_id)
    mapping = (
        ApproverMapping.objects
        .select_related("employee", "reporting_person", "cc_person")
        .filter(employee=employee)
        .first()
    )

    ctx = {
        "employee": employee,
        "employee_obj": employee,  # alias for templates expecting `employee_obj`
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
    """
    Legacy POST handler (kept for backward compatibility).
    Only superusers may write.
    """
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
    """
    Dedicated page to edit only one mapping field:
      /leave/approver-mapping/<user_id>/edit/reporting/
      /leave/approver-mapping/<user_id>/edit/cc/
    Only superusers can save; others may view.
    """
    field = (field or "").strip().lower()
    if field not in ("reporting", "cc"):
        return HttpResponseBadRequest("Unknown field.")

    User = get_user_model()
    employee = get_object_or_404(User, pk=user_id)

    mapping = ApproverMapping.objects.select_related("employee", "reporting_person", "cc_person") \
                                     .filter(employee=employee).first()

    users_qs = User.objects.filter(is_active=True) \
        .exclude(email__isnull=True).exclude(email__exact="") \
        .only("id", "first_name", "last_name", "username", "email") \
        .order_by("first_name", "last_name", "username")

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
            rp = None  # not used for cc

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
        "employee_obj": employee,  # alias for templates expecting `employee_obj`
        "mapping": mapping,
        "options": options,
        "selected_id": selected_id,
        "field": field,  # "reporting" or "cc"
        "next_url": next_url,
    }
    return render(request, "leave/approver_mapping_field_edit.html", ctx)


@login_required
def approver_mapping_edit_reporting(request: HttpRequest, user_id: int) -> HttpResponse:
    """Wrapper for reporting field edit page to match URL name used by templates."""
    return approver_mapping_edit_field(request, user_id, "reporting")


@login_required
def approver_mapping_edit_cc(request: HttpRequest, user_id: int) -> HttpResponse:
    """Wrapper for CC field edit page to match URL name used by templates."""
    return approver_mapping_edit_field(request, user_id, "cc")
