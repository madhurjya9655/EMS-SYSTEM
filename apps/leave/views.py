from __future__ import annotations

import logging
import time
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
from django.db.models import ManyToManyField
from django.db.utils import OperationalError
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views import View
from django.views.decorators.http import require_GET, require_POST
from django import forms
from django.conf import settings  # use settings.ENABLE_CELERY_EMAIL

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
    CCConfiguration,
)

# Notification services
try:
    from .services.notifications import send_leave_decision_email, send_leave_request_email, send_handover_email
except Exception:
    send_leave_decision_email = None
    send_leave_request_email = None
    send_handover_email = None

# Audits
try:
    from .models import LeaveDecisionAudit, DecisionAction
except Exception:
    LeaveDecisionAudit = None
    DecisionAction = None

# Celery tasks
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
    header = _employee_header(request.user)
    now = now_ist()

    if request.method == "POST":
        form = LeaveRequestForm(request.POST, request.FILES, user=request.user)
        if form.is_valid():
            # ---- Robust insert: retry entire transaction if SQLite is busy ----
            max_attempts = 5
            base_sleep = 0.25  # seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    with transaction.atomic():
                        lr = form.save(commit=True)

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

                        handovers_created = []
                        if delegate_to and (cl_ids or dg_ids or ht_ids):
                            handovers = []
                            ef_start = lr.start_date
                            ef_end = lr.end_date
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

                        # ---------------- Email dispatch (sync or Celery) ----------------
                        def _send_emails():
                            try:
                                use_async = bool(getattr(settings, "ENABLE_CELERY_EMAIL", False)) and bool(send_leave_emails_async)

                                if use_async:
                                    # Celery path
                                    send_leave_emails_async.delay(lr.id)
                                elif send_leave_request_email:
                                    # Synchronous fallback
                                    _send_leave_emails_sync(lr)

                                if handovers_created:
                                    use_async_ho = bool(getattr(settings, "ENABLE_CELERY_EMAIL", False)) and bool(send_handover_emails_async)
                                    if use_async_ho:
                                        handover_ids = [h.id for h in handovers_created if h.id]
                                        if handover_ids:
                                            send_handover_emails_async.delay(lr.id, handover_ids)
                                    elif send_handover_email:
                                        _send_handover_emails_sync(lr, handovers_created)
                            except Exception as e:
                                logger.error(f"Failed to send emails for leave {lr.id}: {e}")

                        transaction.on_commit(_send_emails)
                        # ----------------------------------------------------------------

                    # If we reached here, the transaction succeeded -> messages + redirect
                    if handovers_created:
                        messages.success(request, f"Leave application submitted with {len(handovers_created)} task handovers. Email notifications are being sent.")
                    else:
                        messages.success(request, "Leave application submitted successfully. Email notifications are being sent.")
                    return redirect("leave:dashboard")

                except OperationalError as e:
                    # Only retry for 'database is locked'
                    if "database is locked" in str(e).lower() and attempt < max_attempts:
                        sleep_s = base_sleep * (2 ** (attempt - 1))
                        logger.warning(f"SQLite busy on apply_leave (attempt {attempt}/{max_attempts}); retrying in {sleep_s:.2f}s.")
                        time.sleep(sleep_s)
                        continue
                    logger.exception("apply_leave failed due to OperationalError on attempt %s", attempt)
                    messages.error(request, "Database is busy. Please try again.")
                    break
                except Exception as e:
                    logger.exception("apply_leave failed to create leave and/or handover")
                    messages.error(request, f"Could not submit the leave: {str(e)}. Please try again.")
                    break
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
    try:
        # Per-request CCs chosen by employee
        cc_emails = [user.email for user in leave.cc_users.all() if user.email]

        # Manager
        manager_email = None
        if leave.reporting_person and leave.reporting_person.email:
            manager_email = leave.reporting_person.email

        # Admin-managed defaults (M2M) + legacy
        admin_cc_list: List[str] = []
        try:
            # Use mapping-based resolver to fetch default CC users (includes legacy if not duplicated)
            _rp, default_cc_users = LeaveRequest.resolve_routing_multi_for(leave.employee)
            admin_cc_list.extend([u.email for u in default_cc_users if getattr(u, "email", None)])
        except Exception:
            pass

        # Additionally include legacy snapshot on the Leave (if present)
        if leave.cc_person and getattr(leave.cc_person, "email", None):
            admin_cc_list.append(leave.cc_person.email)

        # Merge + normalize
        all_cc = []
        seen = set()
        for e in (admin_cc_list + cc_emails):
            if not e:
                continue
            low = e.strip().lower()
            if low and low not in seen:
                seen.add(low)
                all_cc.append(low)

        send_leave_request_email(leave, manager_email=manager_email, cc_list=all_cc)

        if LeaveDecisionAudit and DecisionAction:
            LeaveDecisionAudit.log(leave, DecisionAction.EMAIL_SENT)

        logger.info(f"Sent leave request email for leave {leave.id}")

    except Exception as e:
        logger.error(f"Failed to send leave emails sync for leave {leave.id}: {e}")


def _send_handover_emails_sync(leave: LeaveRequest, handovers: List[LeaveHandover]):
    try:
        assignee_handovers = {}
        for handover in handovers:
            assignee_id = handover.new_assignee.id
            if assignee_id not in assignee_handovers:
                assignee_handovers[assignee_id] = []
            assignee_handovers[assignee_id].append(handover)

        for assignee_id, user_handovers in assignee_handovers.items():
            try:
                assignee = user_handovers[0].new_assignee
                send_handover_email(leave, assignee, user_handovers)
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
    leaves = (
        LeaveRequest.objects.filter(reporting_person=request.user, status=LeaveStatus.PENDING)
        .select_related("employee", "leave_type")
        .order_by("start_at")
    )
    return render(request, "leave/manager_pending.html", {"leaves": leaves})


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

        allowed = False
        role = _role_for_email(leave, actor_email)
        if role == "manager":
            allowed = True
        if request.user.is_authenticated and getattr(request.user, "is_superuser", False):
            allowed = True
        if request.user.is_authenticated and leave.reporting_person_id == getattr(request.user, "id", None):
            allowed = True

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

        if not (
            (request.user.is_authenticated and (request.user.is_superuser or leave.reporting_person_id == getattr(request.user, "id", None)))
            or (_role_for_email(leave, actor_email) == "manager")
        ):
            raise PermissionDenied("Only the assigned Reporting Person can decide this leave.")

        decider_user = request.user if request.user.is_authenticated else None
        if decider_user is None:
            try:
                User = get_user_model()
                decider_user = User.objects.filter(email__iexact=actor_email).first()
            except Exception:
                decider_user = None
        if decider_user is None:
            decider_user = leave.reporting_person

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


# -----------------------------------------------------------------------------#
# CC Config (admin-only) + per-employee CC assignment                           #
# -----------------------------------------------------------------------------#
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
    rows = list(CCConfiguration.objects.select_related("user").order_by("sort_order", "department", "user__first_name", "user__last_name"))

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
    """
    Admin: assign per-employee default CC recipients.

    Supports BOTH:
      • Legacy: ApproverMapping.cc_person (FK)
      • New:    ApproverMapping.default_cc_users (M2M -> User)

    Template uses a multi-select. For legacy FK, only the first selected user
    will be saved (others are ignored). "Clear all CC" works in both cases.
    """
    if not getattr(request.user, "is_superuser", False):
        return HttpResponseForbidden("Admins only.")

    User = get_user_model()

    # Active CC options (choices)
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

    # Employees to show
    employees = list(
        User.objects.filter(is_active=True)
        .only("id", "first_name", "last_name", "username", "email")
        .order_by("first_name", "last_name", "username")
    )

    # Preload mappings
    mappings = {
        m.employee_id: m
        for m in ApproverMapping.objects.select_related("employee", "cc_person").filter(employee__in=employees)
    }

    # Detect presence of M2M field default_cc_users
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

                # If neither key posted => No change for this row
                if ids_param not in request.POST and clear_param not in request.POST:
                    continue

                # Ensure mapping exists
                if mapping is None:
                    mapping = ApproverMapping.objects.create(employee=emp)
                    mappings[emp.id] = mapping

                # Clear all CC
                if request.POST.get(clear_param) == "1":
                    if has_m2m:
                        mapping.default_cc_users.set([])
                    else:
                        mapping.cc_person = None
                        mapping.save(update_fields=["cc_person", "updated_at"])
                    updated += 1
                    continue

                # Set to selected list (may be empty => clears M2M / FK)
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

    # Build rows for template (preselect + badges)
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
