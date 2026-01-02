from __future__ import annotations

import csv
import logging
from typing import Any, Dict, Iterable, Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Q, Sum, Count, Exists, OuterRef
from django.http import (
    FileResponse,
    Http404,
    HttpResponseForbidden,
    HttpResponse,
    HttpResponseBadRequest,
)
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse, reverse_lazy
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import (
    ListView,
    DetailView,
    TemplateView,
    FormView,
    UpdateView,
)

from django.core.exceptions import ValidationError as DjangoCoreValidationError

from apps.users.mixins import PermissionRequiredMixin
from apps.users.permissions import has_permission

from .forms import (
    ExpenseItemForm,
    ReimbursementCreateForm,
    ManagerApprovalForm,
    ManagementApprovalForm,
    FinanceProcessForm,
    ReimbursementSettingsForm,
    ApproverMappingBulkForm,
    ReimbursementForm,
    ManagerReviewForm,
    FinanceReviewForm,
)
from .models import (
    ExpenseItem,
    ReimbursementRequest,
    ReimbursementLine,
    ReimbursementApproverMapping,
    ReimbursementSettings,
    ReimbursementLog,
    Reimbursement,
)

logger = logging.getLogger(__name__)
User = get_user_model()

# Must match _ACTION_SALT in services/notifications.py
EMAIL_ACTION_SALT = "reimbursement-email-action"

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _user_is_admin(user) -> bool:
    return bool(
        getattr(user, "is_superuser", False)
        or has_permission(user, "reimbursement_admin")
    )

def _user_is_finance(user) -> bool:
    return bool(
        has_permission(user, "reimbursement_finance_pending")
        or has_permission(user, "reimbursement_finance_review")
        or has_permission(user, "reimbursement_review_finance")
        or _user_is_admin(user)
    )

def _user_is_manager(user) -> bool:
    return bool(
        has_permission(user, "reimbursement_manager_pending")
        or has_permission(user, "reimbursement_manager_review")
        or has_permission(user, "reimbursement_review_management")
        or _user_is_admin(user)
    )

def _send_safe(func_name: str, *args, **kwargs) -> None:
    """
    Call a notification helper from apps.reimbursement.services.notifications,
    but never break the main flow if emails fail.
    """
    try:
        from .services import notifications  # type: ignore
        fn = getattr(notifications, func_name, None)
        if fn:
            fn(*args, **kwargs)
    except Exception:
        logger.exception("Reimbursement notification %s failed", func_name)

def _safe_back_url(value: Optional[str]) -> Optional[str]:
    """
    Very small allowlist for back/return URLs:
    - must be relative
    - must start with '/' and must NOT contain a scheme or '//' (to avoid open redirects)
    """
    if not value:
        return None
    v = value.strip()
    if v.startswith("/") and "://" not in v and "//" not in v[1:]:
        return v
    return None

def _redirect_back(request, fallback_name: str) -> HttpResponse:
    """
    Redirect to ?return=<relative-url> if provided, else to reverse(fallback_name).
    Works for both GET and POST.
    """
    back = _safe_back_url(request.GET.get("return") or request.POST.get("return"))
    if back:
        return redirect(back)
    return redirect(fallback_name)

# ---------------------------------------------------------------------------
# Employee: Expense Inbox (upload + list)
# ---------------------------------------------------------------------------

class ExpenseInboxView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Employee "Upload Expenses" page:
    - Show a form to upload new expenses (bills).
    - Existing expenses are listed on the right.
    """

    permission_code = "reimbursement_apply"
    template_name = "reimbursement/expense_inbox.html"

    def get_form(self) -> ExpenseItemForm:
        if self.request.method == "POST":
            return ExpenseItemForm(
                self.request.POST,
                self.request.FILES,
                user=self.request.user,
            )
        return ExpenseItemForm(user=self.request.user)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        items = (
            ExpenseItem.objects.filter(created_by=self.request.user)
            .order_by("-date", "-created_at")
        )
        ctx["items"] = items
        ctx["form"] = getattr(self, "form", self.get_form())
        return ctx

    def get(self, request, *args, **kwargs):
        self.form = self.get_form()
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        self.form = self.get_form()
        if self.form.is_valid():
            obj: ExpenseItem = self.form.save(commit=False)
            obj.created_by = request.user
            obj.status = ExpenseItem.Status.SAVED
            obj.save()
            messages.success(request, "Expense saved to your expenses.")
            return redirect("reimbursement:expense_inbox")
        messages.error(request, "Please fix the errors below.")
        return super().get(request, *args, **kwargs)

class ExpenseItemUpdateView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Employee can edit their own draft expenses (not yet locked).
    """

    permission_code = "reimbursement_apply"
    model = ExpenseItem
    form_class = ExpenseItemForm
    template_name = "reimbursement/expense_edit.html"
    success_url = reverse_lazy("reimbursement:expense_inbox")

    def get_queryset(self):
        return ExpenseItem.objects.filter(created_by=self.request.user)

    def dispatch(self, request, *args, **kwargs):
        obj = self.get_object()
        if obj.is_locked:
            messages.error(
                request,
                "This expense is already attached to a request and cannot be edited.",
            )
            return redirect("reimbursement:expense_inbox")
        return super().dispatch(request, *args, **kwargs)

class ExpenseItemDeleteView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Simple POST-based delete for draft expenses.
    """

    permission_code = "reimbursement_apply"
    template_name = "reimbursement/expense_confirm_delete.html"

    def post(self, request, *args, **kwargs):
        obj = get_object_or_404(
            ExpenseItem,
            pk=kwargs.get("pk"),
            created_by=request.user,
        )
        if obj.is_locked:
            messages.error(
                request,
                "This expense is attached to a request and cannot be deleted.",
            )
        else:
            obj.delete()
            messages.success(request, "Expense deleted.")
        return redirect("reimbursement:expense_inbox")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["expense"] = get_object_or_404(
            ExpenseItem,
            pk=self.kwargs.get("pk"),
            created_by=self.request.user,
        )
        return ctx

# ---------------------------------------------------------------------------
# Employee: Create reimbursement request from multiple expenses
# ---------------------------------------------------------------------------

class ReimbursementCreateView(LoginRequiredMixin, PermissionRequiredMixin, FormView):
    """
    Employee clicks "New Request" from My Requests page:
    - Select multiple ExpenseItems from inbox.
    - Submit as a single ReimbursementRequest.
    """

    permission_code = "reimbursement_apply"
    template_name = "reimbursement/create_request.html"
    form_class = ReimbursementCreateForm
    success_url = reverse_lazy("reimbursement:my_reimbursements")

    def _available_expenses_qs(self):
        """
        Expenses for this user that are NOT already attached to any
        ReimbursementLine (i.e. not part of any reimbursement yet).
        """
        user = self.request.user
        used_lines = ReimbursementLine.objects.filter(expense_item=OuterRef("pk"))
        return (
            ExpenseItem.objects.filter(created_by=user)
            .annotate(has_request=Exists(used_lines))
            .filter(has_request=False)
            .order_by("-date", "-created_at")
        )

    def get_form_kwargs(self) -> Dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs["user"] = self.request.user
        return kwargs

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        qs = self._available_expenses_qs()
        if "expense_items" in form.fields:
            form.fields["expense_items"].queryset = qs
        return form

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        user = self.request.user

        # ✅ Show ONLY bills "pending to send" (available, not attached anywhere)
        all_items = self._available_expenses_qs()

        rows = []
        for item in all_items:
            rows.append(
                {
                    "item": item,
                    "line": None,
                    "status_label": "Available",
                    "status_class": "secondary",
                    "selectable": True,
                }
            )

        ctx["rows"] = rows

        # Manager + finance info for right-side panel
        mapping = ReimbursementApproverMapping.for_employee(user)
        ctx["manager"] = mapping.manager if mapping else None
        finance_emails = ReimbursementSettings.get_solo().finance_email_list()
        ctx["finance_emails"] = finance_emails

        return ctx

    def form_valid(self, form: ReimbursementCreateForm):
        user = self.request.user
        expense_items: Iterable[ExpenseItem] = form.cleaned_data["expense_items"]
        employee_note: str = form.cleaned_data.get("employee_note") or ""

        # Resolve approvers from mapping
        mapping = ReimbursementApproverMapping.for_employee(user)
        manager = mapping.manager if mapping else None
        management = None

        # Finance-first flow
        req = ReimbursementRequest.objects.create(
            created_by=user,
            status=ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
            manager=manager,
            management=management,
        )

        # Attach lines
        for item in expense_items:
            ReimbursementLine.objects.create(
                request=req,
                expense_item=item,
                amount=item.amount,
                description=item.description,
                receipt_file=item.receipt_file,
                status=ReimbursementLine.Status.INCLUDED,
            )
            item.status = ExpenseItem.Status.SUBMITTED
            item.save(update_fields=["status", "updated_at"])

        # Recalculate total & mark submitted time
        req.recalc_total(save=True)
        if not req.submitted_at:
            req.submitted_at = timezone.now()
            req.save(update_fields=["submitted_at", "updated_at"])

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.SUBMITTED,
            actor=user,
            message=employee_note or "Reimbursement submitted.",
            from_status=ReimbursementRequest.Status.DRAFT,
            to_status=req.status,
        )

        # Notify FINANCE (verification stage)
        _send_safe("send_reimbursement_finance_verify", req, employee_note=employee_note)

        messages.success(
            self.request,
            "Reimbursement request created and sent to Finance for verification.",
        )
        return super().form_valid(form)

# ---------------------------------------------------------------------------
# Employee: My Requests + detail + edit/delete + BULK DELETE + RESUBMIT
# ---------------------------------------------------------------------------

class MyReimbursementsView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    List of ReimbursementRequest rows for the logged-in employee.
    """

    permission_code = "reimbursement_list"
    model = ReimbursementRequest
    template_name = "reimbursement/my_reimbursements.html"
    context_object_name = "requests"

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.filter(created_by=self.request.user)
            .select_related("manager", "management")
            .order_by("-created_at")
        )

class ReimbursementBulkDeleteView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Bulk deletion of reimbursement requests from 'My Requests'.
    - Only acts on the current user's requests.
    - Skips Paid requests.
    - Unlocks attached ExpenseItems back to SAVED.
    """

    permission_code = "reimbursement_list"
    template_name = "reimbursement/my_requests.html"  # not rendered on POST

    def post(self, request, *args, **kwargs):
        ids = request.POST.getlist("request_ids")
        if not ids:
            messages.warning(request, "No reimbursement requests selected.")
            return redirect("reimbursement:my_reimbursements")

        qs = (
            ReimbursementRequest.objects.filter(
                created_by=request.user,
                pk__in=ids,
            )
            .prefetch_related("lines__expense_item")
        )

        deleted = 0
        skipped_paid = 0

        for req in qs:
            if req.status == ReimbursementRequest.Status.PAID:
                skipped_paid += 1
                continue

            for line in list(req.lines.all()):
                exp = line.expense_item
                line.delete()
                exp.status = ExpenseItem.Status.SAVED
                exp.save(update_fields=["status", "updated_at"])

            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=request.user,
                message="Request deleted via bulk delete.",
                from_status=req.status,
                to_status="deleted",
            )

            req.delete()
            deleted += 1

        if deleted and skipped_paid:
            messages.success(
                request,
                f"{deleted} request(s) deleted. {skipped_paid} paid request(s) were not deleted.",
            )
        elif deleted:
            messages.success(request, f"{deleted} request(s) deleted.")
        elif skipped_paid:
            messages.info(
                request,
                "Paid reimbursements cannot be deleted. No requests were removed.",
            )
        else:
            messages.info(request, "No matching reimbursement requests found.")

        return redirect("reimbursement:my_reimbursements")

class ReimbursementRequestUpdateView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Edit an existing reimbursement request.
    - Employees may edit their own request ONLY when it's REJECTED (to correct a bill).
    - Admin users (reimbursement_admin / superuser) can edit anytime.
    """

    permission_code = "reimbursement_list"
    model = ReimbursementRequest
    form_class = ReimbursementCreateForm
    template_name = "reimbursement/edit_request.html"
    success_url = reverse_lazy("reimbursement:my_reimbursements")

    def dispatch(self, request, *args, **kwargs):
        self.object = self.get_object()
        if _user_is_admin(request.user):
            return super().dispatch(request, *args, **kwargs)

        # Owner can edit only if REJECTED
        if self.object.created_by_id == request.user.id and self.object.status == ReimbursementRequest.Status.REJECTED:
            return super().dispatch(request, *args, **kwargs)

        messages.error(
            request,
            "You can only edit a request after it is rejected, or contact Admin.",
        )
        return redirect("reimbursement:my_reimbursements")

    def get_queryset(self):
        qs = ReimbursementRequest.objects.select_related("manager", "management")
        if _user_is_admin(self.request.user):
            return qs
        return qs.filter(created_by=self.request.user)

    def get_form_kwargs(self) -> Dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs["user"] = self.request.user
        return kwargs

    def _allowed_expenses_qs(self):
        """
        For editing: allow expenses that are either:
          - already attached to THIS request, or
          - not attached to ANY request at all.
        """
        user = self.request.user
        current_req = self.object

        other_lines = ReimbursementLine.objects.filter(
            expense_item=OuterRef("pk")
        ).exclude(request=current_req)

        qs = (
            ExpenseItem.objects.filter(created_by=user)
            .annotate(has_other_request=Exists(other_lines))
            .filter(has_other_request=False)
            .order_by("-date", "-created_at")
        )
        return qs

    def get_form(self, form_class=None):
        form = super().get_form(form_class)

        if "expense_items" in form.fields:
            form.fields["expense_items"].queryset = self._allowed_expenses_qs()
            current_ids = list(
                self.object.lines.values_list("expense_item_id", flat=True)
            )
            form.fields["expense_items"].initial = current_ids

        return form

    def form_valid(self, form: ReimbursementCreateForm):
        req: ReimbursementRequest = self.object
        user = self.request.user

        new_items: Iterable[ExpenseItem] = form.cleaned_data["expense_items"]
        employee_note: str = form.cleaned_data.get("employee_note") or ""

        keep_ids = [e.id for e in new_items]
        for line in list(req.lines.select_related("expense_item")):
            if line.expense_item_id not in keep_ids:
                exp = line.expense_item
                line.delete()
                exp.status = ExpenseItem.Status.SAVED
                exp.save(update_fields=["status", "updated_at"])

        existing_ids = set(
            req.lines.values_list("expense_item_id", flat=True)
        )
        for item in new_items:
            if item.id in existing_ids:
                continue
            ReimbursementLine.objects.create(
                request=req,
                expense_item=item,
                amount=item.amount,
                description=item.description,
                receipt_file=item.receipt_file,
                status=ReimbursementLine.Status.INCLUDED,
            )
            item.status = ExpenseItem.Status.SUBMITTED
            item.save(update_fields=["status", "updated_at"])

        if hasattr(req, "employee_note"):
            req.employee_note = employee_note

        req.recalc_total(save=True)
        req.updated_at = timezone.now()
        req.save()

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.COMMENTED,
            actor=user,
            message="Reimbursement request edited.",
            from_status=req.status,
            to_status=req.status,
        )

        messages.success(self.request, "Reimbursement request updated.")
        return super().form_valid(form)

class ReimbursementResubmitView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Owner resubmits a REJECTED reimbursement after correcting items.
    """

    permission_code = "reimbursement_list"
    template_name = "reimbursement/request_resubmit_confirm.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(
            ReimbursementRequest,
            pk=kwargs.get("pk"),
            created_by=request.user,
            status=ReimbursementRequest.Status.REJECTED,
        )
        note = (request.POST.get("note") or "").strip()
        try:
            req.employee_resubmit(actor=request.user, note=note)
            _send_safe("send_reimbursement_finance_verify", req, employee_note=f"Resubmitted by {request.user}")
            messages.success(request, "Request resubmitted to Finance Verification.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)) or "Unable to resubmit.")
        return _redirect_back(request, "reimbursement:my_reimbursements")

class ReimbursementRequestDeleteView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Delete a reimbursement request (for the owner).
    - Unlocks attached expenses back to the inbox (SAVED).
    - Paid requests cannot be deleted.
    """

    permission_code = "reimbursement_list"
    template_name = "reimbursement/request_confirm_delete.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(
            ReimbursementRequest,
            pk=kwargs.get("pk"),
            created_by=request.user,
        )

        if req.status == ReimbursementRequest.Status.PAID:
            messages.error(request, "Paid reimbursements cannot be deleted.")
            return redirect("reimbursement:my_reimbursements")

        for line in list(req.lines.select_related("expense_item")):
            exp = line.expense_item
            line.delete()
            exp.status = ExpenseItem.Status.SAVED
            exp.save(update_fields=["status", "updated_at"])

        req.delete()
        messages.success(request, "Reimbursement request deleted.")
        return redirect("reimbursement:my_reimbursements")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["request_obj"] = get_object_or_404(
            ReimbursementRequest,
            pk=self.kwargs.get("pk"),
            created_by=self.request.user,
        )
        return ctx

class ReimbursementDetailView(LoginRequiredMixin, PermissionRequiredMixin, DetailView):
    """
    Detail page for a single ReimbursementRequest.
    """

    permission_code = "reimbursement_list"
    model = ReimbursementRequest
    template_name = "reimbursement/request_detail.html"
    context_object_name = "request_obj"

    def get_queryset(self):
        user = self.request.user
        base = (
            ReimbursementRequest.objects.all()
            .select_related("created_by", "manager", "management")
            .prefetch_related("lines", "logs")
        )

        if _user_is_admin(user) or _user_is_finance(user):
            return base

        if _user_is_manager(user):
            return base.filter(
                Q(manager=user)
                | Q(management=user)
                | Q(created_by=user)
            )

        return base.filter(created_by=user)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        req: ReimbursementRequest = self.object
        ctx["lines"] = req.lines.select_related("expense_item")
        ctx["logs"] = req.logs.select_related("actor")
        # Owner can resubmit when rejected
        ctx["can_resubmit"] = (
            req.status == ReimbursementRequest.Status.REJECTED
            and self.request.user.is_authenticated
            and req.created_by_id == self.request.user.id
        )
        ctx["back_url"] = _safe_back_url(self.request.GET.get("return"))
        return ctx

# ---------------------------------------------------------------------------
# Manager & Management queues
# ---------------------------------------------------------------------------

class ManagerQueueView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Manager queue: requests waiting for manager review.
    """

    permission_code = "reimbursement_manager_pending"
    model = ReimbursementRequest
    template_name = "reimbursement/manager_queue.html"
    context_object_name = "requests"

    def get_queryset(self):
        user = self.request.user
        return (
            ReimbursementRequest.objects.filter(
                status__in=[
                    ReimbursementRequest.Status.PENDING_MANAGER,
                    ReimbursementRequest.Status.CLARIFICATION_REQUIRED,
                ]
            )
            .filter(
                Q(manager=user)
                | Q(created_by__reimbursement_approver_mapping__manager=user)
            )
            .select_related("created_by", "manager", "management")
            .order_by("-created_at")
        )

class ManagerReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Manager review form for a single request.
    """

    permission_code = "reimbursement_manager_review"
    model = ReimbursementRequest
    form_class = ManagerApprovalForm
    template_name = "reimbursement/manager_review.html"
    success_url = reverse_lazy("reimbursement:manager_pending")

    def get_queryset(self):
        user = self.request.user
        return (
            ReimbursementRequest.objects.filter(
                status__in=[
                    ReimbursementRequest.Status.PENDING_MANAGER,
                    ReimbursementRequest.Status.CLARIFICATION_REQUIRED,
                ]
            )
            .filter(
                Q(manager=user)
                | Q(created_by__reimbursement_approver_mapping__manager=user)
            )
            .select_related("created_by", "manager", "management")
        )

    def form_valid(self, form: ManagerApprovalForm):
        req: ReimbursementRequest = self.object
        settings_obj = ReimbursementSettings.get_solo()
        prev_status = req.status

        form.save(commit=True)

        decision = form.cleaned_data["decision"]
        if decision == "approved":
            if settings_obj.require_management_approval:
                req.status = ReimbursementRequest.Status.PENDING_MANAGEMENT
            else:
                req.status = ReimbursementRequest.Status.PENDING_FINANCE
        elif decision == "rejected":
            req.status = ReimbursementRequest.Status.REJECTED
        else:
            req.status = ReimbursementRequest.Status.CLARIFICATION_REQUIRED

        req.manager_decided_at = timezone.now()
        req.save(
            update_fields=[
                "status",
                "manager_decision",
                "manager_comment",
                "manager_decided_at",
                "updated_at",
            ]
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.STATUS_CHANGED if decision != "approved" else ReimbursementLog.Action.MANAGER_APPROVED,
            actor=self.request.user,
            message=f"Manager decision: {decision}",
            from_status=prev_status,
            to_status=req.status,
        )

        _send_safe("send_reimbursement_manager_action", req, decision=decision)

        messages.success(self.request, "Manager decision recorded.")
        return super().form_valid(form)

    def get_success_url(self):
        back = _safe_back_url(self.request.GET.get("return") or self.request.POST.get("return"))
        return back or reverse("reimbursement:manager_pending")

class ManagementQueueView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Management queue: requests waiting for higher-level approval.
    """

    permission_code = "reimbursement_management_pending"
    model = ReimbursementRequest
    template_name = "reimbursement/management_queue.html"
    context_object_name = "requests"

    def get_queryset(self):
        user = self.request.user
        return (
            ReimbursementRequest.objects.filter(
                status=ReimbursementRequest.Status.PENDING_MANAGEMENT
            )
            .filter(Q(management=user) | Q(manager=user) | Q(created_by=user))
            .select_related("created_by", "manager", "management")
            .order_by("-created_at")
        )

class ManagementReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Management review form for a single request.
    """

    permission_code = "reimbursement_management_review"
    model = ReimbursementRequest
    form_class = ManagementApprovalForm
    template_name = "reimbursement/management_review.html"
    success_url = reverse_lazy("reimbursement:management_pending")

    def get_queryset(self):
        user = self.request.user
        return (
            ReimbursementRequest.objects.filter(
                status=ReimbursementRequest.Status.PENDING_MANAGEMENT
            )
            .filter(Q(management=user) | Q(manager=user) | Q(created_by=user))
            .select_related("created_by", "manager", "management")
        )

    def form_valid(self, form: ManagementApprovalForm):
        req: ReimbursementRequest = self.object
        prev_status = req.status

        form.save(commit=True)
        decision = form.cleaned_data["decision"]

        if decision == "approved":
            req.status = ReimbursementRequest.Status.PENDING_FINANCE
        elif decision == "rejected":
            req.status = ReimbursementRequest.Status.REJECTED
        else:
            req.status = ReimbursementRequest.Status.CLARIFICATION_REQUIRED

        req.management_decided_at = timezone.now()
        req.save(
            update_fields=[
                "status",
                "management_decision",
                "management_comment",
                "management_decided_at",
                "updated_at",
            ]
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=self.request.user,
            message=f"Management decision: {decision}",
            from_status=prev_status,
            to_status=req.status,
        )

        _send_safe("send_reimbursement_management_action", req, decision=decision)

        messages.success(self.request, "Management decision recorded.")
        return super().form_valid(form)

    def get_success_url(self):
        back = _safe_back_url(self.request.GET.get("return") or self.request.POST.get("return"))
        return back or reverse("reimbursement:management_pending")

# ---------------------------------------------------------------------------
# Finance queue & processing
# ---------------------------------------------------------------------------

class FinanceQueueView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Finance work queue: Reimbursement Details for Finance.
    """

    permission_code = "reimbursement_finance_pending"
    model = ReimbursementRequest
    template_name = "reimbursement/finance_queue.html"
    context_object_name = "requests"

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.filter(
                status__in=[
                    ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
                    ReimbursementRequest.Status.PENDING_FINANCE,
                    ReimbursementRequest.Status.APPROVED,
                ]
            )
            .select_related("created_by", "manager", "management")
            .order_by("-created_at")
        )

class FinanceVerifyView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Finance verification (pre-manager step): approve (verify) or reject.
    Ensures a SINGLE verified log — uses model.mark_verified() as the SoT.
    """

    permission_code = "reimbursement_finance_review"
    template_name = "reimbursement/finance_verify.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(
            ReimbursementRequest.objects.select_related("created_by", "manager"),
            pk=kwargs.get("pk"),
            status=ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
        )

        decision = (request.POST.get("decision") or "").strip().lower()
        note = (request.POST.get("note") or "").strip()

        if decision in ("verify", "verified"):
            req.mark_verified(actor=request.user, note=note)
            _send_safe("send_reimbursement_finance_verified", req)
            messages.success(request, "Request verified and sent to Manager.")
        elif decision == "rejected":
            prev_status = req.status
            req.finance_note = (req.finance_note + ("\n" if req.finance_note and note else "") + note).strip()
            req.status = ReimbursementRequest.Status.REJECTED
            req.save(update_fields=["finance_note", "status", "updated_at"])
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.STATUS_CHANGED,
                actor=request.user,
                message="Finance rejected at verification stage.",
                from_status=prev_status,
                to_status=req.status,
            )
            _send_safe("send_reimbursement_finance_rejected", req)
            messages.success(request, "Request rejected and employee notified.")
        else:
            messages.error(request, "Invalid decision.")
        return _redirect_back(request, "reimbursement:finance_pending")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["request_obj"] = get_object_or_404(
            ReimbursementRequest.objects.select_related("created_by"),
            pk=self.kwargs.get("pk"),
            status=ReimbursementRequest.Status.PENDING_FINANCE_VERIFY,
        )
        ctx["lines"] = ctx["request_obj"].lines.select_related("expense_item")
        return ctx

class FinanceReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Finance review form, including mark-paid (Claim Settled).
    Uses model.mark_paid() to ensure single, accurate audit entry with actor.
    """

    permission_code = "reimbursement_finance_review"
    model = ReimbursementRequest
    form_class = FinanceProcessForm
    template_name = "reimbursement/finance_review.html"
    success_url = reverse_lazy("reimbursement:finance_pending")

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.filter(
                status__in=[
                    ReimbursementRequest.Status.PENDING_FINANCE,
                    ReimbursementRequest.Status.APPROVED,
                ]
            )
            .select_related("created_by", "manager", "management")
        )

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        req: ReimbursementRequest = self.object
        can, _ = req.can_mark_paid(req.finance_payment_reference or "")
        ctx["can_mark_paid"] = can
        ctx["back_url"] = _safe_back_url(self.request.GET.get("return"))
        return ctx

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        try:
            can, _ = self.object.can_mark_paid(self.object.finance_payment_reference or "")
            if not can and "mark_paid" in form.fields:
                form.fields["mark_paid"].disabled = True
        except Exception:
            pass
        return form

    def form_valid(self, form: FinanceProcessForm):
        req: ReimbursementRequest = self.object
        prev_status = req.status
        mark_paid: bool = form.cleaned_data.get("mark_paid", False)
        ref: str = form.cleaned_data.get("finance_payment_reference") or ""
        note: str = form.cleaned_data.get("finance_note") or ""

        # Always persist non-status fields first
        req.finance_note = note
        req.finance_payment_reference = ref
        req.save(update_fields=["finance_note", "finance_payment_reference", "updated_at"])

        if mark_paid:
            ok, msg = req.can_mark_paid(ref)
            if not ok:
                if "reference" in msg.lower():
                    form.add_error("finance_payment_reference", msg)
                else:
                    form.add_error("mark_paid", msg)
                    form.add_error(None, msg)
                return self.form_invalid(form)

            try:
                req.mark_paid(reference=ref, actor=self.request.user, note=note)
            except DjangoCoreValidationError as e:
                msg = e.message if hasattr(e, "message") else str(e)
                if not msg:
                    msg = "Unable to mark as Claim Settled due to a validation error."
                form.add_error(None, msg)
                return self.form_invalid(form)

            _send_safe("send_reimbursement_paid", req)
            messages.success(self.request, "Request marked as Claim Settled.")
        else:
            if req.status == ReimbursementRequest.Status.PENDING_FINANCE:
                try:
                    req.status = ReimbursementRequest.Status.APPROVED
                    req.save(update_fields=["status", "updated_at"])
                    ReimbursementLog.log(
                        req,
                        ReimbursementLog.Action.STATUS_CHANGED,
                        actor=self.request.user,
                        message="Finance approved (Ready to Pay).",
                        from_status=prev_status,
                        to_status=req.status,
                    )
                    messages.success(self.request, "Finance review saved. Status set to Ready to Pay.")
                except DjangoCoreValidationError as e:
                    form.add_error(None, getattr(e, "message", str(e)) or "Unable to approve this reimbursement.")
                    return self.form_invalid(form)
            else:
                messages.success(self.request, "Finance details updated.")

        return super().form_valid(form)

    def get_success_url(self):
        back = _safe_back_url(self.request.GET.get("return") or self.request.POST.get("return"))
        return back or reverse("reimbursement:finance_pending")

# ---------------------------------------------------------------------------
# Admin dashboards, config & CSV export (ApproverMappingAdminView fixed)
# ---------------------------------------------------------------------------

class AdminBillsSummaryView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Admin-only: flat list of all reimbursement lines (bills) across all users.
    """
    permission_code = "reimbursement_admin"
    model = ReimbursementLine
    template_name = "reimbursement/admin_bills_summary.html"
    context_object_name = "lines"

    def get_queryset(self):
        return (
            ReimbursementLine.objects.select_related(
                "request",
                "expense_item",
                "request__created_by",
            )
            .order_by("-request__submitted_at", "-id")
        )

class AdminRequestsListView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Admin-only: all reimbursement requests.
    """

    permission_code = "reimbursement_admin"
    model = ReimbursementRequest
    template_name = "reimbursement/admin_requests.html"
    context_object_name = "requests"

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.select_related(
                "created_by",
                "manager",
                "management",
            )
            .order_by("-created_at")
        )

class AdminEmployeeSummaryView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Admin-only: aggregate totals per employee.
    """

    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_employee_summary.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        rows = (
            ReimbursementRequest.objects.values(
                "created_by__id",
                "created_by__first_name",
                "created_by__last_name",
                "created_by__username",
                "created_by__email",
            )
            .annotate(
                total_amount=Sum("total_amount"),
                request_count=Count("id"),
            )
            .order_by("-total_amount")
        )
        ctx["rows"] = rows
        return ctx

class AdminStatusSummaryView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Admin-only: aggregate totals by request status.
    """

    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_status_summary.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # Raw aggregation
        raw_rows = (
            ReimbursementRequest.objects.values("status")
            .annotate(
                total_amount=Sum("total_amount"),
                request_count=Count("id"),
            )
            .order_by("status")
        )

        # Map machine status -> human label once here
        status_labels = dict(ReimbursementRequest.Status.choices)
        rows = []
        for r in raw_rows:
            rows.append({
                "status": r["status"],
                "status_label": status_labels.get(r["status"], r["status"]),
                "total_amount": r["total_amount"],
                "request_count": r["request_count"],
            })

        ctx["rows"] = rows
        # Keep for other templates if needed
        ctx["status_labels"] = status_labels
        return ctx

class ApproverMappingAdminView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Admin-only page to manage per-employee approver mapping.
    - Fixes the previous error by NOT injecting unknown kwargs into forms.
    - Renders three logical sections that match the provided template:
      (1) Settings, (2) Bulk apply, (3) Per-employee mapping grid.
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_approver_mapping.html"

    # ----- helpers -----

    def _all_users_for_select(self):
        return User.objects.all().order_by("first_name", "last_name", "username")

    def _rows(self):
        rows = []
        mappings = {
            m.employee_id: m
            for m in ReimbursementApproverMapping.objects.select_related("employee", "manager", "finance")
        }
        for u in self._all_users_for_select():
            rows.append({"user": u, "mapping": mappings.get(u.id)})
        return rows

    # ----- GET -----

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        settings_obj = ReimbursementSettings.get_solo()
        ctx["settings_form"] = ReimbursementSettingsForm(instance=settings_obj)
        ctx["bulk_form"] = ApproverMappingBulkForm()
        ctx["rows"] = self._rows()
        ctx["all_users_for_select"] = self._all_users_for_select()
        return ctx

    # ----- POST -----

    def post(self, request, *args, **kwargs):
        # Which form?
        if "save_settings" in request.POST:
            return self._handle_save_settings(request)
        if "apply_bulk" in request.POST:
            return self._handle_apply_bulk(request)
        if "save_mappings" in request.POST:
            return self._handle_save_mappings(request)

        messages.error(request, "Unknown action.")
        return redirect(request.path)

    def _handle_save_settings(self, request):
        obj = ReimbursementSettings.get_solo()
        form = ReimbursementSettingsForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Settings saved.")
        else:
            messages.error(request, "Please fix the errors in settings.")
        return redirect(request.path)

    def _handle_apply_bulk(self, request):
        form = ApproverMappingBulkForm(request.POST)
        if not form.is_valid():
            messages.error(request, "Please fix the errors in bulk form.")
            return redirect(request.path)

        apply_manager = form.cleaned_data.get("apply_manager_to_all")
        apply_finance = form.cleaned_data.get("apply_finance_to_all")
        manager_for_all = form.cleaned_data.get("manager_for_all")
        finance_for_all = form.cleaned_data.get("finance_for_all")

        users = self._all_users_for_select()
        processed = 0
        for u in users:
            mapping, _ = ReimbursementApproverMapping.objects.get_or_create(employee=u)
            changed = False
            if apply_manager:
                mapping.manager = manager_for_all
                changed = True
            if apply_finance:
                mapping.finance = finance_for_all
                changed = True
            if changed:
                mapping.save()
                processed += 1

        messages.success(request, f"Bulk mapping applied. Rows updated: {processed}.")
        return redirect(request.path)

    def _handle_save_mappings(self, request):
        users = self._all_users_for_select()
        id_to_user = {u.id: u for u in users}
        processed = 0

        for u in users:
            manager_id = request.POST.get(f"manager_{u.id}") or ""
            finance_id = request.POST.get(f"finance_{u.id}") or ""

            manager = id_to_user.get(int(manager_id)) if manager_id.isdigit() else None
            finance = id_to_user.get(int(finance_id)) if finance_id.isdigit() else None

            mapping, _ = ReimbursementApproverMapping.objects.get_or_create(employee=u)
            if mapping.manager_id != (manager.id if manager else None) or mapping.finance_id != (finance.id if finance else None):
                mapping.manager = manager
                mapping.finance = finance
                mapping.save()
                processed += 1

        messages.success(request, f"Per-employee mappings saved. Rows processed: {processed}.")
        return redirect(request.path)

class ReimbursementExportCSVView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Admin-only CSV export ("backend sheet") of reimbursements with attachment links.
    Each line row is exported (1 row per line).
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_export_dummy.html"  # not used

    def get(self, request, *args, **kwargs):
        qs = (
            ReimbursementLine.objects.select_related(
                "request",
                "expense_item",
                "request__created_by",
            )
            .order_by("request_id", "id")
        )

        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="reimbursements_export.csv"'
        writer = csv.writer(response)
        writer.writerow([
            "request_id",
            "employee_name",
            "employee_email",
            "status",
            "submitted_at",
            "total_amount",
            "line_id",
            "expense_date",
            "category",
            "gst_type",
            "vendor",
            "description",
            "amount",
            "receipt_url",
        ])

        for line in qs:
            req = line.request
            expense = line.expense_item
            receipt_url = request.build_absolute_uri(
                reverse("reimbursement:receipt_line", args=[line.id])
            )
            writer.writerow([
                req.id,
                (req.created_by.get_full_name() or req.created_by.username),
                req.created_by.email,
                req.status,
                req.submitted_at.isoformat() if req.submitted_at else "",
                f"{req.total_amount:.2f}",
                line.id,
                expense.date.isoformat() if expense and expense.date else "",
                expense.get_category_display() if expense else "",
                expense.get_gst_type_display() if expense and expense.gst_type else "",
                getattr(expense, "vendor", "") or "",
                (line.description or ""),
                f"{line.amount:.2f}",
                receipt_url,
            ])

        return response

# ---------------------------------------------------------------------------
# Secure receipt download
# ---------------------------------------------------------------------------

def download_receipt(
    request,
    line_id: Optional[int] = None,
    expense_id: Optional[int] = None,
):
    """
    Serve receipt files securely.
    """
    user = request.user
    if not user.is_authenticated:
        raise Http404

    file_field = None

    if line_id is not None:
        line = get_object_or_404(
            ReimbursementLine.objects.select_related(
                "expense_item",
                "request",
                "request__created_by",
            ),
            pk=line_id,
        )
        file_field = line.receipt_file or line.expense_item.receipt_file
        owner = line.request.created_by
    elif expense_id is not None:
        expense = get_object_or_404(
            ExpenseItem.objects.select_related("created_by"),
            pk=expense_id,
        )
        file_field = expense.receipt_file
        owner = expense.created_by
    else:
        raise Http404

    if not file_field:
        raise Http404("No receipt file attached.")

    allowed = False
    if user == owner:
        allowed = True
    elif _user_is_admin(user) or _user_is_finance(user) or _user_is_manager(user):
        allowed = True

    if not allowed:
        return HttpResponseForbidden("You are not allowed to view this receipt.")

    return FileResponse(file_field.open("rb"), as_attachment=False)

# ---------------------------------------------------------------------------
# Magic-link email actions (Approve / Reject buttons)
# ---------------------------------------------------------------------------

@csrf_exempt
def reimbursement_email_action(request):
    """
    Endpoint hit from Approve / Reject buttons in email.
    - Does NOT require login (magic link).
    - Validates the signed token.
    - Applies manager / management decision.
    """
    from django.core import signing
    from django.core.signing import BadSignature

    token = request.GET.get("t") or request.POST.get("t")
    if not token:
        return HttpResponseBadRequest("Missing token.")

    try:
        data = signing.loads(token, salt=EMAIL_ACTION_SALT, max_age=7 * 24 * 3600)
    except BadSignature:
        return HttpResponseBadRequest("Invalid or expired link.")

    req_id = data.get("req_id")
    role = data.get("role")
    decision = data.get("decision")

    if not req_id or role not in ("manager", "management") or decision not in (
        "approved",
        "rejected",
        "clarification",
    ):
        return HttpResponseBadRequest("Malformed token.")

    req = get_object_or_404(ReimbursementRequest, pk=req_id)
    prev_status = req.status
    now = timezone.now()

    if role == "manager":
        settings_obj = ReimbursementSettings.get_solo()
        req.manager_decision = decision
        base_comment = req.manager_comment or ""
        note = "Decision recorded via email link."
        req.manager_comment = (base_comment + "\n" if base_comment else "") + note

        if decision == "approved":
            if settings_obj.require_management_approval:
                req.status = ReimbursementRequest.Status.PENDING_MANAGEMENT
            else:
                req.status = ReimbursementRequest.Status.PENDING_FINANCE
        elif decision == "rejected":
            req.status = ReimbursementRequest.Status.REJECTED
        else:
            req.status = ReimbursementRequest.Status.CLARIFICATION_REQUIRED

        req.manager_decided_at = now
        req.save(
            update_fields=[
                "status",
                "manager_decision",
                "manager_comment",
                "manager_decided_at",
                "updated_at",
            ]
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.STATUS_CHANGED if decision != "approved" else ReimbursementLog.Action.MANAGER_APPROVED,
            actor=req.manager,
            message=f"Manager decision via email: {decision}",
            from_status=prev_status,
            to_status=req.status,
        )
        _send_safe("send_reimbursement_manager_action", req, decision=decision)

    elif role == "management":
        req.management_decision = decision
        base_comment = req.management_comment or ""
        note = "Decision recorded via email link."
        req.management_comment = (base_comment + "\n" if base_comment else "") + note

        if decision == "approved":
            req.status = ReimbursementRequest.Status.PENDING_FINANCE
        elif decision == "rejected":
            req.status = ReimbursementRequest.Status.REJECTED
        else:
            req.status = ReimbursementRequest.Status.CLARIFICATION_REQUIRED

        req.management_decided_at = now
        req.save(
            update_fields=[
                "status",
                "management_decision",
                "management_comment",
                "management_decided_at",
                "updated_at",
            ]
        )

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=req.management,
            message=f"Management decision via email: {decision}",
            from_status=prev_status,
            to_status=req.status,
        )
        _send_safe("send_reimbursement_management_action", req, decision=decision)

    html = """
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;">
    <div style="max-width:480px;margin:40px auto;padding:24px;border-radius:10px;
                border:1px solid #e5e7eb;background:#ffffff;text-align:center;">
      <h2 style="margin-top:0;color:#16a34a;">Thank you</h2>
      <p style="margin:12px 0;">Your decision has been recorded successfully.</p>
      <p style="margin:12px 0;font-size:13px;color:#6b7280;">
        You can now close this window.
      </p>
    </div>
  </body>
</html>
"""
    return HttpResponse(html)

# ---------------------------------------------------------------------------
# NEW: Admin Override endpoints (POST-only helpers; wire in urls if needed)
# ---------------------------------------------------------------------------

class AdminReverseToFinanceVerificationView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    POST-only action for Admins: Reverse back to FINANCE VERIFICATION.
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_reverse_confirm.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(ReimbursementRequest, pk=kwargs.get("pk"))
        if not _user_is_admin(request.user):
            return HttpResponseForbidden("Not allowed.")
        reason = (request.POST.get("reason") or "").strip()
        try:
            req.reverse_to_finance_verification(actor=request.user, reason=reason or "Admin reversal")
            _send_safe("send_reimbursement_finance_verify", req, employee_note=f"Admin reversal by {request.user}")
            messages.success(request, "Reversal recorded. Sent back to Finance Verification.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)) or "Unable to reverse this reimbursement.")
        return _redirect_back(request, "reimbursement:admin_requests")

class AdminResendToFinanceView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    POST-only: Resend to Finance Verification (idempotent; always logs).
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_resend_finance.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(ReimbursementRequest, pk=kwargs.get("pk"))
        if not _user_is_admin(request.user):
            return HttpResponseForbidden("Not allowed.")
        reason = (request.POST.get("reason") or "").strip()
        try:
            req.resend_to_finance(actor=request.user, reason=reason)
            _send_safe("send_reimbursement_finance_verify", req, employee_note=f"Admin resend by {request.user}")
            messages.success(request, "Request re-sent to Finance Verification.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)))
        return _redirect_back(request, "reimbursement:admin_requests")

class AdminResendToManagerView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    POST-only: Resend to Manager (requires verified_by to be present).
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_resend_manager.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(ReimbursementRequest, pk=kwargs.get("pk"))
        if not _user_is_admin(request.user):
            return HttpResponseForbidden("Not allowed.")
        reason = (request.POST.get("reason") or "").strip()
        try:
            req.resend_to_manager(actor=request.user, reason=reason)
            messages.success(request, "Request re-sent to Manager for approval.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)))
        return _redirect_back(request, "reimbursement:admin_requests")

class AdminForceMoveView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    POST-only: Admin can manually move backward/forward **without** bypassing model validations.
    Disallows direct move to PAID.
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_force_move.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(ReimbursementRequest, pk=kwargs.get("pk"))
        if not _user_is_admin(request.user):
            return HttpResponseForbidden("Not allowed.")
        target = (request.POST.get("target_status") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        try:
            req.admin_force_move(target, actor=request.user, reason=reason)
            messages.success(request, f"Request moved to {target}.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)) or "Unable to move the request.")
        return _redirect_back(request, "reimbursement:admin_requests")

class AdminDeleteWithAuditView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    POST-only: Admin delete with strict checks and full audit.
    """
    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_delete_confirm.html"

    def post(self, request, *args, **kwargs):
        req = get_object_or_404(ReimbursementRequest, pk=kwargs.get("pk"))
        if not _user_is_admin(request.user):
            return HttpResponseForbidden("Not allowed.")
        reason = (request.POST.get("reason") or "").strip()
        try:
            req.delete_with_audit(actor=request.user, reason=reason or "Admin delete")
            messages.success(request, "Reimbursement deleted with audit trail.")
        except DjangoCoreValidationError as e:
            messages.error(request, getattr(e, "message", str(e)) or "Unable to delete this reimbursement.")
        return _redirect_back(request, "reimbursement:admin_requests")

# ---------------------------------------------------------------------------
# LEGACY VIEWS (single-bill Reimbursement model)
# ---------------------------------------------------------------------------

class LegacyMyReimbursementsView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Legacy 'My Reimbursements' for the simple Reimbursement model.
    """

    permission_code = "reimbursement_list"
    model = Reimbursement
    template_name = "reimbursement/legacy_my_requests.html"
    context_object_name = "requests"

    def get_queryset(self):
        return Reimbursement.objects.filter(employee=self.request.user).order_by(
            "-submitted_at"
        )

class LegacyReimbursementCreateView(LoginRequiredMixin, PermissionRequiredMixin, FormView):
    """
    Legacy single-bill reimbursement create view.
    """

    permission_code = "reimbursement_apply"
    template_name = "reimbursement/legacy_apply.html"
    form_class = ReimbursementForm
    success_url = reverse_lazy("reimbursement:my_reimbursements")

    def form_valid(self, form: ReimbursementForm):
        obj = form.save(commit=False)
        obj.employee = self.request.user
        obj.save()
        messages.success(self.request, "Legacy reimbursement submitted.")
        return super().form_valid(form)

class LegacyManagerPendingView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    permission_code = "reimbursement_manager_pending"
    model = Reimbursement
    template_name = "reimbursement/legacy_manager_pending.html"
    context_object_name = "requests"

    def get_queryset(self):
        return Reimbursement.objects.filter(status="PM")

class LegacyManagerReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    permission_code = "reimbursement_manager_review"
    model = Reimbursement
    form_class = ManagerReviewForm
    template_name = "reimbursement/legacy_manager_review.html"
    success_url = reverse_lazy("reimbursement:manager_pending")

class LegacyFinancePendingView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    permission_code = "reimbursement_finance_pending"
    model = Reimbursement
    template_name = "reimbursement/legacy_finance_pending.html"
    context_object_name = "requests"

    def get_queryset(self):
        return Reimbursement.objects.filter(status="PF")

class LegacyFinanceReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    permission_code = "reimbursement_finance_review"
    model = Reimbursement
    form_class = FinanceReviewForm
    template_name = "reimbursement/legacy_finance_review.html"
    success_url = reverse_lazy("reimbursement:finance_pending")
