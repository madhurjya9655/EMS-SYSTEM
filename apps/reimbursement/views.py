# apps/reimbursement/views.py
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import models
from django.db.models import Q, Sum, Count, Exists, OuterRef
from django.http import (
    FileResponse,
    Http404,
    HttpResponseForbidden,
    HttpResponse,
    HttpResponseBadRequest,
)
    # HttpResponse and HttpResponseBadRequest used in email action view
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse, reverse_lazy
from django.utils import timezone
from django.views.generic import (
    ListView,
    DetailView,
    TemplateView,
    FormView,
    UpdateView,
)
from django.views.decorators.csrf import csrf_exempt
from django.core import signing
from django.core.signing import BadSignature

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


# ---------------------------------------------------------------------------
# Employee: Expense Inbox (upload + list)
# ---------------------------------------------------------------------------

class ExpenseInboxView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Employee "BOS Reimburse Log → Expense Inbox":

    - List all ExpenseItems for the current user (draft + submitted + attached).
    - Provide a form to upload new expenses (bills).
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
            messages.success(request, "Expense saved to your inbox.")
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

    Behaviour:
      * Only expenses that are NOT already used in any request can be selected.
      * After submit, those expenses become "pending" (blocked from further use)
        until the request is paid / rejected.
    """

    permission_code = "reimbursement_apply"
    template_name = "reimbursement/create_request.html"
    form_class = ReimbursementCreateForm
    success_url = reverse_lazy("reimbursement:my_reimbursements")

    # --- helpers ---------------------------------------------------------

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

    # --- Form wiring -----------------------------------------------------

    def get_form_kwargs(self) -> Dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs["user"] = self.request.user
        return kwargs

    def get_form(self, form_class=None):
        """
        Ensure the expense_items field only allows "available" expenses.
        """
        form = super().get_form(form_class)
        qs = self._available_expenses_qs()
        if "expense_items" in form.fields:
            form.fields["expense_items"].queryset = qs
        return form

    # --- Context for template -------------------------------------------

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        user = self.request.user

        # All expenses for this user (for display)
        all_items = (
            ExpenseItem.objects.filter(created_by=user)
            .order_by("-date", "-created_at")
        )

        # Map each expense -> its reimbursement line (if any)
        lines = (
            ReimbursementLine.objects.filter(expense_item__in=all_items)
            .select_related("request", "expense_item")
        )
        line_map = {ln.expense_item_id: ln for ln in lines}

        rows = []
        for item in all_items:
            line = line_map.get(item.id)
            if not line:
                # Never used: can be selected
                status_label = "Available"
                status_class = "secondary"
                selectable = True
            else:
                req = line.request
                if req.status == ReimbursementRequest.Status.PAID:
                    status_label = "Paid"
                    status_class = "success"
                elif req.status == ReimbursementRequest.Status.REJECTED:
                    status_label = "Rejected"
                    status_class = "danger"
                else:
                    status_label = "Pending"
                    status_class = "warning"
                selectable = False

            rows.append(
                {
                    "item": item,
                    "line": line,
                    "status_label": status_label,
                    "status_class": status_class,
                    "selectable": selectable,
                }
            )

        ctx["rows"] = rows

        # Manager + finance info for right-side panel
        mapping = ReimbursementApproverMapping.for_employee(user)
        ctx["manager"] = mapping.manager if mapping else None
        finance_emails = ReimbursementSettings.get_solo().finance_email_list()
        ctx["finance_emails"] = finance_emails

        return ctx

    # --- Submit ----------------------------------------------------------

    def form_valid(self, form: ReimbursementCreateForm):
        user = self.request.user
        settings_obj = ReimbursementSettings.get_solo()
        expense_items: Iterable[ExpenseItem] = form.cleaned_data["expense_items"]
        employee_note: str = form.cleaned_data.get("employee_note") or ""

        # Resolve approvers from mapping
        mapping = ReimbursementApproverMapping.for_employee(user)
        manager = mapping.manager if mapping else None
        management = None  # optional: could be derived later if needed

        req = ReimbursementRequest.objects.create(
            created_by=user,
            status=ReimbursementRequest.Status.PENDING_MANAGER,
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
            # Lock the item in inbox (pending while request is processed)
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

        # Notifications (manager + admin summary)
        _send_safe("send_reimbursement_submitted", req, employee_note=employee_note)
        if settings_obj.admin_email_list():
            _send_safe("send_reimbursement_admin_summary", req)

        messages.success(
            self.request,
            "Reimbursement request created and submitted for approval.",
        )
        return super().form_valid(form)


# ---------------------------------------------------------------------------
# Employee: My Requests + detail + edit/delete + BULK DELETE
# ---------------------------------------------------------------------------

class MyReimbursementsView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    List of ReimbursementRequest rows for the logged-in employee.
    """

    permission_code = "reimbursement_list"
    model = ReimbursementRequest
    template_name = "reimbursement/my_requests.html"
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
    template_name = "reimbursement/my_requests.html"  # not actually rendered on POST

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

            # Unlock expenses and delete lines
            for line in list(req.lines.all()):
                exp = line.expense_item
                line.delete()
                exp.status = ExpenseItem.Status.SAVED
                exp.save(update_fields=["status", "updated_at"])

            # Optional: log before deletion (audit)
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
    Edit an existing reimbursement request (for the owner).

    - Uses the same ReimbursementCreateForm to let the user change selected
      expense items and note.
    - An expense can be attached to this request, or unused in any request.
      It cannot be attached to some other request.
    """

    permission_code = "reimbursement_list"
    model = ReimbursementRequest
    form_class = ReimbursementCreateForm
    template_name = "reimbursement/edit_request.html"
    success_url = reverse_lazy("reimbursement:my_reimbursements")

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.filter(created_by=self.request.user)
            .select_related("manager", "management")
        )

    def get_form_kwargs(self) -> Dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs["user"] = self.request.user
        return kwargs

    def _allowed_expenses_qs(self):
        """
        For editing: allow expenses that are either:
          - already attached to THIS request, or
          - not attached to ANY request at all.

        Implemented via subquery on ReimbursementLine; we DO NOT
        use a non-existent reverse 'lines' on ExpenseItem.
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

        # Restrict choices
        if "expense_items" in form.fields:
            form.fields["expense_items"].queryset = self._allowed_expenses_qs()

            # Pre-select currently attached expenses
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

        # Detach existing lines not in new selection
        keep_ids = [e.id for e in new_items]
        for line in list(req.lines.select_related("expense_item")):
            if line.expense_item_id not in keep_ids:
                exp = line.expense_item
                line.delete()
                # Unlock the expense back to inbox
                exp.status = ExpenseItem.Status.SAVED
                exp.save(update_fields=["status", "updated_at"])

        # Attach any newly selected expenses
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

        # Update note (if your model has a field for it)
        if hasattr(req, "employee_note"):
            req.employee_note = employee_note

        req.recalc_total(save=True)
        req.updated_at = timezone.now()
        req.save()

        ReimbursementLog.log(
            req,
            ReimbursementLog.Action.COMMENTED,
            actor=user,
            message="Employee edited reimbursement request.",
            from_status=req.status,
            to_status=req.status,
        )

        messages.success(self.request, "Reimbursement request updated.")
        return super().form_valid(form)


class ReimbursementRequestDeleteView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Delete a reimbursement request (for the owner).

    - Unlocks attached expenses back to the inbox (SAVED).
    - Typically you should NOT allow deleting a PAID request.
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

        # Unlock expenses and delete lines
        for line in list(req.lines.select_related("expense_item")):
            exp = line.expense_item
            line.delete()
            exp.status = ExpenseItem.Status.SAVED
            exp.save(update_fields=["status", "updated_at"])

        req.delete()
        messages.success(request, "Reimbursement request deleted.")
        return redirect("reimbursement:my_reimbursements")

    def get_context_data(self, **kwargs):
        """
        If you ever hit this via GET you can render a confirm page.
        The inline delete form on my_requests.html only uses POST.
        """
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

        # Manager / management can see items they are responsible for
        if _user_is_manager(user):
            return base.filter(
                Q(manager=user)
                | Q(management=user)
                | Q(created_by=user)
            )

        # Regular employee: only own requests
        return base.filter(created_by=user)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        req: ReimbursementRequest = self.object
        ctx["lines"] = req.lines.select_related("expense_item")
        ctx["logs"] = req.logs.select_related("actor")
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

        # Persist decision comment + decision field
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
            ReimbursementLog.Action.STATUS_CHANGED,
            actor=self.request.user,
            message=f"Manager decision: {decision}",
            from_status=prev_status,
            to_status=req.status,
        )

        # Notifications
        _send_safe("send_reimbursement_manager_action", req, decision=decision)

        messages.success(self.request, "Manager decision recorded.")
        return super().form_valid(form)


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


# ---------------------------------------------------------------------------
# Finance queue & processing
# ---------------------------------------------------------------------------

class FinanceQueueView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Finance work queue: requests pending finance review and payment.
    """

    permission_code = "reimbursement_finance_pending"
    model = ReimbursementRequest
    template_name = "reimbursement/finance_queue.html"
    context_object_name = "requests"

    def get_queryset(self):
        return (
            ReimbursementRequest.objects.filter(
                status__in=[
                    ReimbursementRequest.Status.PENDING_FINANCE,
                    ReimbursementRequest.Status.APPROVED,
                ]
            )
            .select_related("created_by", "manager", "management")
            .order_by("-created_at")
        )


class FinanceReviewView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    """
    Finance review form, including mark-paid.
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

    def form_valid(self, form: FinanceProcessForm):
        req: ReimbursementRequest = self.object
        prev_status = req.status
        mark_paid: bool = form.cleaned_data.get("mark_paid", False)
        ref: str = form.cleaned_data.get("finance_payment_reference") or ""
        note: str = form.cleaned_data.get("finance_note") or ""

        # Always update finance_note/reference first
        req.finance_note = note
        req.finance_payment_reference = ref

        if mark_paid:
            # Mark request as paid
            req.status = ReimbursementRequest.Status.PAID
            req.paid_at = timezone.now()
            req.save(
                update_fields=[
                    "status",
                    "finance_note",
                    "finance_payment_reference",
                    "paid_at",
                    "updated_at",
                ]
            )

            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.PAID,
                actor=self.request.user,
                message="Finance marked request as paid.",
                from_status=prev_status,
                to_status=req.status,
            )
            _send_safe("send_reimbursement_paid", req)
            messages.success(self.request, "Request marked as paid.")
        else:
            # Just save notes without changing status
            req.save(
                update_fields=["finance_note", "finance_payment_reference", "updated_at"]
            )
            ReimbursementLog.log(
                req,
                ReimbursementLog.Action.COMMENTED,
                actor=self.request.user,
                message="Finance note updated.",
                from_status=prev_status,
                to_status=req.status,
            )
            messages.success(self.request, "Finance details updated.")

        return super().form_valid(form)


# ---------------------------------------------------------------------------
# Admin dashboards: bills summary, requests, employee + status summaries
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
        rows = (
            ReimbursementRequest.objects.values("status")
            .annotate(
                total_amount=Sum("total_amount"),
                request_count=Count("id"),
            )
            .order_by("status")
        )
        ctx["rows"] = rows
        ctx["status_labels"] = dict(ReimbursementRequest.Status.choices)
        return ctx


# ---------------------------------------------------------------------------
# Admin: Settings + Approver Mapping (manager & finance per employee)
# ---------------------------------------------------------------------------

class ApproverMappingAdminView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Admin-only page where:

    - Admin can edit global reimbursement settings (emails, flags).
    - Admin can quickly assign manager/finance for all employees in one grid.
    - Optional bulk apply manager/finance to everyone.
    """

    permission_code = "reimbursement_admin"
    template_name = "reimbursement/admin_approver_mapping.html"

    def get_users_queryset(self):
        return User.objects.filter(is_active=True).order_by(
            "first_name",
            "last_name",
            "username",
        )

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        settings_obj = ReimbursementSettings.get_solo()
        ctx["settings_form"] = ReimbursementSettingsForm(instance=settings_obj)
        ctx["bulk_form"] = ApproverMappingBulkForm()

        users = self.get_users_queryset()
        mappings = {
            m.employee_id: m
            for m in ReimbursementApproverMapping.objects.select_related(
                "manager",
                "finance",
            )
        }

        # Original objects (kept for any other use)
        ctx["users"] = users
        ctx["mappings"] = mappings
        ctx["all_users_for_select"] = users

        # Rows used by template: direct mapping for each user
        ctx["rows"] = [
            {"user": u, "mapping": mappings.get(u.id)}
            for u in users
        ]

        return ctx

    def post(self, request, *args, **kwargs):
        if "save_settings" in request.POST:
            return self._handle_save_settings(request)
        if "apply_bulk" in request.POST:
            return self._handle_apply_bulk(request)
        if "save_mappings" in request.POST:
            return self._handle_save_mappings(request)
        messages.error(request, "Unknown action.")
        return redirect("reimbursement:approver_mapping_admin")

    def _handle_save_settings(self, request):
        settings_obj = ReimbursementSettings.get_solo()
        form = ReimbursementSettingsForm(request.POST, instance=settings_obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Reimbursement settings updated.")
        else:
            messages.error(request, "Please correct errors in settings form.")
        return redirect("reimbursement:approver_mapping_admin")

    def _handle_apply_bulk(self, request):
        form = ApproverMappingBulkForm(request.POST)
        if not form.is_valid():
            messages.error(request, "Please correct errors in bulk form.")
            return redirect("reimbursement:approver_mapping_admin")

        apply_mgr = form.cleaned_data.get("apply_manager_to_all")
        apply_fin = form.cleaned_data.get("apply_finance_to_all")
        mgr_for_all = form.cleaned_data.get("manager_for_all")
        fin_for_all = form.cleaned_data.get("finance_for_all")

        users = self.get_users_queryset()
        count = 0
        for user in users:
            mapping, _ = ReimbursementApproverMapping.objects.get_or_create(
                employee=user
            )
            changed = False
            if apply_mgr:
                mapping.manager = mgr_for_all
                changed = True
            if apply_fin:
                mapping.finance = fin_for_all
                changed = True
            if changed:
                # Remove empty mappings (no manager nor finance)
                if not mapping.manager and not mapping.finance:
                    mapping.delete()
                else:
                    mapping.save()
                count += 1

        messages.success(
            self.request,
            f"Bulk mapping applied to {count} employees.",
        )
        return redirect("reimbursement:approver_mapping_admin")

    def _handle_save_mappings(self, request):
        users = self.get_users_queryset()
        count = 0
        for user in users:
            mgr_id = request.POST.get(f"manager_{user.id}") or ""
            fin_id = request.POST.get(f"finance_{user.id}") or ""

            manager = User.objects.filter(pk=mgr_id).first() if mgr_id else None
            finance = User.objects.filter(pk=fin_id).first() if fin_id else None

            try:
                mapping = ReimbursementApproverMapping.objects.get(employee=user)
            except ReimbursementApproverMapping.DoesNotExist:
                mapping = None

            if not manager and not finance:
                if mapping:
                    mapping.delete()
                    count += 1
                continue

            if not mapping:
                mapping = ReimbursementApproverMapping(employee=user)

            mapping.manager = manager
            mapping.finance = finance
            mapping.save()
            count += 1

        messages.success(self.request, f"Mappings saved for {count} employees.")
        return redirect("reimbursement:approver_mapping_admin")


# ---------------------------------------------------------------------------
# Secure receipt download
# ---------------------------------------------------------------------------

def download_receipt(
    request,
    line_id: Optional[int] = None,
    expense_id: Optional[int] = None,
):
    """
    Serve receipt files securely:

    - Employee can see their own receipts.
    - Manager/management/finance/admin can see receipts for requests they handle.
    """
    user = request.user
    if not user.is_authenticated:
        raise Http404

    line: Optional[ReimbursementLine] = None
    expense: Optional[ExpenseItem] = None
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
        req = line.request
    elif expense_id is not None:
        expense = get_object_or_404(
            ExpenseItem.objects.select_related("created_by"),
            pk=expense_id,
        )
        file_field = expense.receipt_file
        owner = expense.created_by
        req = None
    else:
        raise Http404

    if not file_field:
        raise Http404("No receipt file attached.")

    # Permission checks
    allowed = False
    if user == owner:
        allowed = True
    elif _user_is_admin(user) or _user_is_finance(user) or _user_is_manager(user):
        allowed = True
    else:
        allowed = False

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
    - Logs status change and sends follow-up notification.
    """
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
            ReimbursementLog.Action.STATUS_CHANGED,
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
# LEGACY VIEWS (single-bill Reimbursement model)
# ---------------------------------------------------------------------------

class LegacyMyReimbursementsView(LoginRequiredMixin, PermissionRequiredMixin, ListView):
    """
    Legacy 'My Reimbursements' for the simple Reimbursement model.
    New UI uses ReimbursementRequest instead.
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
    Legacy single-bill reimbursement create view (not used by new flows).
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
