from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.views.generic import TemplateView

from apps.users.mixins import PermissionRequiredMixin
from .models import ReimbursementLine, validate_receipt_file


def _safe_back_url(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    if v.startswith("/") and "://" not in v and "//" not in v[1:]:
        return v
    return None


class FinanceAttachReceiptView(LoginRequiredMixin, PermissionRequiredMixin, TemplateView):
    """
    Simple page for Finance/Admin to attach a missing receipt to a bill line.
    GET: show a small upload form
    POST: validate + save the file, then redirect back
    """
    permission_code = "reimbursement_finance_review"
    template_name = "reimbursement/finance_attach_receipt.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        line = get_object_or_404(
            ReimbursementLine.objects.select_related("request", "expense_item"),
            pk=self.kwargs.get("pk"),
        )
        ctx["line"] = line
        ctx["request_obj"] = line.request
        ctx["back_url"] = _safe_back_url(self.request.GET.get("return"))
        return ctx

    def post(self, request, *args, **kwargs):
        line = get_object_or_404(
            ReimbursementLine.objects.select_related("request", "expense_item"),
            pk=kwargs.get("pk"),
        )
        back = _safe_back_url(request.POST.get("return"))
        f = request.FILES.get("receipt_file")

        if not f:
            messages.error(request, "Please choose a file to upload.")
            return redirect(back or reverse("reimbursement:finance_verify", args=[line.request_id]))

        try:
            validate_receipt_file(f)
        except Exception as e:
            msg = getattr(e, "message", str(e)) or "Invalid file."
            messages.error(request, msg)
            return redirect(back or reverse("reimbursement:finance_attach_receipt", args=[line.pk]))

        # Save to the bill line
        line.receipt_file = f
        line.save(update_fields=["receipt_file", "updated_at"])

        # If the linked expense has no file, fill it too (helps employee views)
        exp = line.expense_item
        try:
            has_exp_file = bool(getattr(exp, "receipt_file", None) and getattr(exp.receipt_file, "name", ""))
            if exp and not has_exp_file:
                exp.receipt_file = line.receipt_file
                exp.save(update_fields=["receipt_file", "updated_at"])
        except Exception:
            # Keep going even if expense update fails
            pass

        messages.success(request, "Receipt attached to the bill.")
        return redirect(back or reverse("reimbursement:finance_verify", args=[line.request_id]))
