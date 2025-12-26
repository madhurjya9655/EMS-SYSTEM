# apps/reimbursement/forms.py
from __future__ import annotations

import os
from typing import Optional, Iterable, List

from django import forms
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from .models import (
    ExpenseItem,
    ReimbursementRequest,
    ReimbursementLine,
    ReimbursementApproverMapping,
    ReimbursementSettings,
    Reimbursement,
    REIMBURSEMENT_CATEGORY_CHOICES,
    GST_TYPE_CHOICES,
)

User = get_user_model()


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

class DateInput(forms.DateInput):
    input_type = "date"


def _allowed_exts() -> List[str]:
    """
    Allowed file extensions for receipts / bills. Comes from settings so
    Admins can adjust without code changes.
    """
    exts = getattr(settings, "REIMBURSEMENT_ALLOWED_EXTENSIONS", None)
    if not exts:
        exts = [".jpg", ".jpeg", ".png", ".pdf", ".xls", ".xlsx"]
    if isinstance(exts, (list, tuple)):
        return [str(e).lower().strip() for e in exts if str(e).strip()]
    return [s.strip().lower() for s in str(exts).split(",") if s.strip()]


def _max_file_mb() -> int:
    return int(getattr(settings, "REIMBURSEMENT_MAX_RECEIPT_MB", 8))


def _validate_uploaded_file(f, *, field_label: str = "file") -> None:
    """
    Lightweight server-side validator for uploaded receipt/bill files:
    - Extension must be one of the allowed list
    - Size must be <= REIMBURSEMENT_MAX_RECEIPT_MB
    """
    if not f:
        return
    name = getattr(f, "name", "")
    ext = os.path.splitext(name)[1].lower()
    if ext not in _allowed_exts():
        raise ValidationError(
            f"Unsupported {field_label} type '{ext}'. "
            f"Allowed: {', '.join(_allowed_exts())}"
        )
    try:
        size = int(getattr(f, "size", 0))
    except Exception:
        size = 0
    if _max_file_mb() and size > _max_file_mb() * 1024 * 1024:
        raise ValidationError(
            f"{field_label.capitalize()} is too large. "
            f"Max size is {_max_file_mb()} MB."
        )


# ---------------------------------------------------------------------------
# Expense items (employee uploads / inbox)
# ---------------------------------------------------------------------------

class ExpenseItemForm(forms.ModelForm):
    """
    Form for employees to upload individual expenses (bills).

    - Vendor field is kept in the model (for old data) but hidden from the form.
    - New field gst_type: GST Bill / Non GST Bill.
    """

    class Meta:
        model = ExpenseItem
        fields = [
            "date",
            "category",
            "amount",
            "description",
            "gst_type",
            "receipt_file",
        ]
        widgets = {
            "date": DateInput(attrs={"class": "form-control"}),
            "category": forms.Select(
                attrs={"class": "form-select"},
                choices=REIMBURSEMENT_CATEGORY_CHOICES,
            ),
            "amount": forms.NumberInput(
                attrs={
                    "class": "form-control",
                    "step": "0.01",
                    "min": "0.01",
                    "placeholder": "Amount",
                }
            ),
            "description": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Description (optional)",
                }
            ),
            "gst_type": forms.RadioSelect(
                attrs={"class": "form-check-input"},
                choices=GST_TYPE_CHOICES,
            ),
            # Advertise Excel & common image/PDF types in the browser picker
            "receipt_file": forms.FileInput(
                attrs={
                    "class": "form-control",
                    "accept": ".pdf,.jpg,.jpeg,.png,.xls,.xlsx",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        """
        Optionally accept `user` kwarg to pre-fill sensible defaults later, if needed.
        """
        self.user = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)

        # UI labels / defaults
        self.fields["category"].label = "Type of Expense"
        self.fields["gst_type"].label = "Bill Type"
        self.fields["gst_type"].initial = "non_gst"

    def clean_receipt_file(self):
        f = self.files.get("receipt_file") or self.cleaned_data.get("receipt_file")
        _validate_uploaded_file(f, field_label="receipt")
        return f


class ExpenseStatusFilterForm(forms.Form):
    """
    Simple filter for the Expense Inbox list.
    """
    STATUS_CHOICES = [("", "All")] + list(ExpenseItem.Status.choices)

    status = forms.ChoiceField(
        choices=STATUS_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "form-select form-select-sm"}),
    )


# ---------------------------------------------------------------------------
# Reimbursement request creation (bulk from inbox)
# ---------------------------------------------------------------------------

class ReimbursementCreateForm(forms.ModelForm):
    """
    Employee-facing form to submit a new ReimbursementRequest from multiple ExpenseItems.

    NOTE:
    - Manager / Finance approvers are fully controlled by Admin via
      ReimbursementApproverMapping; employees cannot change them here.
    """

    expense_items = forms.ModelMultipleChoiceField(
        queryset=ExpenseItem.objects.none(),
        widget=forms.CheckboxSelectMultiple,
        required=True,
        help_text="Select one or more expenses from your inbox to include in this request.",
    )
    employee_note = forms.CharField(
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "form-control",
                "rows": 3,
                "placeholder": "Optional note for your manager / finance.",
            }
        ),
        help_text="Optional note that will be included in email notifications.",
    )

    class Meta:
        model = ReimbursementRequest
        fields = []  # We control fields explicitly above

    def __init__(self, *args, **kwargs):
        """
        Requires `user` kwarg (employee submitting).
        """
        self.user = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)

        # Limit selectable expenses to the employee's own, still usable items
        if self.user is not None:
            self.fields["expense_items"].queryset = ExpenseItem.objects.filter(
                created_by=self.user,
                status__in=[ExpenseItem.Status.SAVED, ExpenseItem.Status.SUBMITTED],
            ).order_by("-date", "-id")
        else:
            self.fields["expense_items"].queryset = ExpenseItem.objects.none()

    def clean_expense_items(self):
        qs = self.cleaned_data.get("expense_items")
        if not qs or qs.count() == 0:
            raise forms.ValidationError("Please select at least one expense.")
        # Ensure all items belong to the user
        if self.user is not None:
            for item in qs:
                if item.created_by_id != self.user.id:
                    raise forms.ValidationError("You can only submit your own expenses.")
        return qs


class RequestFilterForm(forms.Form):
    """
    Filter for 'My Requests' and summary pages.
    """
    STATUS_CHOICES = [("", "All statuses")] + list(ReimbursementRequest.Status.choices)

    status = forms.ChoiceField(
        choices=STATUS_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "form-select form-select-sm"}),
    )
    from_date = forms.DateField(
        required=False,
        widget=forms.DateInput(
            attrs={"type": "date", "class": "form-control form-control-sm"}
        ),
    )
    to_date = forms.DateField(
        required=False,
        widget=forms.DateInput(
            attrs={"type": "date", "class": "form-control form-control-sm"}
        ),
    )


# ---------------------------------------------------------------------------
# Manager / Management / Finance review forms
# ---------------------------------------------------------------------------

_MANAGER_DECISION_CHOICES = [
    ("approved", "Approve & Send Forward"),
    ("rejected", "Reject"),
    ("clarification", "Request Clarification"),
]


class ManagerApprovalForm(forms.ModelForm):
    decision = forms.ChoiceField(
        choices=_MANAGER_DECISION_CHOICES,
        widget=forms.Select(attrs={"class": "form-select"}),
        label="Manager Decision",
    )

    class Meta:
        model = ReimbursementRequest
        fields = [
            "manager_comment",
        ]
        widgets = {
            "manager_comment": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Optional comment for the employee.",
                }
            ),
        }

    def save(self, commit=True) -> ReimbursementRequest:
        obj: ReimbursementRequest = super().save(commit=False)
        obj.manager_decision = self.cleaned_data["decision"]
        if commit:
            obj.save(update_fields=["manager_decision", "manager_comment", "updated_at"])
        return obj


class ManagementApprovalForm(forms.ModelForm):
    decision = forms.ChoiceField(
        choices=_MANAGER_DECISION_CHOICES,
        widget=forms.Select(attrs={"class": "form-select"}),
        label="Management Decision",
    )

    class Meta:
        model = ReimbursementRequest
        fields = [
            "management_comment",
        ]
        widgets = {
            "management_comment": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Optional comment for the employee.",
                }
            ),
        }

    def save(self, commit=True) -> ReimbursementRequest:
        obj: ReimbursementRequest = super().save(commit=False)
        obj.management_decision = self.cleaned_data["decision"]
        if commit:
            obj.save(
                update_fields=[
                    "management_decision",
                    "management_comment",
                    "updated_at",
                ]
            )
        return obj


class FinanceProcessForm(forms.ModelForm):
    """
    Finance review + mark-paid form.
    """

    mark_paid = forms.BooleanField(
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        label="Mark as Claim Settled",  # renamed for Finance language
        help_text="Tick this box to mark the reimbursement as Claim Settled (Paid).",
    )

    class Meta:
        model = ReimbursementRequest
        fields = [
            "finance_note",
            "finance_payment_reference",
        ]
        widgets = {
            "finance_note": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Internal finance notes (optional).",
                }
            ),
            "finance_payment_reference": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Payment reference / transaction ID (required if marking as Claim Settled).",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # ✅ Gate the checkbox using the model's single source-of-truth guard,
        #    not a raw status == APPROVED check.
        try:
            ref = (self.instance.finance_payment_reference or "") if self.instance else ""
            can, msg = self.instance.can_mark_paid(ref) if self.instance else (False, "")
            if not can and "reference" not in (msg or "").lower():
                self.fields["mark_paid"].disabled = True
                # Keep a concise hint without exposing backend messages here.
                self.fields["mark_paid"].help_text = "Available after approvals, during Finance review."
        except Exception:
            # Never break rendering
            pass

    def clean(self):
        cleaned = super().clean()
        mark_paid = cleaned.get("mark_paid")
        ref = (cleaned.get("finance_payment_reference") or "").strip()

        # Use the model's single source of truth
        if mark_paid:
            ok, msg = self.instance.can_mark_paid(ref)
            if not ok:
                # Attach the message to the most relevant field for better UX
                if "reference" in msg.lower():
                    self.add_error("finance_payment_reference", msg)
                else:
                    # Non-field and checkbox error to make it obvious
                    self.add_error("mark_paid", msg)
                    self.add_error(None, msg)
        return cleaned


# ---------------------------
# Finance verification (NEW)
# ---------------------------

# IMPORTANT: values align with FinanceVerifyView.post which accepts "verify" / "rejected"
_FINANCE_VERIFY_CHOICES = [
    ("verify", "Verify & Send to Manager"),
    ("rejected", "Reject"),
]


class FinanceVerifyForm(forms.ModelForm):
    """
    First step Finance verification before manager approval.
    """

    decision = forms.ChoiceField(
        choices=_FINANCE_VERIFY_CHOICES,
        widget=forms.Select(attrs={"class": "form-select"}),
        label="Finance Decision",
    )

    class Meta:
        model = ReimbursementRequest
        fields = [
            "finance_note",
        ]
        widgets = {
            "finance_note": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Optional note (visible to employee & approvers).",
                }
            ),
        }

    def save(self, commit=True) -> ReimbursementRequest:
        obj: ReimbursementRequest = super().save(commit=False)
        # The actual status transition and verified_by/verified_at will be handled in the view.
        if commit:
            obj.save(update_fields=["finance_note", "updated_at"])
        return obj


# ---------------------------------------------------------------------------
# Admin: Settings & Approver mappings
# ---------------------------------------------------------------------------

class ReimbursementSettingsForm(forms.ModelForm):
    """
    Admin form to manage reimbursement email recipients and policy flags,
    including the approval-chain routing emails.
    """

    class Meta:
        model = ReimbursementSettings
        fields = [
            "admin_emails",
            "finance_emails",
            "management_emails",
            "require_management_approval",
            "daily_digest_enabled",
            "digest_hour_local",
            # NEW: approval chain routing (all editable by Admin)
            "approver_level1_email",   # e.g. vilas@blueoceansteels.com
            "approver_level2_email",   # e.g. akshay@blueoceansteels.com
            "approver_cc_emails",      # e.g. amreen@...
            "approver_bcc_emails",     # e.g. vilas@... (for BCC on level2 mail)
        ]
        widgets = {
            "admin_emails": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "admin1@example.com, admin2@example.com",
                }
            ),
            "finance_emails": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "finance1@example.com, finance2@example.com",
                }
            ),
            "management_emails": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "mgmt1@example.com, mgmt2@example.com",
                }
            ),
            "require_management_approval": forms.CheckboxInput(
                attrs={"class": "form-check-input"}
            ),
            "daily_digest_enabled": forms.CheckboxInput(
                attrs={"class": "form-check-input"}
            ),
            "digest_hour_local": forms.NumberInput(
                attrs={
                    "class": "form-control",
                    "min": 0,
                    "max": 23,
                }
            ),
            "approver_level1_email": forms.EmailInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Primary approver (e.g. vilas@blueoceansteels.com)",
                }
            ),
            "approver_level2_email": forms.EmailInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Next approver after level 1 (e.g. akshay@blueoceansteels.com)",
                }
            ),
            "approver_cc_emails": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "Comma-separated CC emails, e.g. amreen@blueoceansteels.com",
                }
            ),
            "approver_bcc_emails": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "Comma-separated BCC emails, e.g. vilas@blueoceansteels.com",
                }
            ),
        }

    def clean_digest_hour_local(self):
        hour = self.cleaned_data.get("digest_hour_local")
        if hour is None:
            return 9
        if not (0 <= hour <= 23):
            raise forms.ValidationError("Digest hour must be between 0 and 23.")
        return hour


class ApproverMappingForm(forms.ModelForm):
    """
    Per-employee mapping row (used in admin UI).
    """

    class Meta:
        model = ReimbursementApproverMapping
        fields = ["employee", "manager", "finance"]
        widgets = {
            "employee": forms.Select(attrs={"class": "form-select"}),
            "manager": forms.Select(attrs={"class": "form-select"}),
            "finance": forms.Select(attrs={"class": "form-select"}),
        }


class ApproverMappingBulkForm(forms.Form):
    """
    Admin helper form to set the same manager/finance for all employees in one go.
    (kept for compatibility even if current view uses ApproverDefaultsForm instead)
    """

    apply_manager_to_all = forms.BooleanField(
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        label="Apply manager to all",
    )
    apply_finance_to_all = forms.BooleanField(
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        label="Apply finance to all",
    )

    manager_for_all = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("first_name", "last_name", "username"),
        required=False,
        widget=forms.Select(attrs={"class": "form-select"}),
        label="Manager (for all)",
    )
    finance_for_all = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("first_name", "last_name", "username"),
        required=False,
        widget=forms.Select(attrs={"class": "form-select"}),
        label="Finance (for all)",
    )


# Extra helpers used by the new mapping view (grid-style)

class ApproverDefaultsForm(forms.Form):
    """
    Small helper form at the top of the mapping page.

    Admin can pick one Manager and/or one Finance user and click
    "Apply to all" – the view then pre-fills every row with those defaults
    before saving.
    """

    default_manager = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("first_name", "last_name", "username"),
        required=False,
        widget=forms.Select(attrs={"class": "form-select form-select-sm"}),
        label="Default Manager",
    )
    default_finance = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("first_name", "last_name", "username"),
        required=False,
        widget=forms.Select(attrs={"class": "form-select form-select-sm"}),
        label="Default Finance",
    )


ApproverMappingFormSet = forms.modelformset_factory(
    ReimbursementApproverMapping,
    form=ApproverMappingForm,
    extra=0,
    can_delete=True,
)


# ---------------------------------------------------------------------------
# Legacy forms (for `Reimbursement` model) – kept for backward compatibility
# ---------------------------------------------------------------------------

class ReimbursementForm(forms.ModelForm):
    """
    Legacy/simple reimbursement form (single bill).
    New flows should use ExpenseItem + ReimbursementRequest.
    """

    class Meta:
        model = Reimbursement
        fields = ["amount", "category", "bill"]
        widgets = {
            "amount": forms.NumberInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Amount",
                    "step": "0.01",
                    "min": "0.01",
                }
            ),
            "category": forms.Select(
                attrs={"class": "form-select"},
                choices=REIMBURSEMENT_CATEGORY_CHOICES,
            ),
            "bill": forms.FileInput(
                attrs={
                    "class": "form-control",
                    "accept": ".pdf,.jpg,.jpeg,.png,.xls,.xlsx",
                }
            ),
        }

    def clean_bill(self):
        f = self.files.get("bill") or self.cleaned_data.get("bill")
        _validate_uploaded_file(f, field_label="bill")
        return f


class ManagerReviewForm(forms.ModelForm):
    """
    Legacy manager review form for simple Reimbursement model.
    """

    class Meta:
        model = Reimbursement
        fields = ["status", "manager_comment"]
        widgets = {
            "status": forms.Select(attrs={"class": "form-select"}),
            "manager_comment": forms.Textarea(
                attrs={"class": "form-control", "rows": 3}
            ),
        }


class FinanceReviewForm(forms.ModelForm):
    """
    Legacy finance review form for simple Reimbursement model.
    """

    class Meta:
        model = Reimbursement
        fields = ["status", "finance_comment"]
        widgets = {
            "status": forms.Select(attrs={"class": "form-select"}),
            "finance_comment": forms.Textarea(
                attrs={"class": "form-control", "rows": 3}
            ),
        }
