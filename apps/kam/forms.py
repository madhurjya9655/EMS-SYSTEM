# File: E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\kam\forms.py
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from .models import (
    CallLog,
    CollectionPlan,
    CollectionTxn,
    Customer,
    TargetLine,
    VisitActual,
    VisitBatch,
    VisitPlan,
)

User = get_user_model()


# ---------------------------
# Visit Planning (single-line)
# ---------------------------
class VisitPlanForm(forms.ModelForm):
    """
    Single-visit form supporting:
      - visit_category (Vendor/Customer/Supplier/Warehouse)
      - customer nullable when category != CUSTOMER
      - counterparty_name required when category != CUSTOMER
      - visit_date_to optional but must be >= visit_date if present
      - location required at form layer
    """

    class Meta:
        model = VisitPlan
        # B2: visit_category must appear at top
        fields = [
            "visit_category",
            "customer",
            "counterparty_name",
            "visit_date",
            "visit_date_to",
            "visit_type",
            "purpose",
            "expected_sales_mt",
            "expected_collection",
            "location",
        ]
        widgets = {
            "visit_date": forms.DateInput(attrs={"type": "date"}),
            "visit_date_to": forms.DateInput(attrs={"type": "date"}),
            "purpose": forms.TextInput(attrs={"placeholder": "Purpose of visit"}),
            "expected_sales_mt": forms.NumberInput(attrs={"step": "0.001", "min": "0"}),
            "expected_collection": forms.NumberInput(attrs={"step": "0.01", "min": "0"}),
            "location": forms.TextInput(attrs={"placeholder": "Enter location (required)"}),
            "counterparty_name": forms.TextInput(attrs={"placeholder": "Vendor / Supplier / Warehouse name (if applicable)"}),
        }

    def clean(self):
        data = super().clean()

        d1 = data.get("visit_date")
        d2 = data.get("visit_date_to")
        if d1 and d2 and d2 < d1:
            self.add_error("visit_date_to", "End date cannot be earlier than start date.")

        category = data.get("visit_category")
        customer = data.get("customer")
        counterparty = (data.get("counterparty_name") or "").strip()

        if category == VisitPlan.CAT_CUSTOMER:
            if not customer:
                self.add_error("customer", "Customer is required for Customer Visit.")
        else:
            # Vendor / Supplier / Warehouse visits
            if customer:
                self.add_error("customer", "Customer must be empty for Vendor/Supplier/Warehouse visit.")
            if not counterparty:
                self.add_error("counterparty_name", "Counterparty name is required for Vendor/Supplier/Warehouse visit.")

        if not (data.get("location") or "").strip():
            self.add_error("location", "Location is required.")

        return data


# --------------------------------------
# Visit Batch (header) + Multi-line input
# --------------------------------------
class VisitBatchForm(forms.ModelForm):
    """
    Header for batch submission.

    Template sends:
      - visit_category, from_date, to_date, purpose
      - customers (multi-select) when category=CUSTOMER
      - counterparty_name[], counterparty_location[], counterparty_purpose[] when not customer

    Note: In Section B4, `purpose` is used as mandatory remarks for Proceed-to-Manager
    to avoid schema changes/migrations in live production.
    """

    customers = forms.ModelMultipleChoiceField(
        queryset=Customer.objects.all().order_by("name"),
        required=False,
        help_text="Pick one or more customers (required for Customer Visit).",
    )

    class Meta:
        model = VisitBatch
        fields = ["from_date", "to_date", "visit_category", "purpose"]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
            "purpose": forms.TextInput(attrs={"placeholder": "Purpose / Remarks (required for Proceed to Manager)"}),
        }

    def clean(self):
        data = super().clean()

        d1 = data.get("from_date")
        d2 = data.get("to_date")
        if d1 and d2 and d2 < d1:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        category = data.get("visit_category")
        customers = self.cleaned_data.get("customers")

        if category == VisitPlan.CAT_CUSTOMER:
            if not customers or customers.count() == 0:
                self.add_error("customers", "Select at least one customer for Customer Visit.")
        else:
            if customers and customers.count() > 0:
                self.add_error("customers", "Do not select customers for Vendor/Supplier/Warehouse batch.")

        return data


class MultiVisitPlanLineForm(forms.Form):
    """
    For Vendor/Supplier/Warehouse lines inside batch submission.

    IMPORTANT:
    The template uses list-input names:
      - counterparty_name[]
      - counterparty_location[]
      - counterparty_purpose[]

    Views should read them using request.POST.getlist("counterparty_name[]"), etc.
    This form is mainly a validation helper per-line (views can instantiate per row).
    """

    counterparty_name = forms.CharField(
        required=True,
        max_length=255,
        widget=forms.TextInput(attrs={"placeholder": "Name (Vendor/Supplier/Warehouse)"}),
    )
    counterparty_location = forms.CharField(
        required=True,
        max_length=255,
        widget=forms.TextInput(attrs={"placeholder": "Location"}),
    )
    counterparty_purpose = forms.CharField(
        required=False,
        max_length=128,
        widget=forms.TextInput(attrs={"placeholder": "Purpose (optional)"}),
    )


# ---------------------------
# Post-visit capture
# ---------------------------
class VisitActualForm(forms.ModelForm):
    """
    After-visit mandatory capture (enforced at form layer):
      - actual_sales_mt
      - actual_collection
      - summary
      - confirmed_location
    """

    class Meta:
        model = VisitActual
        fields = [
            "summary",
            "successful",
            "not_success_reason",
            "actual_sales_mt",
            "actual_collection",
            "next_action",
            "next_action_date",
            "reminder_cc_manager",
            "confirmed_location",
        ]
        widgets = {
            "actual_sales_mt": forms.NumberInput(attrs={"step": "0.001", "min": "0"}),
            "actual_collection": forms.NumberInput(attrs={"step": "0.01", "min": "0"}),
            "next_action_date": forms.DateInput(attrs={"type": "date"}),
            "summary": forms.Textarea(attrs={"rows": 3, "placeholder": "Remarks / summary of the visit"}),
            "confirmed_location": forms.TextInput(attrs={"placeholder": "Confirm actual location visited"}),
        }

    def clean(self):
        data = super().clean()

        successful = data.get("successful")
        reason = (data.get("not_success_reason") or "").strip()
        if (successful is False) and not reason:
            self.add_error("not_success_reason", "Please select a reason when the visit was not successful.")

        def _must_number(val: Optional[Decimal], field: str, label: str):
            if val is None:
                self.add_error(field, f"{label} is required.")
                return
            try:
                Decimal(val)
            except Exception:
                self.add_error(field, f"{label} must be a number.")

        _must_number(data.get("actual_sales_mt"), "actual_sales_mt", "Actual Sales (MT)")
        _must_number(data.get("actual_collection"), "actual_collection", "Actual Collection (₹)")

        summary = (data.get("summary") or "").strip()
        if not summary:
            self.add_error("summary", "Remarks are required.")

        cloc = (data.get("confirmed_location") or "").strip()
        if not cloc:
            self.add_error("confirmed_location", "Confirmed location is required.")

        return data


# ---------------------------
# Quick entry: Calls
# ---------------------------
class CallForm(forms.ModelForm):
    class Meta:
        model = CallLog
        fields = ["customer", "call_datetime", "duration_minutes", "summary", "outcome"]
        widgets = {
            "call_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "summary": forms.Textarea(attrs={"rows": 3}),
        }


# ---------------------------
# Quick entry: Collections
# ---------------------------
class CollectionForm(forms.ModelForm):
    class Meta:
        model = CollectionTxn
        fields = ["customer", "txn_datetime", "amount", "mode", "reference"]
        widgets = {
            "txn_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "amount": forms.NumberInput(attrs={"step": "0.01", "min": "0"}),
        }


# ---------------------------
# Targets (inline line editor)
# ---------------------------
class TargetLineInlineForm(forms.ModelForm):
    kam = forms.ModelChoiceField(
        queryset=User.objects.filter(is_active=True).order_by("username"),
        empty_label="Select user…",
        help_text="Assign targets to a KAM (active users only).",
    )

    class Meta:
        model = TargetLine
        fields = [
            "kam",
            "sales_target_mt",
            "visits_target",
            "calls_target",
            "leads_target_mt",
            "nbd_target_monthly",
            "collections_plan_amount",
        ]
        widgets = {
            "sales_target_mt": forms.NumberInput(attrs={"step": "0.001", "min": "0"}),
            "leads_target_mt": forms.NumberInput(attrs={"step": "0.001", "min": "0"}),
            "collections_plan_amount": forms.NumberInput(attrs={"step": "0.01", "min": "0"}),
        }


# ---------------------------
# Collections Plan (dual mode)
# ---------------------------
class CollectionPlanForm(forms.ModelForm):
    """
    Dual-mode planning:
      - Period mode: (period_type, period_id) present; from/to blank
      - Range mode : (from_date, to_date) present; period_* blank
    """

    class Meta:
        model = CollectionPlan
        fields = ["period_type", "period_id", "from_date", "to_date", "customer", "planned_amount"]
        widgets = {
            "planned_amount": forms.NumberInput(attrs={"step": "0.01", "min": "0"}),
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
        }

    def clean(self):
        data = super().clean()

        period_type = (data.get("period_type") or "").strip()
        period_id = (data.get("period_id") or "").strip()
        f = data.get("from_date")
        t = data.get("to_date")

        period_mode = bool(period_type and period_id)
        range_mode = bool(f and t)

        if period_mode and range_mode:
            raise ValidationError("Provide either Period Type/Id or From/To date range, not both.")

        if not period_mode and not range_mode:
            raise ValidationError("Provide Period Type/Id or From/To date range.")

        if range_mode and t < f:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        return data
