# FILE: apps/kam/forms.py
# PURPOSE: All collection forms including CollectionPlanActualForm for recording actuals.
# UPDATED: 2026-03-05

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from .models import (
    Customer,
    VisitPlan,
    VisitActual,
    CallLog,
    CollectionTxn,
    TargetSetting,
    CollectionPlan,
    VisitBatch,
    KamManagerMapping,
)

User = get_user_model()


# ─────────────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_decimal(val) -> Decimal:
    try:
        return Decimal(val or 0)
    except Exception:
        return Decimal(0)


def _clean_decimal_field(value, allow_blank: bool = True) -> Optional[Decimal]:
    if value is None:
        return None if allow_blank else Decimal(0)
    if isinstance(value, Decimal):
        return value
    s = str(value).strip()
    if s == "":
        return None if allow_blank else Decimal(0)
    try:
        return Decimal(s)
    except Exception:
        raise ValidationError("Enter a valid number.")


# ─────────────────────────────────────────────────────────────────────────────
# Visit: Single plan form
# ─────────────────────────────────────────────────────────────────────────────

class VisitPlanForm(forms.ModelForm):
    """
    Used for SINGLE visit pane.
    NOTE: View enforces that this always saves as DRAFT; do not put approval logic here.
    purpose / location / remarks are all optional — validation removed.
    """

    class Meta:
        model = VisitPlan
        fields = [
            "visit_category",
            "visit_type",
            "customer",
            "counterparty_name",
            "visit_date",
            "visit_date_to",
            "purpose",
            "location",
            "expected_sales_mt",
            "expected_collection",
        ]
        widgets = {
            "visit_date": forms.DateInput(attrs={"type": "date"}),
            "visit_date_to": forms.DateInput(attrs={"type": "date"}),
            "purpose": forms.Textarea(attrs={"rows": 2, "placeholder": "Purpose / remarks (optional)"}),
            "location": forms.Textarea(attrs={"rows": 2, "placeholder": "Location (optional)"}),
        }

    def clean(self):
        data = super().clean()
        cat = data.get("visit_category")
        cust = data.get("customer")
        cpn = (data.get("counterparty_name") or "").strip()
        vd = data.get("visit_date")
        vdt = data.get("visit_date_to")

        if not vd:
            self.add_error("visit_date", "Visit Date is required.")
        if not vdt:
            self.add_error("visit_date_to", "To Date is required.")
        if vd and vdt and vdt < vd:
            self.add_error("visit_date_to", "To Date cannot be earlier than Visit Date.")

        if cat == VisitPlan.CAT_CUSTOMER:
            if not cust:
                self.add_error("customer", "Customer is required for Customer Visit.")
            if cpn:
                self.add_error("counterparty_name", "Counterparty Name must be empty for Customer Visit.")
        else:
            if not cpn:
                self.add_error("counterparty_name", "Name is required for non-customer visit.")
            if cust:
                self.add_error("customer", "Customer must be empty for non-customer visit.")

        try:
            data["expected_sales_mt"] = _clean_decimal_field(data.get("expected_sales_mt"), allow_blank=True)
        except ValidationError as e:
            self.add_error("expected_sales_mt", e)

        try:
            data["expected_collection"] = _clean_decimal_field(data.get("expected_collection"), allow_blank=True)
        except ValidationError as e:
            self.add_error("expected_collection", e)

        return data


# ─────────────────────────────────────────────────────────────────────────────
# Visit Actual form
# ─────────────────────────────────────────────────────────────────────────────

class VisitActualForm(forms.ModelForm):
    """
    meeting_notes is OPTIONAL — remarks can be left blank.
    """

    class Meta:
        model = VisitActual
        fields = [
            "actual_datetime",
            "confirmed_location",
            "successful",
            "not_success_reason",
            "meeting_notes",
            "actual_sales_mt",
            "actual_collection",
            "next_action",
            "next_action_date",
            "reminder_cc_manager",
        ]
        widgets = {
            "actual_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "meeting_notes": forms.Textarea(
                attrs={"rows": 3, "placeholder": "Meeting notes / remarks (optional)"}
            ),
            "next_action": forms.Textarea(attrs={"rows": 2}),
            "next_action_date": forms.DateInput(attrs={"type": "date"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["meeting_notes"].required = False
        self.fields["confirmed_location"].required = False

        if self.instance and getattr(self.instance, "pk", None):
            if not (self.instance.meeting_notes or "").strip() and (self.instance.summary or "").strip():
                self.fields["meeting_notes"].initial = self.instance.summary or ""

    def clean(self):
        data = super().clean()
        successful = data.get("successful")
        reason = data.get("not_success_reason")

        if successful is False and not reason:
            self.add_error("not_success_reason", "Please select a reason when visit is not successful.")
        if successful is True:
            data["not_success_reason"] = None

        try:
            data["actual_sales_mt"] = _clean_decimal_field(data.get("actual_sales_mt"), allow_blank=True)
        except ValidationError as e:
            self.add_error("actual_sales_mt", e)

        try:
            data["actual_collection"] = _clean_decimal_field(data.get("actual_collection"), allow_blank=True)
        except ValidationError as e:
            self.add_error("actual_collection", e)

        return data

    def save(self, commit=True):
        inst = super().save(commit=False)
        notes = (inst.meeting_notes or "").strip()
        inst.meeting_notes = notes or None
        inst.summary = inst.meeting_notes
        if commit:
            inst.save()
        return inst


# ─────────────────────────────────────────────────────────────────────────────
# Call & Collection forms
# ─────────────────────────────────────────────────────────────────────────────

class CallForm(forms.ModelForm):
    class Meta:
        model = CallLog
        fields = ["customer", "call_datetime", "duration_minutes", "summary", "outcome"]
        widgets = {
            "call_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "summary": forms.Textarea(
                attrs={"rows": 3, "placeholder": "Call summary / remarks (optional)"}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["summary"].required = False
        self.fields["outcome"].required = False
        self.fields["duration_minutes"].required = False


class CollectionForm(forms.ModelForm):
    class Meta:
        model = CollectionTxn
        fields = ["customer", "txn_datetime", "amount", "mode", "reference"]
        widgets = {
            "txn_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["mode"].required = False
        self.fields["reference"].required = False

    def clean_amount(self):
        amt = self.cleaned_data.get("amount")
        amt = _clean_decimal_field(amt, allow_blank=False)
        if amt is not None and amt < 0:
            raise ValidationError("Amount cannot be negative.")
        return amt


# ─────────────────────────────────────────────────────────────────────────────
# Batch visit forms
# ─────────────────────────────────────────────────────────────────────────────

class VisitBatchForm(forms.ModelForm):
    customers = forms.ModelMultipleChoiceField(
        queryset=Customer.objects.none(),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 10}),
        help_text="Select customers for Customer Visit batch.",
    )

    class Meta:
        model = VisitBatch
        fields = ["visit_category", "from_date", "to_date", "purpose"]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
            "purpose": forms.Textarea(
                attrs={"rows": 2, "placeholder": "Remarks / purpose (optional)"}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["purpose"].required = False

    def clean(self):
        data = super().clean()
        fd = data.get("from_date")
        td = data.get("to_date")
        pur = (data.get("purpose") or "").strip()

        if not fd:
            self.add_error("from_date", "From date is required.")
        if not td:
            self.add_error("to_date", "To date is required.")
        if fd and td and td < fd:
            self.add_error("to_date", "To date cannot be earlier than From date.")
        if len(pur) > 1000:
            self.add_error("purpose", "Remarks too long (max 1000 chars).")

        return data


class MultiVisitPlanLineForm(forms.Form):
    counterparty_name = forms.CharField(required=True, max_length=255)
    counterparty_location = forms.CharField(
        required=False, max_length=255,
        widget=forms.TextInput(attrs={"placeholder": "Location (optional)"}),
    )
    counterparty_purpose = forms.CharField(
        required=False, max_length=500,
        widget=forms.TextInput(attrs={"placeholder": "Purpose / remarks (optional)"}),
    )

    def clean_counterparty_name(self):
        s = (self.cleaned_data.get("counterparty_name") or "").strip()
        if not s:
            raise ValidationError("Name is required.")
        return s


# ─────────────────────────────────────────────────────────────────────────────
# Target forms
# ─────────────────────────────────────────────────────────────────────────────

class TargetSettingForm(forms.ModelForm):
    class Meta:
        model = TargetSetting
        fields = [
            "from_date", "to_date",
            "sales_target_mt", "calls_target",
            "leads_target_mt", "collections_target_amount",
        ]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
        }

    def clean(self):
        data = super().clean()
        fd = data.get("from_date")
        td = data.get("to_date")
        if fd and td and td < fd:
            self.add_error("to_date", "To date cannot be earlier than From date.")
        return data


class ManagerTargetForm(forms.Form):
    id = forms.CharField(required=False)
    from_date = forms.DateField(required=True, widget=forms.DateInput(attrs={"type": "date"}))
    to_date = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))
    fixed_for_next_3_months = forms.BooleanField(required=False, initial=False)
    kam_username = forms.ChoiceField(
        required=False, choices=[],
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    bulk_all_kams = forms.BooleanField(required=False, initial=False)
    sales_target_mt = forms.DecimalField(required=True, max_digits=12, decimal_places=2)
    leads_target_mt = forms.DecimalField(required=True, max_digits=12, decimal_places=2)
    calls_target = forms.IntegerField(required=True, min_value=0)
    collections_target_amount = forms.DecimalField(required=False, max_digits=14, decimal_places=2)
    auto_collections_30pct_overdue = forms.BooleanField(required=False, initial=True)

    def __init__(self, *args, **kwargs):
        self.kam_options = kwargs.pop("kam_options", []) or []
        super().__init__(*args, **kwargs)
        choices = [("", "— Select KAM —")]
        choices += [(u, u) for u in self.kam_options]
        self.fields["kam_username"].choices = choices

    def clean_kam_username(self):
        u = (self.cleaned_data.get("kam_username") or "").strip()
        if u and self.kam_options and (u not in self.kam_options):
            raise ValidationError("Selected KAM is not in your allowed scope.")
        return u

    def clean(self):
        data = super().clean()
        fd = data.get("from_date")
        td = data.get("to_date")
        if not fd:
            self.add_error("from_date", "From date is required.")
        if td and fd and td < fd:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        for field, label in [
            ("sales_target_mt", "Sales target"),
            ("leads_target_mt", "Leads target"),
            ("calls_target", "Calls target"),
        ]:
            val = data.get(field)
            if val is not None and val < 0:
                self.add_error(field, f"{label} cannot be negative.")

        coll_amt = data.get("collections_target_amount")
        if coll_amt is not None and coll_amt < 0:
            self.add_error("collections_target_amount", "Collections target cannot be negative.")
        auto = bool(data.get("auto_collections_30pct_overdue"))
        if not auto and coll_amt is None:
            self.add_error("collections_target_amount", "Enter a collections target or enable auto 30% overdue.")
        return data


# ─────────────────────────────────────────────────────────────────────────────
# Collections plan — Create / Edit
# ─────────────────────────────────────────────────────────────────────────────

class CollectionPlanForm(forms.ModelForm):
    """
    FIX 2026-03-03: Removed strict mutual-exclusion error; view applies period-takes-priority logic.
    FIX 2026-03-04: notes/remarks are now optional (required=False enforced).
    """

    class Meta:
        model = CollectionPlan
        fields = [
            "customer", "planned_amount", "notes",
            "from_date", "to_date",
            "period_type", "period_id",
        ]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
            "period_id": forms.TextInput(attrs={"placeholder": "e.g. 2026-W09 / 2026-02 / 2026-Q1 / 2026"}),
            "notes": forms.Textarea(attrs={"rows": 2, "placeholder": "Remarks / notes (optional)"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["notes"].required = False
        self.fields["period_type"].required = False
        self.fields["period_id"].required = False
        self.fields["from_date"].required = False
        self.fields["to_date"].required = False

    def clean_planned_amount(self):
        amt = self.cleaned_data.get("planned_amount")
        amt = _clean_decimal_field(amt, allow_blank=False)
        if amt is not None and amt < 0:
            raise ValidationError("Planned amount cannot be negative.")
        return amt

    def clean(self):
        data = super().clean()
        fd = data.get("from_date")
        td = data.get("to_date")
        ptype = data.get("period_type")
        pid = (data.get("period_id") or "").strip()

        has_period = bool(ptype) and bool(pid)
        has_range = bool(fd) and bool(td)

        if fd and td and td < fd:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        if ptype and not pid:
            self.add_error("period_id", "Period ID is required when Period Type is selected.")
        if pid and not ptype:
            self.add_error("period_type", "Period Type is required when Period ID is provided.")

        if fd and not td:
            self.add_error("to_date", "To date is required when From date is provided.")
        if td and not fd:
            self.add_error("from_date", "From date is required when To date is provided.")

        if not has_period and not has_range:
            raise ValidationError("Provide either Period Type + Period ID, or From Date + To Date.")

        data["period_id"] = pid or None
        return data


# ─────────────────────────────────────────────────────────────────────────────
# Collection Plan — Record Actual Collection
# ─────────────────────────────────────────────────────────────────────────────

class CollectionPlanActualForm(forms.ModelForm):
    """
    Used to record actual collection against an existing CollectionPlan entry.
    Only exposes actual_amount, collection_date, and collection_reference.
    The view links this to an existing CollectionPlan instance (by pk).

    Rules:
    - planned_amount is NEVER modified by this form.
    - actual_amount is EDITABLE (can be updated to correct entry).
    - collection_date is REQUIRED when recording actual.
    - collection_status is AUTO-DERIVED by CollectionPlan.save().
    """

    class Meta:
        model = CollectionPlan
        fields = ["actual_amount", "collection_date", "collection_reference", "notes"]
        widgets = {
            "collection_date": forms.DateInput(attrs={"type": "date"}),
            "notes": forms.Textarea(attrs={"rows": 2, "placeholder": "Remarks (optional)"}),
            "collection_reference": forms.TextInput(
                attrs={"placeholder": "Cheque no / UTR / reference (optional)"}
            ),
        }
        labels = {
            "actual_amount": "Amount Collected (₹)",
            "collection_date": "Date of Collection",
            "collection_reference": "Reference / UTR",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["notes"].required = False
        self.fields["collection_reference"].required = False
        self.fields["collection_date"].required = True
        self.fields["actual_amount"].required = True

    def clean_actual_amount(self):
        amt = self.cleaned_data.get("actual_amount")
        amt = _clean_decimal_field(amt, allow_blank=False)
        if amt is not None and amt < 0:
            raise ValidationError("Amount cannot be negative.")
        return amt

    def clean(self):
        data = super().clean()
        if not data.get("collection_date"):
            self.add_error("collection_date", "Collection date is required.")
        return data


# ─────────────────────────────────────────────────────────────────────────────
# Target line inline / KAM-Manager mapping forms
# ─────────────────────────────────────────────────────────────────────────────

class TargetLineInlineForm(forms.Form):
    grade = forms.CharField(required=True, max_length=50)
    size = forms.CharField(required=False, max_length=50)
    target_mt = forms.DecimalField(required=True, max_digits=12, decimal_places=2)


class KamManagerMappingForm(forms.Form):
    kam_username = forms.CharField(required=True)
    manager_username = forms.CharField(required=True)
    active = forms.BooleanField(required=False, initial=True)

    def clean_kam_username(self):
        uname = (self.cleaned_data.get("kam_username") or "").strip()
        if not uname:
            raise ValidationError("KAM username is required.")
        if not User.objects.filter(username=uname, is_active=True).exists():
            raise ValidationError("KAM user not found.")
        return uname

    def clean_manager_username(self):
        uname = (self.cleaned_data.get("manager_username") or "").strip()
        if not uname:
            raise ValidationError("Manager username is required.")
        if not User.objects.filter(username=uname, is_active=True).exists():
            raise ValidationError("Manager user not found.")
        return uname

    def clean(self):
        data = super().clean()
        if data.get("kam_username") and data.get("manager_username"):
            if data["kam_username"] == data["manager_username"]:
                self.add_error("manager_username", "Manager cannot be the same as KAM.")
        return data