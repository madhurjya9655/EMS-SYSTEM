# FILE: apps/kam/forms.py
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


class VisitPlanForm(forms.ModelForm):
    """
    Legacy single visit form.

    Purpose of Visit is mandatory.
    DB field remains VisitPlan.purpose.
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
            "purpose": forms.Textarea(
                attrs={
                    "rows": 2,
                    "placeholder": "Enter Purpose of Visit",
                    "required": "required",
                }
            ),
            "location": forms.Textarea(
                attrs={
                    "rows": 2,
                    "placeholder": "Location",
                }
            ),
        }
        labels = {
            "purpose": "Purpose of Visit",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["purpose"].required = True
        self.fields["purpose"].label = "Purpose of Visit"

    def clean(self):
        data = super().clean()

        cat = data.get("visit_category")
        cust = data.get("customer")
        cpn = (data.get("counterparty_name") or "").strip()
        vd = data.get("visit_date")
        vdt = data.get("visit_date_to")
        purpose = (data.get("purpose") or "").strip()

        if not vd:
            self.add_error("visit_date", "Visit Date is required.")

        if not vdt:
            self.add_error("visit_date_to", "To Date is required.")

        if vd and vdt and vdt < vd:
            self.add_error("visit_date_to", "To Date cannot be earlier than Visit Date.")

        if not purpose:
            self.add_error("purpose", "Purpose of Visit is required.")
        elif len(purpose) > 2000:
            self.add_error("purpose", "Purpose of Visit is too long (max 2000 characters).")
        else:
            data["purpose"] = purpose

        if cat == VisitPlan.CAT_CUSTOMER:
            if not cust:
                self.add_error("customer", "Customer is required for Customer Visit.")

            if cpn:
                self.add_error(
                    "counterparty_name",
                    "Counterparty Name must be empty for Customer Visit.",
                )

        else:
            if not cpn:
                self.add_error(
                    "counterparty_name",
                    "Name is required for non-customer visit.",
                )

            if cust:
                self.add_error(
                    "customer",
                    "Customer must be empty for non-customer visit.",
                )

        try:
            data["expected_sales_mt"] = _clean_decimal_field(
                data.get("expected_sales_mt"),
                allow_blank=True,
            )
        except ValidationError as exc:
            self.add_error("expected_sales_mt", exc)

        try:
            data["expected_collection"] = _clean_decimal_field(
                data.get("expected_collection"),
                allow_blank=True,
            )
        except ValidationError as exc:
            self.add_error("expected_collection", exc)

        return data

    def save(self, commit=True):
        inst: VisitPlan = super().save(commit=False)
        inst.purpose = (inst.purpose or "").strip()

        if commit:
            inst.save()

        return inst


class SingleVisitForm(forms.ModelForm):
    """
    Single Visit approval workflow form.

    Important:
      - customer_queryset is passed by views.py.
      - This prevents dropdown from using all customers or none incorrectly.
      - Manual customer mode remains supported.
      - Purpose of Visit is mandatory for Save Draft and Submit to Manager.
    """

    class Meta:
        model = VisitPlan
        fields = [
            "visit_category",
            "visit_date",
            "customer",
            "counterparty_name",
            "location",
            "purpose",
        ]
        widgets = {
            "visit_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "form-control",
                }
            ),
            "location": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Enter visit location",
                }
            ),
            "purpose": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 3,
                    "placeholder": "Enter Purpose of Visit",
                    "required": "required",
                }
            ),
            "counterparty_name": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Customer / Vendor / Supplier / Warehouse Name",
                }
            ),
        }
        labels = {
            "visit_category": "Visit Category",
            "visit_date": "Visit Date",
            "customer": "Customer Name",
            "counterparty_name": "Customer / Vendor / Supplier / Warehouse Name",
            "location": "Location",
            "purpose": "Purpose of Visit",
        }

    def __init__(self, *args, **kwargs):
        customer_queryset = kwargs.pop("customer_queryset", None)

        super().__init__(*args, **kwargs)

        self.fields["visit_category"].required = True
        self.fields["visit_date"].required = True

        # Keep False because manual customer mode disables the dropdown.
        self.fields["customer"].required = False

        self.fields["counterparty_name"].required = False
        self.fields["location"].required = True
        self.fields["purpose"].required = True
        self.fields["purpose"].label = "Purpose of Visit"

        self.fields["visit_category"].widget.attrs.update(
            {
                "class": "form-select",
            }
        )

        self.fields["customer"].widget.attrs.update(
            {
                "class": "form-select",
                "data-placeholder": "Search and select customer",
                "data-role": "legacy-single-customer",
            }
        )

        self.fields["purpose"].widget.attrs.update(
            {
                "required": "required",
                "placeholder": "Enter Purpose of Visit",
            }
        )

        if "customer" in self.fields:
            qs = customer_queryset if customer_queryset is not None else Customer.objects.none()
            self.fields["customer"].queryset = qs.order_by("name", "code")
            self.fields["customer"].empty_label = "— Select customer —"
            self.fields["customer"].label_from_instance = self.customer_label_from_instance

        if self.instance and getattr(self.instance, "pk", None):
            if self.instance.visit_category == VisitPlan.CAT_CUSTOMER and self.instance.customer_id:
                self.fields["counterparty_name"].required = False

    def customer_label_from_instance(self, obj):
        parts = []

        name = (getattr(obj, "name", "") or "").strip()
        code = (getattr(obj, "code", "") or "").strip()
        address = (getattr(obj, "address", "") or "").strip()

        if name:
            parts.append(name)

        if code:
            parts.append(code)

        if address:
            short_address = address.split(",")[0].strip()
            if short_address:
                parts.append(short_address)

        return " — ".join(parts) if parts else str(obj)

    def clean(self):
        data = super().clean()

        visit_category = data.get("visit_category")
        visit_date = data.get("visit_date")
        customer = data.get("customer")
        counterparty_name = (data.get("counterparty_name") or "").strip()
        location = (data.get("location") or "").strip()
        purpose = (data.get("purpose") or "").strip()

        manual_customer = (self.data.get("manual_customer") or "").strip()
        selected_customer_ids = []

        if hasattr(self.data, "getlist"):
            raw_selected_ids = (
                self.data.getlist("customers_selected[]")
                or self.data.getlist("customers_selected")
                or self.data.getlist(f"{self.prefix}-customers")
                or self.data.getlist("customers")
            )
        else:
            raw_selected_ids = []

        for raw_id in raw_selected_ids:
            raw_id = str(raw_id or "").strip()
            if raw_id.isdigit():
                selected_customer_ids.append(int(raw_id))

        selected_customer_ids = list(dict.fromkeys(selected_customer_ids))
        data["selected_customer_ids"] = selected_customer_ids

        if not visit_category:
            self.add_error("visit_category", "Visit Category is required.")

        if not visit_date:
            self.add_error("visit_date", "Visit Date is required.")

        if not location:
            self.add_error("location", "Location is required.")
        elif len(location) > 1000:
            self.add_error("location", "Location is too long (max 1000 characters).")
        else:
            data["location"] = location

        if not purpose:
            self.add_error("purpose", "Purpose of Visit is required.")
        elif len(purpose) > 2000:
            self.add_error("purpose", "Purpose of Visit is too long (max 2000 characters).")
        else:
            data["purpose"] = purpose

        if visit_category == VisitPlan.CAT_CUSTOMER:
            if not customer and not selected_customer_ids and not manual_customer:
                self.add_error(
                    "customer",
                    "Customer is required. Select one or more existing customers or enter a new one.",
                )

            if counterparty_name:
                self.add_error(
                    "counterparty_name",
                    "Manual name must be empty for Customer Visit. Select customers or enter a new customer instead.",
                )

        else:
            if customer or selected_customer_ids:
                self.add_error(
                    "customer",
                    "Customer must be empty for non-customer visits.",
                )

            if not counterparty_name:
                self.add_error(
                    "counterparty_name",
                    "Name is required for Vendor / Supplier / Warehouse visit.",
                )
            elif len(counterparty_name) > 255:
                self.add_error(
                    "counterparty_name",
                    "Name is too long (max 255 characters).",
                )
            else:
                data["counterparty_name"] = counterparty_name

        return data

    def save(self, commit=True):
        inst: VisitPlan = super().save(commit=False)
        inst.visit_type = VisitPlan.PLANNED

        if inst.visit_category == VisitPlan.CAT_CUSTOMER:
            inst.counterparty_name = None

            if not (inst.location or "").strip() and inst.customer and inst.customer.address:
                inst.location = inst.customer.address

        else:
            inst.customer = None
            inst.counterparty_name = (inst.counterparty_name or "").strip() or None

        inst.location = (inst.location or "").strip() or None
        inst.purpose = (inst.purpose or "").strip()

        if commit:
            inst.save()

        return inst


class VisitActualForm(forms.ModelForm):
    """
    Visit actual / post-meeting form.

    Mandatory post-meeting fields:
      - actual_datetime
      - successful / meeting outcome
      - meeting_notes = Discussion Summary / Customer Feedback
      - next_action = Required Follow-up / Next Action

    Next Meeting Date rule:
      - Hidden and cleared until manager accepts post-visit review.
      - Visible only when workflow_completed=True.
      - workflow_completed=True only when VisitPlan.approval_status == COMPLETED.
    """

    class Meta:
        model = VisitActual
        fields = [
            "actual_datetime",
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
            "actual_datetime": forms.DateTimeInput(
                attrs={
                    "type": "datetime-local",
                    "required": "required",
                    "class": "form-control",
                }
            ),
            "successful": forms.Select(
                choices=(
                    ("", "— Select outcome —"),
                    ("true", "Successful"),
                    ("false", "Not Successful"),
                ),
                attrs={
                    "class": "form-select",
                },
            ),
            "not_success_reason": forms.Select(
                attrs={
                    "class": "form-select",
                }
            ),
            "meeting_notes": forms.Textarea(
                attrs={
                    "rows": 3,
                    "placeholder": "Enter Discussion Summary / Customer Feedback / Remarks",
                    "required": "required",
                    "class": "form-control",
                }
            ),
            "actual_sales_mt": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "class": "form-control",
                    "placeholder": "Sales Opportunity",
                }
            ),
            "actual_collection": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "class": "form-control",
                    "placeholder": "Expected / Actual Collection",
                }
            ),
            "next_action": forms.Textarea(
                attrs={
                    "rows": 2,
                    "placeholder": "Enter Required Follow-up / Next Action",
                    "required": "required",
                    "class": "form-control",
                }
            ),
            "next_action_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "form-control",
                }
            ),
            "reminder_cc_manager": forms.CheckboxInput(
                attrs={
                    "class": "form-check-input",
                }
            ),
        }
        labels = {
            "actual_datetime": "Post Meeting Date & Time",
            "successful": "Meeting Outcome",
            "not_success_reason": "Not Successful Reason",
            "meeting_notes": "Discussion Summary / Customer Feedback",
            "actual_sales_mt": "Sales Opportunity",
            "actual_collection": "Expected / Actual Collection",
            "next_action": "Required Follow-up / Next Action",
            "next_action_date": "Next Meeting Date",
            "reminder_cc_manager": "CC Manager on Reminder",
        }

    def __init__(self, *args, **kwargs):
        self.workflow_completed = bool(kwargs.pop("workflow_completed", False))

        super().__init__(*args, **kwargs)

        self.fields["actual_datetime"].required = True
        self.fields["meeting_notes"].required = True
        self.fields["next_action"].required = True

        # successful may be a BooleanField. Keep backend validation in clean()
        # because unchecked/empty handling can vary by widget/model field.
        self.fields["successful"].required = False

        self.fields["meeting_notes"].label = "Discussion Summary / Customer Feedback"
        self.fields["actual_sales_mt"].label = "Sales Opportunity"
        self.fields["actual_collection"].label = "Expected / Actual Collection"
        self.fields["next_action"].label = "Required Follow-up / Next Action"

        if not self.workflow_completed:
            self.fields["next_action_date"].required = False
            self.fields["next_action_date"].widget = forms.HiddenInput()
            self.fields["next_action_date"].help_text = (
                "Next Meeting Date is available only after manager accepts post-visit review."
            )
        else:
            self.fields["next_action_date"].required = False
            self.fields["next_action_date"].widget.attrs.update(
                {
                    "type": "date",
                    "class": "form-control",
                }
            )

        if self.instance and getattr(self.instance, "pk", None):
            if not (self.instance.meeting_notes or "").strip() and (self.instance.summary or "").strip():
                self.fields["meeting_notes"].initial = self.instance.summary or ""

    def clean_successful(self):
        value = self.cleaned_data.get("successful")

        if value in (True, False):
            return value

        raw_value = self.data.get(self.add_prefix("successful"))

        if raw_value in ("true", "True", "1", "yes", "on"):
            return True

        if raw_value in ("false", "False", "0", "no", "off"):
            return False

        return value

    def clean(self):
        data = super().clean()

        actual_datetime = data.get("actual_datetime")
        successful = data.get("successful")
        reason = data.get("not_success_reason")
        meeting_notes = (data.get("meeting_notes") or "").strip()
        next_action = (data.get("next_action") or "").strip()
        next_action_date = data.get("next_action_date")

        if not actual_datetime:
            self.add_error("actual_datetime", "Post Meeting Date & Time is required.")

        if successful is None:
            self.add_error("successful", "Meeting Outcome is required.")

        if successful is False and not reason:
            self.add_error(
                "not_success_reason",
                "Please select a reason when visit is not successful.",
            )

        if successful is True:
            data["not_success_reason"] = None

        if not meeting_notes:
            self.add_error(
                "meeting_notes",
                "Discussion Summary / Customer Feedback is required.",
            )
        elif len(meeting_notes) > 5000:
            self.add_error(
                "meeting_notes",
                "Discussion Summary / Customer Feedback is too long.",
            )
        else:
            data["meeting_notes"] = meeting_notes

        if not next_action:
            self.add_error(
                "next_action",
                "Required Follow-up / Next Action is required.",
            )
        elif len(next_action) > 2000:
            self.add_error(
                "next_action",
                "Required Follow-up / Next Action is too long.",
            )
        else:
            data["next_action"] = next_action

        if not self.workflow_completed:
            data["next_action_date"] = None

        elif next_action_date and (not meeting_notes or not next_action):
            self.add_error(
                "next_action_date",
                "Next Meeting Date can be entered only after post-meeting details and follow-up are completed.",
            )

        try:
            data["actual_sales_mt"] = _clean_decimal_field(
                data.get("actual_sales_mt"),
                allow_blank=True,
            )
        except ValidationError as exc:
            self.add_error("actual_sales_mt", exc)

        try:
            data["actual_collection"] = _clean_decimal_field(
                data.get("actual_collection"),
                allow_blank=True,
            )
        except ValidationError as exc:
            self.add_error("actual_collection", exc)

        return data

    def save(self, commit=True):
        inst = super().save(commit=False)

        notes = (inst.meeting_notes or "").strip()
        action = (inst.next_action or "").strip()

        inst.meeting_notes = notes or None
        inst.summary = inst.meeting_notes
        inst.next_action = action or None

        if not self.workflow_completed:
            inst.next_action_date = None

        if not inst.meeting_notes or not inst.next_action:
            inst.next_action_date = None

        if commit:
            inst.save()

        return inst


class CallForm(forms.ModelForm):
    class Meta:
        model = CallLog
        fields = [
            "customer",
            "call_datetime",
            "duration_minutes",
            "summary",
            "outcome",
        ]
        widgets = {
            "call_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "summary": forms.Textarea(
                attrs={
                    "rows": 3,
                    "placeholder": "Call summary / remarks (optional)",
                }
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
        fields = [
            "customer",
            "txn_datetime",
            "amount",
            "mode",
            "reference",
        ]
        widgets = {
            "txn_datetime": forms.DateTimeInput(attrs={"type": "datetime-local"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["mode"].required = False
        self.fields["reference"].required = False

    def clean_amount(self):
        amount = self.cleaned_data.get("amount")
        amount = _clean_decimal_field(amount, allow_blank=False)

        if amount is not None and amount < 0:
            raise ValidationError("Amount cannot be negative.")

        return amount


class VisitBatchForm(forms.ModelForm):
    customers = forms.ModelMultipleChoiceField(
        queryset=Customer.objects.none(),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 10}),
        help_text="Select customers for Customer Visit batch.",
    )

    class Meta:
        model = VisitBatch
        fields = [
            "visit_category",
            "from_date",
            "to_date",
            "purpose",
        ]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
            "purpose": forms.Textarea(
                attrs={
                    "rows": 2,
                    "placeholder": "Enter Purpose of Visit",
                    "required": "required",
                }
            ),
        }
        labels = {
            "purpose": "Purpose of Visit",
        }

    def __init__(self, *args, **kwargs):
        customer_queryset = kwargs.pop("customer_queryset", None)

        super().__init__(*args, **kwargs)

        self.fields["purpose"].required = True
        self.fields["purpose"].label = "Purpose of Visit"
        self.fields["purpose"].widget.attrs.update(
            {
                "required": "required",
                "placeholder": "Enter Purpose of Visit",
            }
        )

        if customer_queryset is not None:
            self.fields["customers"].queryset = customer_queryset.order_by("name", "code")
        else:
            self.fields["customers"].queryset = Customer.objects.none()

    def clean(self):
        data = super().clean()

        from_date = data.get("from_date")
        to_date = data.get("to_date")
        purpose = (data.get("purpose") or "").strip()

        if not from_date:
            self.add_error("from_date", "From date is required.")

        if not to_date:
            self.add_error("to_date", "To date is required.")

        if from_date and to_date and to_date < from_date:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        if not purpose:
            self.add_error("purpose", "Purpose of Visit is required.")
        elif len(purpose) > 1000:
            self.add_error("purpose", "Purpose of Visit is too long (max 1000 characters).")
        else:
            data["purpose"] = purpose

        return data

    def save(self, commit=True):
        inst: VisitBatch = super().save(commit=False)
        inst.purpose = (inst.purpose or "").strip()

        if commit:
            inst.save()

        return inst


class MultiVisitPlanLineForm(forms.Form):
    counterparty_name = forms.CharField(
        required=True,
        max_length=255,
        label="Name",
    )

    counterparty_location = forms.CharField(
        required=False,
        max_length=255,
        label="Location",
        widget=forms.TextInput(
            attrs={
                "placeholder": "Location",
            }
        ),
    )

    counterparty_purpose = forms.CharField(
        required=True,
        max_length=500,
        label="Purpose of Visit",
        widget=forms.TextInput(
            attrs={
                "placeholder": "Enter Purpose of Visit",
                "required": "required",
            }
        ),
    )

    def clean_counterparty_name(self):
        value = (self.cleaned_data.get("counterparty_name") or "").strip()

        if not value:
            raise ValidationError("Name is required.")

        return value

    def clean_counterparty_purpose(self):
        value = (self.cleaned_data.get("counterparty_purpose") or "").strip()

        if not value:
            raise ValidationError("Purpose of Visit is required.")

        if len(value) > 500:
            raise ValidationError("Purpose of Visit is too long (max 500 characters).")

        return value


class TargetSettingForm(forms.ModelForm):
    class Meta:
        model = TargetSetting
        fields = [
            "from_date",
            "to_date",
            "sales_target_mt",
            "calls_target",
            "leads_target_mt",
            "collections_target_amount",
        ]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
        }

    def clean(self):
        data = super().clean()

        from_date = data.get("from_date")
        to_date = data.get("to_date")

        if from_date and to_date and to_date < from_date:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        return data


class ManagerTargetForm(forms.Form):
    id = forms.CharField(required=False)

    from_date = forms.DateField(
        required=True,
        widget=forms.DateInput(attrs={"type": "date"}),
    )

    to_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={"type": "date"}),
    )

    fixed_for_next_3_months = forms.BooleanField(
        required=False,
        initial=False,
    )

    kam_username = forms.ChoiceField(
        required=False,
        choices=[],
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    bulk_all_kams = forms.BooleanField(
        required=False,
        initial=False,
    )

    sales_target_mt = forms.DecimalField(
        required=True,
        max_digits=12,
        decimal_places=2,
    )

    leads_target_mt = forms.DecimalField(
        required=True,
        max_digits=12,
        decimal_places=2,
    )

    calls_target = forms.IntegerField(
        required=True,
        min_value=0,
    )

    collections_target_amount = forms.DecimalField(
        required=False,
        max_digits=14,
        decimal_places=2,
    )

    auto_collections_30pct_overdue = forms.BooleanField(
        required=False,
        initial=True,
    )

    def __init__(self, *args, **kwargs):
        self.kam_options = kwargs.pop("kam_options", []) or []

        super().__init__(*args, **kwargs)

        choices = [
            ("", "— Select KAM —"),
        ]

        choices += [
            (username, username)
            for username in self.kam_options
        ]

        self.fields["kam_username"].choices = choices

    def clean_kam_username(self):
        username = (self.cleaned_data.get("kam_username") or "").strip()

        if username and self.kam_options and username not in self.kam_options:
            raise ValidationError("Selected KAM is not in your allowed scope.")

        return username

    def clean(self):
        data = super().clean()

        from_date = data.get("from_date")
        to_date = data.get("to_date")

        if not from_date:
            self.add_error("from_date", "From date is required.")

        if to_date and from_date and to_date < from_date:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        for field, label in [
            ("sales_target_mt", "Sales target"),
            ("leads_target_mt", "Leads target"),
            ("calls_target", "Calls target"),
        ]:
            value = data.get(field)

            if value is not None and value < 0:
                self.add_error(field, f"{label} cannot be negative.")

        collections_target = data.get("collections_target_amount")

        if collections_target is not None and collections_target < 0:
            self.add_error(
                "collections_target_amount",
                "Collections target cannot be negative.",
            )

        auto = bool(data.get("auto_collections_30pct_overdue"))

        if not auto and collections_target is None:
            self.add_error(
                "collections_target_amount",
                "Enter a collections target or enable auto 30% overdue.",
            )

        return data


class CollectionPlanForm(forms.ModelForm):
    class Meta:
        model = CollectionPlan
        fields = [
            "customer",
            "planned_amount",
            "notes",
            "from_date",
            "to_date",
            "period_type",
            "period_id",
        ]
        widgets = {
            "from_date": forms.DateInput(attrs={"type": "date"}),
            "to_date": forms.DateInput(attrs={"type": "date"}),
            "period_id": forms.TextInput(
                attrs={
                    "placeholder": "e.g. 2026-W09 / 2026-02 / 2026-Q1 / 2026",
                }
            ),
            "notes": forms.Textarea(
                attrs={
                    "rows": 2,
                    "placeholder": "Remarks / notes (optional)",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["notes"].required = False
        self.fields["period_type"].required = False
        self.fields["period_id"].required = False
        self.fields["from_date"].required = False
        self.fields["to_date"].required = False

    def clean_planned_amount(self):
        amount = self.cleaned_data.get("planned_amount")
        amount = _clean_decimal_field(amount, allow_blank=False)

        if amount is not None and amount < 0:
            raise ValidationError("Planned amount cannot be negative.")

        return amount

    def clean(self):
        data = super().clean()

        from_date = data.get("from_date")
        to_date = data.get("to_date")
        period_type = data.get("period_type")
        period_id = (data.get("period_id") or "").strip()

        has_period = bool(period_type) and bool(period_id)
        has_range = bool(from_date) and bool(to_date)

        if from_date and to_date and to_date < from_date:
            self.add_error("to_date", "To date cannot be earlier than From date.")

        if period_type and not period_id:
            self.add_error("period_id", "Period ID is required when Period Type is selected.")

        if period_id and not period_type:
            self.add_error("period_type", "Period Type is required when Period ID is provided.")

        if from_date and not to_date:
            self.add_error("to_date", "To date is required when From date is provided.")

        if to_date and not from_date:
            self.add_error("from_date", "From date is required when To date is provided.")

        if not has_period and not has_range:
            raise ValidationError(
                "Provide either Period Type + Period ID, or From Date + To Date."
            )

        data["period_id"] = period_id or None

        return data


class CollectionPlanActualForm(forms.ModelForm):
    class Meta:
        model = CollectionPlan
        fields = [
            "actual_amount",
            "collection_date",
            "payment_details",
            "utr_number",
            "notes",
        ]
        widgets = {
            "collection_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "form-control",
                }
            ),
            "payment_details": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Cash / NEFT / RTGS / Cheque / UPI",
                }
            ),
            "utr_number": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "UTR / Cheque / Reference No.",
                }
            ),
            "notes": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 2,
                    "placeholder": "Remarks (optional)",
                }
            ),
        }
        labels = {
            "actual_amount": "Amount Collected (₹)",
            "collection_date": "Date of Collection",
            "payment_details": "Payment Mode / Details",
            "utr_number": "UTR / Cheque Number",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["actual_amount"].required = True
        self.fields["collection_date"].required = True
        self.fields["payment_details"].required = False
        self.fields["utr_number"].required = False
        self.fields["notes"].required = False

        self.fields["actual_amount"].widget.attrs.update(
            {
                "class": "form-control",
                "step": "0.01",
                "min": "0",
                "placeholder": "0.00",
            }
        )

    def clean_actual_amount(self):
        amount = self.cleaned_data.get("actual_amount")
        amount = _clean_decimal_field(amount, allow_blank=False)

        if amount is not None and amount < 0:
            raise ValidationError("Amount cannot be negative.")

        return amount

    def clean(self):
        data = super().clean()

        if not data.get("collection_date"):
            self.add_error("collection_date", "Collection date is required.")

        return data

    def save(self, commit=True):
        inst = super().save(commit=False)

        if inst.utr_number:
            inst.collection_reference = inst.utr_number

        if commit:
            inst.save()

        return inst


class TargetLineInlineForm(forms.Form):
    grade = forms.CharField(
        required=True,
        max_length=50,
    )

    size = forms.CharField(
        required=False,
        max_length=50,
    )

    target_mt = forms.DecimalField(
        required=True,
        max_digits=12,
        decimal_places=2,
    )


class KamManagerMappingForm(forms.Form):
    kam_username = forms.CharField(required=True)
    manager_username = forms.CharField(required=True)
    active = forms.BooleanField(required=False, initial=True)

    def clean_kam_username(self):
        username = (self.cleaned_data.get("kam_username") or "").strip()

        if not username:
            raise ValidationError("KAM username is required.")

        if not User.objects.filter(username=username, is_active=True).exists():
            raise ValidationError("KAM user not found.")

        return username

    def clean_manager_username(self):
        username = (self.cleaned_data.get("manager_username") or "").strip()

        if not username:
            raise ValidationError("Manager username is required.")

        if not User.objects.filter(username=username, is_active=True).exists():
            raise ValidationError("Manager user not found.")

        return username

    def clean(self):
        data = super().clean()

        if data.get("kam_username") and data.get("manager_username"):
            if data["kam_username"] == data["manager_username"]:
                self.add_error(
                    "manager_username",
                    "Manager cannot be the same as KAM.",
                )

        return data