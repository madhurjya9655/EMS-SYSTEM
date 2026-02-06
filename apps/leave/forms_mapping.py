from __future__ import annotations

from typing import Optional, Iterable

from django import forms
from django.contrib.auth import get_user_model

from .models import ApproverMapping

User = get_user_model()


# ---------- pretty option labels ----------

class _UserChoice(forms.ModelChoiceField):
    def label_from_instance(self, obj):
        name = (getattr(obj, "get_full_name", lambda: "")() or obj.username or "").strip()
        email = (obj.email or "no-email").strip()
        return f"{name} ({email})"


class _UserMultiChoice(forms.ModelMultipleChoiceField):
    def label_from_instance(self, obj):
        name = (getattr(obj, "get_full_name", lambda: "")() or obj.username or "").strip()
        email = (obj.email or "no-email").strip()
        return f"{name} ({email})"


class ApproverMappingForm(forms.ModelForm):
    """
    Admin form to edit employee → approver/CC mappings.
    """

    employee = _UserChoice(queryset=User.objects.none(), required=True, disabled=True, label="Employee")
    reporting_person = _UserChoice(queryset=User.objects.none(), required=True, label="Reporting Person")
    cc_person = _UserChoice(queryset=User.objects.none(), required=False, label="Legacy CC (optional)")

    default_cc_users = _UserMultiChoice(
        queryset=User.objects.none(),
        required=False,
        label="Default CC Users (optional)",
        help_text="Users copied by default on approval emails.",
    )

    class Meta:
        model = ApproverMapping
        fields = ("employee", "reporting_person", "cc_person", "default_cc_users", "notes")
        widgets = {
            "notes": forms.Textarea(attrs={"rows": 5, "placeholder": "Optional admin notes"}),
        }

    def __init__(self, *args, employee_obj: Optional[User] = None, **kwargs):
        super().__init__(*args, **kwargs)

        base_qs = User.objects.filter(is_active=True).order_by(
            "first_name", "last_name", "username", "id"
        )
        email_qs = base_qs.exclude(email__isnull=True).exclude(email__exact="")

        self.fields["employee"].queryset = base_qs
        self.fields["reporting_person"].queryset = email_qs
        self.fields["cc_person"].queryset = email_qs
        self.fields["default_cc_users"].queryset = email_qs

        emp_from_instance = getattr(self.instance, "employee", None)
        employee_final = employee_obj or emp_from_instance
        if employee_final is not None:
            self.fields["employee"].initial = employee_final.pk

        if getattr(self.instance, "pk", None):
            self.fields["default_cc_users"].initial = list(
                self.instance.default_cc_users.values_list("pk", flat=True)
            )

    # --------------- validation ---------------
    def clean(self):
        cleaned = super().clean()

        employee: Optional[User] = None
        if getattr(self.instance, "employee_id", None):
            employee = getattr(self.instance, "employee", None)
        else:
            emp_id = self.fields["employee"].initial
            if emp_id:
                employee = User.objects.filter(pk=emp_id).first()

        rp: Optional[User] = cleaned.get("reporting_person")
        cc: Optional[User] = cleaned.get("cc_person")
        multi_cc: Iterable[User] = cleaned.get("default_cc_users") or []

        if rp is None:
            self.add_error("reporting_person", "Reporting person is required.")
        elif not (rp.email or "").strip():
            self.add_error("reporting_person", "Reporting person must have an email address.")

        if cc is not None and not (cc.email or "").strip():
            self.add_error("cc_person", "Legacy CC must have an email address.")

        bad_multi = [u for u in multi_cc if not (u.email or "").strip()]
        if bad_multi:
            self.add_error("default_cc_users", "All default CC users must have an email address.")

        if employee and rp and employee.id == rp.id:
            self.add_error("reporting_person", "Employee cannot be their own reporting person.")
        if employee and cc and employee.id == cc.id:
            self.add_error("cc_person", "Employee cannot be their own legacy CC.")
        if rp and cc and rp.id == cc.id:
            self.add_error("cc_person", "Legacy CC cannot be the same as the reporting person.")

        ids_seen = set()
        for u in multi_cc:
            if employee and u.id == employee.id:
                self.add_error("default_cc_users", "Default CC list cannot contain the employee.")
            if rp and u.id == rp.id:
                self.add_error("default_cc_users", "Default CC list cannot contain the reporting person.")
            if cc and u.id == cc.id:
                self.add_error("default_cc_users", "Default CC list cannot contain the legacy CC.")
            if u.id in ids_seen:
                self.add_error("default_cc_users", "Duplicate users in default CC list are not allowed.")
            ids_seen.add(u.id)

        return cleaned

    # --------------- save ---------------
    def save(self, commit: bool = True) -> ApproverMapping:
        instance: ApproverMapping = super().save(commit=False)

        if not getattr(instance, "employee_id", None):
            emp_id = self.fields["employee"].initial
            if emp_id:
                instance.employee = User.objects.get(pk=emp_id)

        if commit:
            instance.save()

        if "default_cc_users" in self.cleaned_data:
            instance.default_cc_users.set(self.cleaned_data.get("default_cc_users") or [])

        return instance
