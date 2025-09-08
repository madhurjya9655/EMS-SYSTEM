# apps/leave/forms_mapping.py
from __future__ import annotations

from typing import Optional

from django import forms
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from .models import ApproverMapping

User = get_user_model()


class _UserChoice(forms.ModelChoiceField):
    def label_from_instance(self, obj):
        name = (getattr(obj, "get_full_name", lambda: "")() or obj.username or "").strip()
        email = (obj.email or "no-email").strip()
        return f"{name} ({email})"


class ApproverMappingForm(forms.ModelForm):
    """
    Admin form to edit the employee → (reporting_person, cc_person) mapping.

    Notes:
    - `employee` is shown read-only (disabled). Set via `employee_obj` or instance.
    - `reporting_person` is required and must have an email.
    - `cc_person` is optional; if provided, must have an email.
    - Prevents selecting the same user as both RP and CC or mapping employee to themselves.
    """

    employee = _UserChoice(queryset=User.objects.none(), required=True, disabled=True, label="Employee")
    reporting_person = _UserChoice(queryset=User.objects.none(), required=True, label="Reporting Person")
    cc_person = _UserChoice(queryset=User.objects.none(), required=False, label="CC Person (optional)")

    class Meta:
        model = ApproverMapping
        fields = ("employee", "reporting_person", "cc_person", "notes")
        widgets = {
            "notes": forms.Textarea(attrs={"rows": 5, "placeholder": "Optional notes for admins"}),
        }

    def __init__(self, *args, employee_obj: Optional[User] = None, **kwargs):
        """
        Pass the employee being edited via `employee_obj` for create flows,
        or rely on `instance.employee` for edit flows.
        """
        super().__init__(*args, **kwargs)

        # Choices: all active users (names/emails shown in labels)
        qs = User.objects.filter(is_active=True).order_by("first_name", "last_name", "username", "id")
        self.fields["employee"].queryset = qs
        self.fields["reporting_person"].queryset = qs
        self.fields["cc_person"].queryset = qs

        # Show employee in the disabled field (from instance or explicit arg)
        emp_from_instance = getattr(self.instance, "employee", None)
        employee_final = employee_obj or emp_from_instance
        if employee_final is not None:
            # Ensure the disabled field displays a value
            self.fields["employee"].initial = employee_final.pk

    # ---------------------------
    # Validation
    # ---------------------------
    def clean(self):
        cleaned = super().clean()

        employee: Optional[User] = None
        # Prefer instance.employee; fall back to initial (disabled field)
        if getattr(self.instance, "employee_id", None):
            employee = getattr(self.instance, "employee", None)
        else:
            try:
                emp_id = self.fields["employee"].initial
                if emp_id:
                    employee = User.objects.filter(pk=emp_id).first()
            except Exception:
                employee = None

        rp: Optional[User] = cleaned.get("reporting_person")
        cc: Optional[User] = cleaned.get("cc_person")

        # Required RP
        if rp is None:
            self.add_error("reporting_person", "Reporting person is required.")
        else:
            if not (rp.email or "").strip():
                self.add_error("reporting_person", "Reporting person must have an email address.")

        # Optional CC
        if cc is not None and not (cc.email or "").strip():
            self.add_error("cc_person", "CC person must have an email address.")

        # Uniqueness / sanity checks
        if employee and rp and employee.id == rp.id:
            self.add_error("reporting_person", "Employee cannot be their own reporting person.")
        if employee and cc and employee.id == cc.id:
            self.add_error("cc_person", "Employee cannot be their own CC person.")
        if rp and cc and rp.id == cc.id:
            self.add_error("cc_person", "CC person cannot be the same as the reporting person.")

        return cleaned

    # ---------------------------
    # Save
    # ---------------------------
    def save(self, commit: bool = True) -> ApproverMapping:
        """
        Ensure `instance.employee` is set (for create flows where the field is disabled)
        and then save.
        """
        instance: ApproverMapping = super().save(commit=False)

        # If the mapping instance doesn't have an employee yet, pull it from the field's initial
        if not getattr(instance, "employee_id", None):
            try:
                emp_id = self.fields["employee"].initial
                if emp_id:
                    instance.employee = User.objects.get(pk=emp_id)
            except Exception:
                # Let the DB-level constraints complain if truly missing
                pass

        if commit:
            instance.save()
        return instance
