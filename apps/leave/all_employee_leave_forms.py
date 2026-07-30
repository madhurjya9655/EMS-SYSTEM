#apps\leave\all_employee_leave_forms.py
from decimal import Decimal
from django import forms
from .models import EmployeeLeaveBalance


class EmployeeLeaveBalanceAdminForm(forms.ModelForm):
    base_leave = forms.DecimalField(max_digits=6, decimal_places=1, min_value=Decimal("0.0"))
    assigned_leave = forms.DecimalField(max_digits=6, decimal_places=1, min_value=Decimal("0.0"))
    available_leave = forms.DecimalField(max_digits=6, decimal_places=1)
    effective_leave = forms.DecimalField(max_digits=6, decimal_places=1, disabled=True, required=False)
    remarks = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}))

    class Meta:
        model = EmployeeLeaveBalance
        fields = ["leave_year_start", "leave_year_end", "base_leave", "carry_forward_adjustment", "opening_adjustment", "assigned_leave", "available_leave", "remarks"]
        widgets = {"leave_year_start": forms.DateInput(attrs={"type": "date"}), "leave_year_end": forms.DateInput(attrs={"type": "date"})}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        total = Decimal(str(self.instance.total_paid_leaves or 0))
        carry = Decimal(str(self.instance.carry_forward_adjustment or 0))
        opening = Decimal(str(self.instance.opening_adjustment or 0))
        base = total - carry - opening
        self.fields["base_leave"].initial = base
        self.fields["assigned_leave"].initial = base
        self.fields["available_leave"].initial = self.instance.remaining_paid_leaves
        self.fields["effective_leave"].initial = total
        for field in self.fields.values():
            field.widget.attrs.setdefault("class", "form-control")

    def clean(self):
        cleaned = super().clean()
        start, end = cleaned.get("leave_year_start"), cleaned.get("leave_year_end")
        if start and end and end <= start:
            self.add_error("leave_year_end", "Financial year end must be after the start date.")
        assigned = cleaned.get("assigned_leave") or cleaned.get("base_leave") or Decimal("0.0")
        carry = cleaned.get("carry_forward_adjustment") or Decimal("0.0")
        opening = cleaned.get("opening_adjustment") or Decimal("0.0")
        cleaned["base_leave"] = assigned
        cleaned["effective_leave"] = assigned + carry + opening
        return cleaned
