# apps/reports/forms.py
from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

User = get_user_model()


class PCReportFilterForm(forms.Form):
    """
    Filter form used across reports pages.
    """
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        label="Employee",
        empty_label="All Employees",
        widget=forms.Select(
            attrs={
                "class": "form-select select2-employee",
                "id": "doerFilter",
                "data-placeholder": "All Employees",
            }
        ),
    )

    department = forms.ChoiceField(
        choices=[],
        required=False,
        label="Department Name",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    date_from = forms.DateField(
        required=False,
        label="From",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
    )

    date_to = forms.DateField(
        required=False,
        label="To",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop("user", None)
        doer_queryset = kwargs.pop("doer_queryset", None)

        super().__init__(*args, **kwargs)

        if doer_queryset is not None:
            self.fields["doer"].queryset = doer_queryset
        elif user and (user.is_staff or user.is_superuser):
            self.fields["doer"].queryset = User.objects.filter(is_active=True).order_by(
                "first_name",
                "last_name",
                "username",
            )
        elif user:
            self.fields["doer"].queryset = User.objects.filter(pk=user.pk, is_active=True)
        else:
            self.fields["doer"].queryset = User.objects.none()

        group_names = Group.objects.order_by("name").values_list("name", "name")
        self.fields["department"].choices = [("", "All")] + list(group_names)

    def clean(self):
        """
        Make the form resilient:
        - If both dates exist and are inverted, swap them.
        """
        cleaned = super().clean()
        frm = cleaned.get("date_from")
        to = cleaned.get("date_to")
        if frm and to and frm > to:
            cleaned["date_from"], cleaned["date_to"] = to, frm
        return cleaned


class WeeklyMISCommitmentForm(forms.Form):
    """
    Form for entering weekly MIS commitments per user.
    Integer fields accept 0..100; descriptions are free text.
    (Views already default missing integers to 0.)
    """
    # Checklist
    checklist = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="Checklist (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    checklist_desc = forms.CharField(
        required=False,
        label="Checklist – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )
    checklist_ontime = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="Checklist On-time (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    checklist_ontime_desc = forms.CharField(
        required=False,
        label="Checklist On-time – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )

    # Delegation
    delegation = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="Delegation (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    delegation_desc = forms.CharField(
        required=False,
        label="Delegation – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )
    delegation_ontime = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="Delegation On-time (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    delegation_ontime_desc = forms.CharField(
        required=False,
        label="Delegation On-time – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )

    # FMS & Audit (optional KPIs)
    fms = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="FMS (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    fms_desc = forms.CharField(
        required=False,
        label="FMS – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )
    audit = forms.IntegerField(
        required=False, min_value=0, max_value=100,
        label="Audit (target %)",
        widget=forms.NumberInput(attrs={"class": "form-control"})
    )
    audit_desc = forms.CharField(
        required=False,
        label="Audit – Description",
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 2})
    )