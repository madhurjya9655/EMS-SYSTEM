from django import forms
from django.contrib.auth import get_user_model
from apps.recruitment.models import Employee

User = get_user_model()

class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.all(),
        required=False,
        label='Doer Name'
    )
    department = forms.ChoiceField(
        required=False,
        label='Department',
        choices=[],
    )
    date_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type':'date'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type':'date'}),
        label='To'
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)

        # Populate department dropdown from Employee.department
        depts = Employee.objects.values_list('department', flat=True).distinct()
        choices = [('', '---------')] + [(d, d) for d in depts]
        self.fields['department'].choices = choices

        # Restrict doer to self for non-admins
        if user and not (user.is_staff or user.is_superuser):
            self.fields['doer'].queryset = User.objects.filter(pk=user.pk)
        self.user = user

class WeeklyMISFilterForm(forms.Form):
    doer = forms.ModelChoiceField(queryset=User.objects.all(), required=False, label="Doer")
    department = forms.ChoiceField(choices=[], required=False, label="Department")
    date_from  = forms.DateField(widget=forms.DateInput(attrs={'type':'date'}), required=False)
    date_to    = forms.DateField(widget=forms.DateInput(attrs={'type':'date'}), required=False)

    # For entering “This Week Commitment”
    checklist_commitment  = forms.IntegerField(required=False, min_value=0, label="This Week Commitment (Checklist)")
    delegation_commitment = forms.IntegerField(required=False, min_value=0, label="This Week Commitment (Delegation)")
    fms_commitment        = forms.IntegerField(required=False, min_value=0, label="This Week Commitment (FMS)")
    audit_commitment      = forms.IntegerField(required=False, min_value=0, label="This Week Commitment (Audit)")

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        # department list from Employee
        depts = Employee.objects.values_list('department', flat=True).distinct()
        self.fields['department'].choices = [('', '---------')] + [(d,d) for d in depts]
        if user and not (user.is_staff or user.is_superuser):
            self.fields['doer'].queryset = User.objects.filter(pk=user.pk)
        self.user = user
