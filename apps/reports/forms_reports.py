from django import forms
from django.contrib.auth import get_user_model

User = get_user_model()

DEPARTMENT_CHOICES = [
    ('', 'All'),
    ('FINANCE', 'FINANCE'),
    ('MARKETING', 'MARKETING'),
    ('MDO TEAM', 'MDO TEAM'),
    ('SALES OPERATION TEAM', 'SALES OPERATION TEAM'),
]

class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),  # Set dynamically in __init__
        required=False,
        empty_label='All',
        label='Doer Name'
    )
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label='Department'
    )
    date_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label='To'
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if user:
            if user.is_staff or user.is_superuser:
                self.fields['doer'].queryset = User.objects.order_by('username')
            else:
                self.fields['doer'].queryset = User.objects.filter(pk=user.pk)
        else:
            self.fields['doer'].queryset = User.objects.none()

class WeeklyMISFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        empty_label='All',
        label='Doer Name'
    )
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label='Department'
    )
    date_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label='To'
    )
    checklist_commitment = forms.IntegerField(
        required=False, min_value=0, label="This Week Commitment (Checklist)"
    )
    delegation_commitment = forms.IntegerField(
        required=False, min_value=0, label="This Week Commitment (Delegation)"
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if user:
            if user.is_staff or user.is_superuser:
                self.fields['doer'].queryset = User.objects.order_by('username')
            else:
                self.fields['doer'].queryset = User.objects.filter(pk=user.pk)
        else:
            self.fields['doer'].queryset = User.objects.none()

class WeeklyMISCommitmentForm(forms.Form):
    checklist = forms.IntegerField(label="Checklist Commitment (number)", min_value=0, required=False)
    checklist_desc = forms.CharField(label="Checklist Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
    checklist_ontime = forms.IntegerField(label="Checklist OnTime Commitment (number)", min_value=0, required=False)
    checklist_ontime_desc = forms.CharField(label="Checklist OnTime Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
    delegation = forms.IntegerField(label="Delegation Commitment (number)", min_value=0, required=False)
    delegation_desc = forms.CharField(label="Delegation Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
    delegation_ontime = forms.IntegerField(label="Delegation OnTime Commitment (number)", min_value=0, required=False)
    delegation_ontime_desc = forms.CharField(label="Delegation OnTime Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
    fms = forms.IntegerField(label="FMS Commitment (number)", min_value=0, required=False)
    fms_desc = forms.CharField(label="FMS Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
    audit = forms.IntegerField(label="Audit Commitment (number)", min_value=0, required=False)
    audit_desc = forms.CharField(label="Audit Commitment (description)", widget=forms.Textarea(attrs={'rows':2}), required=False)
