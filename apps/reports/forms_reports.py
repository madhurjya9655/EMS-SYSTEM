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
        queryset=User.objects.order_by('username'),
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
        widget=forms.DateInput(attrs={'type': 'date'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date'}),
        label='To'
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if user and not (user.is_staff or user.is_superuser):
            self.fields['doer'].queryset = User.objects.filter(pk=user.pk)

class WeeklyMISFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.order_by('username'),
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
        widget=forms.DateInput(attrs={'type': 'date'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date'}),
        label='To'
    )
    checklist_commitment  = forms.IntegerField(required=False, min_value=0,
                                               label="This Week Commitment (Checklist)")
    delegation_commitment = forms.IntegerField(required=False, min_value=0,
                                               label="This Week Commitment (Delegation)")

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if user and not (user.is_staff or user.is_superuser):
            self.fields['doer'].queryset = User.objects.filter(pk=user.pk)
