from django import forms
from django.contrib.auth import get_user_model
from django.utils import timezone

User = get_user_model()

DEPARTMENT_CHOICES = [
    ('', 'All'),
    ('FINANCE', 'FINANCE'),
    ('MARKETING', 'MARKETING'),
    ('MDO TEAM', 'MDO TEAM'),
    ('SALES OPERATION TEAM', 'SALES OPERATION TEAM'),
]


def _label_user(u: User) -> str:
    full = (u.get_full_name() or "").strip()
    return full or u.username


class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),            # set in __init__
        required=False,
        empty_label='All',
        label='Doer Name',
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label='Department',
        widget=forms.Select(attrs={'class': 'form-select'})
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

        qs = User.objects.filter(is_active=True).order_by('username')
        if user:
            if user.is_staff or user.is_superuser:
                self.fields['doer'].queryset = qs
            else:
                self.fields['doer'].queryset = qs.filter(pk=user.pk)
                # optional: default to self for non-staff
                self.fields['doer'].initial = user.pk
        else:
            self.fields['doer'].queryset = qs.none()

        # pretty labels
        self.fields['doer'].label_from_instance = _label_user

    def clean(self):
        cleaned = super().clean()
        df, dt = cleaned.get('date_from'), cleaned.get('date_to')
        if df and dt and dt < df:
            self.add_error('date_to', 'End date must be on/after start date.')
        return cleaned


class WeeklyMISFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        empty_label='All',
        label='Doer Name',
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label='Department',
        widget=forms.Select(attrs={'class': 'form-select'})
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
        required=False, min_value=0, label="This Week Commitment (Checklist)",
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    delegation_commitment = forms.IntegerField(
        required=False, min_value=0, label="This Week Commitment (Delegation)",
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)

        qs = User.objects.filter(is_active=True).order_by('username')
        if user:
            if user.is_staff or user.is_superuser:
                self.fields['doer'].queryset = qs
            else:
                self.fields['doer'].queryset = qs.filter(pk=user.pk)
                self.fields['doer'].initial = user.pk
        else:
            self.fields['doer'].queryset = qs.none()

        self.fields['doer'].label_from_instance = _label_user

    def clean(self):
        cleaned = super().clean()
        df, dt = cleaned.get('date_from'), cleaned.get('date_to')
        if df and dt and dt < df:
            self.add_error('date_to', 'End date must be on/after start date.')
        return cleaned


class WeeklyMISCommitmentForm(forms.Form):
    checklist = forms.IntegerField(
        label="Checklist Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    checklist_desc = forms.CharField(
        label="Checklist Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )
    checklist_ontime = forms.IntegerField(
        label="Checklist OnTime Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    checklist_ontime_desc = forms.CharField(
        label="Checklist OnTime Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )
    delegation = forms.IntegerField(
        label="Delegation Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    delegation_desc = forms.CharField(
        label="Delegation Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )
    delegation_ontime = forms.IntegerField(
        label="Delegation OnTime Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    delegation_ontime_desc = forms.CharField(
        label="Delegation OnTime Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )
    fms = forms.IntegerField(
        label="FMS Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    fms_desc = forms.CharField(
        label="FMS Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )
    audit = forms.IntegerField(
        label="Audit Commitment (number)", min_value=0, required=False,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )
    audit_desc = forms.CharField(
        label="Audit Commitment (description)",
        widget=forms.Textarea(attrs={'rows': 2, 'class': 'form-control'}), required=False
    )

    def clean(self):
        cleaned = super().clean()
        # keep on-time <= total, when both are provided
        for base, ontime in (('checklist', 'checklist_ontime'),
                             ('delegation', 'delegation_ontime')):
            base_val = cleaned.get(base)
            on_val = cleaned.get(ontime)
            if base_val is not None and on_val is not None and on_val > base_val:
                self.add_error(ontime, 'On-time commitment cannot exceed total commitment.')
        return cleaned
