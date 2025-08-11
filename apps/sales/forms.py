from django import forms
from .models import SalesKPI
from .google_sheets_utils import (
    get_unique_customer_names_from_sheet,
    get_unique_kam_names_from_sheet,
    get_unique_location_from_sheet,
)
from django.contrib.auth import get_user_model

User = get_user_model()

MONTH_CHOICES = [
    ('April', 'April'), ('May', 'May'), ('June', 'June'), ('July', 'July'),
    ('August', 'August'), ('September', 'September'), ('October', 'October'),
    ('November', 'November'), ('December', 'December'), ('January', 'January'),
    ('February', 'February'), ('March', 'March')
]
WEEK_CHOICES = [
    ('Week-1', 'Week-1'), ('Week-2', 'Week-2'),
    ('Week-3', 'Week-3'), ('Week-4', 'Week-4'), ('Week-5', 'Week-5')
]

class KPIPlanForm(forms.ModelForm):
    customer = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    kam = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    location = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    month = forms.ChoiceField(choices=MONTH_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))
    week = forms.ChoiceField(choices=WEEK_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))
    wire_rod = forms.DecimalField(max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    wr_actual = forms.DecimalField(max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    round_bar = forms.DecimalField(max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    total_plan = forms.DecimalField(max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))

    class Meta:
        model = SalesKPI
        fields = [
            'customer', 'month', 'week', 'location', 'kam',
            'wire_rod', 'wr_actual', 'round_bar', 'total_plan',
            'metric', 'period_type', 'period_start', 'period_end', 'target'
        ]
        widgets = {
            'metric': forms.Select(attrs={'class': 'form-select'}),
            'period_type': forms.Select(attrs={'class': 'form-select'}),
            'period_start': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'period_end': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'target': forms.NumberInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['customer'].choices = [('', '---------')] + get_unique_customer_names_from_sheet(tab_name="Sheet1")
        self.fields['kam'].choices = [('', '---------')] + get_unique_kam_names_from_sheet(tab_name="Sheet1")
        self.fields['location'].choices = [('', '---------')] + get_unique_location_from_sheet(tab_name="Sheet1")

class KPIActualForm(forms.ModelForm):
    class Meta:
        model = SalesKPI
        fields = ['actual']
        widgets = {
            'actual': forms.NumberInput(attrs={'class': 'form-control'}),
        }

class SalesDashboardFilterForm(forms.Form):
    kam = forms.ChoiceField(label="KAM Name", required=False, choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    month = forms.ChoiceField(label="Month", required=False, choices=[('', '--- All ---')] + MONTH_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))
    week = forms.ChoiceField(label="Week", required=False, choices=[('', '--- All ---')] + WEEK_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['kam'].choices = [('', '--- All ---')] + get_unique_kam_names_from_sheet(tab_name="Sheet1")

class CollectionPlanForm(forms.ModelForm):
    customer = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))

    class Meta:
        model = SalesKPI
        fields = ['customer', 'period_type', 'period_start', 'period_end', 'target']
        widgets = {
            'period_type': forms.Select(attrs={'class': 'form-select'}),
            'period_start': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'period_end': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'target': forms.NumberInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['customer'].choices = [('', '---------')] + get_unique_customer_names_from_sheet(tab_name="Sheet1")

class CollectionActualForm(forms.ModelForm):
    class Meta:
        model = SalesKPI
        fields = ['actual']
        widgets = {
            'actual': forms.NumberInput(attrs={'class': 'form-control'}),
        }

class CallPlanForm(forms.ModelForm):
    customer = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))

    class Meta:
        model = SalesKPI
        fields = ['customer', 'period_type', 'period_start', 'period_end', 'target']
        widgets = {
            'period_type': forms.Select(attrs={'class': 'form-select'}),
            'period_start': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'period_end': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'target': forms.NumberInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['customer'].choices = [('', '---------')] + get_unique_customer_names_from_sheet(tab_name="Sheet1")

class CallActualForm(forms.ModelForm):
    class Meta:
        model = SalesKPI
        fields = ['actual']
        widgets = {
            'actual': forms.NumberInput(attrs={'class': 'form-control'}),
        }

class VisitDetailsForm(forms.ModelForm):
    customer = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))

    class Meta:
        model = SalesKPI
        fields = ['customer', 'period_type', 'period_start', 'period_end', 'target']
        widgets = {
            'period_type': forms.Select(attrs={'class': 'form-select'}),
            'period_start': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'period_end': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'target': forms.NumberInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['customer'].choices = [('', '---------')] + get_unique_customer_names_from_sheet(tab_name="Sheet1")

class NBDForm(forms.ModelForm):
    customer = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))

    class Meta:
        model = SalesKPI
        fields = ['customer', 'period_type', 'period_start', 'period_end', 'target']
        widgets = {
            'period_type': forms.Select(attrs={'class': 'form-select'}),
            'period_start': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'period_end': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'target': forms.NumberInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['customer'].choices = [('', '---------')] + get_unique_customer_names_from_sheet(tab_name="Sheet1")

class SalesPlanEntryForm(forms.Form):
    month = forms.ChoiceField(label="Month", choices=MONTH_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))
    week = forms.ChoiceField(label="Week", choices=WEEK_CHOICES, widget=forms.Select(attrs={'class': 'form-select'}))
    customer_name = forms.CharField(label="Customer Name", max_length=200, widget=forms.TextInput(attrs={'class': 'form-control'}))
    location = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    kam = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class': 'form-select'}))
    wire_rod = forms.DecimalField(label="Wire Rod", max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    wr_actual = forms.DecimalField(label="WR Actual", max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    round_bar = forms.DecimalField(label="Round Bar", max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))
    total_plan = forms.DecimalField(label="Total Plan", max_digits=12, decimal_places=2, widget=forms.NumberInput(attrs={'class': 'form-control'}))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['location'].choices = [('', '---------')] + get_unique_location_from_sheet(tab_name="Sheet1")
        self.fields['kam'].choices = [('', '---------')] + get_unique_kam_names_from_sheet(tab_name="Sheet1")
