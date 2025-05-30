from django import forms
from .models import SalesKPI

class KPIPlanForm(forms.ModelForm):
    class Meta:
        model = SalesKPI
        fields = ['metric','period_type','period_start','period_end','target']
        widgets = {
            'metric': forms.Select(attrs={'class':'form-select'}),
            'period_type': forms.Select(attrs={'class':'form-select'}),
            'period_start': forms.DateInput(attrs={'type':'date','class':'form-control'}),
            'period_end': forms.DateInput(attrs={'type':'date','class':'form-control'}),
            'target': forms.NumberInput(attrs={'class':'form-control'}),
        }

class KPIActualForm(forms.ModelForm):
    class Meta:
        model = SalesKPI
        fields = ['actual']
        widgets = {
            'actual': forms.NumberInput(attrs={'class':'form-control'}),
        }
