from django import forms
from .models import LeaveRequest

class LeaveRequestForm(forms.ModelForm):
    class Meta:
        model = LeaveRequest
        fields = ['leave_type','start_date','end_date','reason']
        widgets = {
            'leave_type': forms.Select(attrs={'class':'form-select'}),
            'start_date': forms.DateInput(attrs={'type':'date','class':'form-control'}),
            'end_date': forms.DateInput(attrs={'type':'date','class':'form-control'}),
            'reason': forms.Textarea(attrs={'class':'form-control','rows':3}),
        }
