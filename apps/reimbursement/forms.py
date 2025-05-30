from django import forms
from .models import Reimbursement

class ReimbursementForm(forms.ModelForm):
    class Meta:
        model = Reimbursement
        fields = ['amount','category','bill']
        widgets = {
            'amount': forms.NumberInput(attrs={'class':'form-control','placeholder':'Amount'}),
            'category': forms.Select(attrs={'class':'form-select'}),
            'bill': forms.FileInput(attrs={'class':'form-control'}),
        }

class ManagerReviewForm(forms.ModelForm):
    class Meta:
        model = Reimbursement
        fields = ['status','manager_comment']
        widgets = {
            'status': forms.Select(attrs={'class':'form-select'}),
            'manager_comment': forms.Textarea(attrs={'class':'form-control','rows':3}),
        }

class FinanceReviewForm(forms.ModelForm):
    class Meta:
        model = Reimbursement
        fields = ['status','finance_comment']
        widgets = {
            'status': forms.Select(attrs={'class':'form-select'}),
            'finance_comment': forms.Textarea(attrs={'class':'form-control','rows':3}),
        }
