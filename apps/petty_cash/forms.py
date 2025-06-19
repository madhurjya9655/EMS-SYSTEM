from django import forms
from .models import PettyCashRequest

class PettyCashForm(forms.ModelForm):
    class Meta:
        model  = PettyCashRequest
        fields = ['reason', 'amount', 'urgency']
        widgets = {
            'reason':  forms.Textarea(attrs={'rows': 3}),
            'amount':  forms.NumberInput(),
            'urgency': forms.Select(),
        }
