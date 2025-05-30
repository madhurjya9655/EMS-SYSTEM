from django import forms
from .models import PettyCashRequest

class PettyCashRequestForm(forms.ModelForm):
    class Meta:
        model = PettyCashRequest
        fields = ['reason','amount','urgency']
