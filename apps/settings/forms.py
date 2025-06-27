from django import forms
from .models import AuthorizedNumber, Holiday

class AuthorizedNumberForm(forms.ModelForm):
    class Meta:
        model = AuthorizedNumber
        fields = ['label', 'number']
        widgets = {
            'label': forms.TextInput(attrs={'class':'form-control'}),
            'number': forms.TextInput(attrs={'class':'form-control'}),
        }


class HolidayForm(forms.ModelForm):
    class Meta:
        model = Holiday
        fields = ['date', 'name']
        widgets = {
            'date': forms.DateInput(attrs={'type':'date','class':'form-control'}),
            'name': forms.TextInput(attrs={'class':'form-control'}),
        }


class HolidayUploadForm(forms.Form):
    file = forms.FileField(
        label="Upload Excel/CSV",
        help_text="First column: date (YYYY-MM-DD), second: holiday name",
        widget=forms.ClearableFileInput(attrs={'class':'form-control'})
    )
