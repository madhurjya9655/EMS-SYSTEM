from django import forms
from django.contrib.auth import get_user_model

User = get_user_model()

class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(queryset=User.objects.all(), required=False, label='Doer Name')
    department = forms.CharField(required=False, label='Department')
    date_from = forms.DateField(required=False, widget=forms.DateInput(attrs={'type':'date'}), label='From')
    date_to = forms.DateField(required=False, widget=forms.DateInput(attrs={'type':'date'}), label='To')
