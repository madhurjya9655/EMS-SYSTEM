from django import forms
from django.contrib.auth import get_user_model

User = get_user_model()

class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(queryset=User.objects.all(), required=False, label='Doer Name')
    department = forms.CharField(required=False, label='Department')
    date_from = forms.DateField(required=False, widget=forms.DateInput(attrs={'type':'date'}), label='From')
    date_to = forms.DateField(required=False, widget=forms.DateInput(attrs={'type':'date'}), label='To')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not getattr(self, 'user', None) or (self.user and not (self.user.is_staff or self.user.is_superuser)):
            self.fields['doer'].queryset = User.objects.filter(pk=self.user.pk) if self.user else User.objects.none()
