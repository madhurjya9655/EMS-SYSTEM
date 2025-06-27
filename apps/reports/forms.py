from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

User = get_user_model()

class PCReportFilterForm(forms.Form):
    doer = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        label='Doer Name',
        widget=forms.Select(attrs={'class':'form-select'})
    )
    department = forms.ChoiceField(
        choices=[],
        required=False,
        label='Department Name',
        widget=forms.Select(attrs={'class':'form-select'})
    )
    date_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type':'date','class':'form-control'}),
        label='From'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type':'date','class':'form-control'}),
        label='To'
    )

    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if user and (user.is_staff or user.is_superuser):
            self.fields['doer'].queryset = User.objects.all().order_by('username')
        else:
            self.fields['doer'].queryset = User.objects.filter(pk=user.pk) if user else User.objects.none()
        group_names = Group.objects.order_by('name').values_list('name','name')
        self.fields['department'].choices = [('', 'All')] + list(group_names)
