from django import forms
from django.contrib.auth.models import User
from .models import Profile

MODULE_CHOICES = [
    ('leave_apply', 'Leave Apply'),
    ('leave_list', 'Leave List'),
    ('reimbursement_apply', 'Reimbursement Apply'),
    ('reimbursement_list', 'Reimbursement List'),
    ('pettycash_apply', 'Petty-Cash Apply'),
    ('pettycash_list', 'Petty-Cash List'),
    ('sales_plan_add', 'Add Sales Plan'),
    ('sales_plan_list', 'List Sales Plan'),
    ('tasks_add_checklist', 'Add Checklist'),
    ('tasks_list_checklist', 'List Checklist'),
    ('tasks_add_delegation', 'Add Delegation'),
    ('tasks_list_delegation', 'List Delegation'),
    ('reports_doer_tasks', 'View Doer Tasks Report'),
    ('reports_performance_score', 'View Performance Score'),
    # … add more as needed …
]

class UserForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)
    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'username', 'email', 'password']

class ProfileForm(forms.ModelForm):
    permissions = forms.MultipleChoiceField(
        choices=MODULE_CHOICES,
        widget=forms.CheckboxSelectMultiple,
        required=False
    )
    class Meta:
        model = Profile
        fields = ['phone', 'role', 'branch', 'department', 'team_leader', 'permissions']
