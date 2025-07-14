from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import AuthenticationForm
from .models import Profile

UserModel = get_user_model()

DEPARTMENT_CHOICES = [
    ('', 'Select One'),
    ('FINANCE', 'FINANCE'),
    ('MARKETING', 'MARKETING'),
    ('MDO TEAM', 'MDO TEAM'),
    ('SALES OPERATION TEAM', 'SALES OPERATION TEAM'),
]

MODULE_CHOICES = [
    # Leave
    ('leave_apply',           'Leave Apply'),
    ('leave_list',            'Leave List'),

    # Master Tasks
    ('tasks_list_checklist',  'List Checklist'),
    ('tasks_add_checklist',   'Add Checklist'),
    ('tasks_list_delegation', 'List Delegation'),
    ('tasks_add_delegation',  'Add Delegation'),
    ('tasks_bulk_upload',     'Bulk Upload'),

    # Help Ticket
    ('help_ticket_list',          'List All Tickets'),
    ('help_ticket_add',           'Add Ticket'),
    ('help_ticket_assigned_to',   'Assigned to Me'),
    ('help_ticket_assigned_by',   'Assigned by Me'),

    # Petty Cash
    ('pettycash_apply',       'Petty-Cash Apply'),
    ('pettycash_list',        'Petty-Cash List'),

    # Sales
    ('sales_plan_add',        'Add Sales Plan'),
    ('sales_plan_list',       'List Sales Plan'),
    ('sales_dashboard',       'Dashboard'),

    # Reimbursement
    ('reimbursement_apply',   'Reimbursement Apply'),
    ('reimbursement_list',    'Reimbursement List'),

    # Reports
    ('reports_doer_tasks',          'Doer Tasks'),
    ('reports_weekly_mis_score',    'Weekly MIS Score'),
    ('reports_performance_score',   'Performance Score'),

    # Users
    ('users_list',            'List Users'),
    ('users_add',             'Add User'),

    # Settings
    ('settings_authorized_numbers', 'Authorized Numbers'),
    ('settings_holiday_list',       'Holiday List'),
    ('settings_system_settings',    'System Settings'),
]

class CustomAuthForm(AuthenticationForm):
    error_messages = {
        'invalid_login': "Please enter a correct username and password.",
        'inactive':      "Your account is inactive; please contact the administrator.",
    }
    def clean(self):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')
        if username and password:
            try:
                user = UserModel._default_manager.get_by_natural_key(username)
            except UserModel.DoesNotExist:
                user = None
            else:
                if user.check_password(password) and not user.is_active:
                    raise forms.ValidationError(
                        self.error_messages['inactive'],
                        code='inactive',
                    )
        return super().clean()

class UserForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)

    class Meta:
        model = UserModel
        fields = ['first_name', 'last_name', 'username', 'email', 'password']

    def clean_username(self):
        username = self.cleaned_data['username']
        qs = UserModel.objects.filter(username=username)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if qs.exists():
            raise forms.ValidationError("This username is already taken.")
        return username

    def clean_email(self):
        email = self.cleaned_data['email']
        qs = UserModel.objects.filter(email=email)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if qs.exists():
            raise forms.ValidationError("This email address is already in use.")
        return email

class ProfileForm(forms.ModelForm):
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label="Department"
    )
    permissions = forms.MultipleChoiceField(
        choices=MODULE_CHOICES,
        required=False,
        widget=forms.CheckboxSelectMultiple(attrs={'class': 'form-check-input'}),
        label="Permissions"
    )

    class Meta:
        model = Profile
        fields = [
            'phone',
            'role',
            'branch',
            'department',
            'team_leader',
            'permissions'
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        instance = kwargs.get('instance', None)
        if instance and getattr(instance, 'pk', None) and getattr(instance, 'permissions', None):
            self.initial['permissions'] = instance.permissions

    def clean_phone(self):
        phone = self.cleaned_data['phone']
        qs = Profile.objects.filter(phone=phone)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if qs.exists():
            raise forms.ValidationError("This phone number is already registered.")
        return phone
