from __future__ import annotations

from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import AuthenticationForm

from .models import Profile
from .permissions import PERMISSIONS_STRUCTURE

UserModel = get_user_model()

DEPARTMENT_CHOICES = [
    ('', 'Select One'),
    ('FINANCE', 'FINANCE'),
    ('MARKETING', 'MARKETING'),
    ('MDO TEAM', 'MDO TEAM'),
    ('SALES OPERATION TEAM', 'SALES OPERATION TEAM'),
]


def get_permission_choices():
    # Preserve the order defined in PERMISSIONS_STRUCTURE
    choices = []
    for perms in PERMISSIONS_STRUCTURE.values():
        choices.extend(perms)
    return choices


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
    """
    - On create: password required.
    - On edit: password optional; if left blank, keep existing password.
    """
    password = forms.CharField(widget=forms.PasswordInput, required=True)

    class Meta:
        model = UserModel
        fields = ['first_name', 'last_name', 'username', 'email', 'password']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Make password optional on edit (instance exists)
        if self.instance and self.instance.pk:
            self.fields['password'].required = False
            # Help text to clarify behavior
            self.fields['password'].help_text = "Leave empty to keep the current password."

        # Basic Bootstrap classes (if you want consistent styling without widget_tweaks)
        for name, field in self.fields.items():
            if not isinstance(field.widget, (forms.CheckboxInput, forms.RadioSelect, forms.FileInput)):
                field.widget.attrs.setdefault('class', 'form-control')

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
        if email:
            qs = UserModel.objects.filter(email=email)
            if self.instance and self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise forms.ValidationError("This email address is already in use.")
        return email

    def clean_password(self):
        """
        Allow blank password on edit.
        """
        pwd = self.cleaned_data.get('password', '')
        if self.instance and self.instance.pk:
            # Optional on edit
            return pwd or ''
        # Required on create
        if not pwd:
            raise forms.ValidationError("Please set an initial password.")
        return pwd


class ProfileForm(forms.ModelForm):
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label="Department"
    )
    permissions = forms.MultipleChoiceField(
        choices=get_permission_choices(),
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

        # Make selects/inputs pretty
        self.fields['role'].widget.attrs.setdefault('class', 'form-select')
        self.fields['team_leader'].widget.attrs.setdefault('class', 'form-select')
        for name in ('phone', 'branch'):
            self.fields[name].widget.attrs.setdefault('class', 'form-control')

        # Narrow team_leader choices to active users, ordered nicely
        if hasattr(self.fields['team_leader'], 'queryset'):
            self.fields['team_leader'].queryset = (
                self.fields['team_leader'].queryset.filter(is_active=True).order_by('first_name', 'last_name', 'username')
            )

        # Pre-populate permissions when editing
        instance = kwargs.get('instance', None)
        if instance and getattr(instance, 'pk', None) and getattr(instance, 'permissions', None):
            self.initial['permissions'] = instance.permissions

    def clean_phone(self):
        phone = (self.cleaned_data.get('phone') or '').strip()
        qs = Profile.objects.filter(phone=phone)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if phone and qs.exists():
            raise forms.ValidationError("This phone number is already registered.")
        if not phone:
            raise forms.ValidationError("Phone is required.")
        # Basic length check (you can expand this to full validation if needed)
        if not phone.isdigit() or len(phone) != 10:
            raise forms.ValidationError("Enter a valid 10-digit phone number.")
        return phone
