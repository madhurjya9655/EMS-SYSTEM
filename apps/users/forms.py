from __future__ import annotations

from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import AuthenticationForm

from .models import Profile
from .permissions import PERMISSIONS_STRUCTURE

UserModel = get_user_model()

DEPARTMENT_CHOICES = [
    ("", "Select One"),
    ("FINANCE", "FINANCE"),
    ("MARKETING", "MARKETING"),
    ("MDO TEAM", "MDO TEAM"),
    ("SALES OPERATION TEAM", "SALES OPERATION TEAM"),
]


def get_permission_choices():
    """
    Flatten PERMISSIONS_STRUCTURE into a single list of (code, label) tuples,
    preserving the order defined in PERMISSIONS_STRUCTURE.
    """
    choices = []
    for perms in PERMISSIONS_STRUCTURE.values():
        choices.extend(perms)
    return choices


class CustomAuthForm(AuthenticationForm):
    error_messages = {
        "invalid_login": "Please enter a correct username and password.",
        "inactive": "Your account is inactive; please contact the administrator.",
    }

    def clean(self):
        username = self.cleaned_data.get("username")
        password = self.cleaned_data.get("password")
        if username and password:
            try:
                user = UserModel._default_manager.get_by_natural_key(username)
            except UserModel.DoesNotExist:
                user = None
            else:
                if user.check_password(password) and not user.is_active:
                    raise forms.ValidationError(
                        self.error_messages["inactive"],
                        code="inactive",
                    )
        return super().clean()


class UserForm(forms.ModelForm):
    """
    - On create: password required.
    - On edit: password optional; if left blank, keep existing password.

    IMPORTANT: We DO NOT include 'password' in Meta.fields so that
    ModelForm does not assign it to instance.password during save().
    We handle password only in the view via set_password().
    """
    password = forms.CharField(
        widget=forms.PasswordInput,
        required=False,  # required on create enforced in clean()
        help_text="Leave blank to keep the current password.",
    )

    class Meta:
        model = UserModel
        # NOTE: 'password' is intentionally omitted here
        fields = ["first_name", "last_name", "username", "email"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # On create, show password as required; on edit, optional
        if not (self.instance and self.instance.pk):
            self.fields["password"].required = True
            self.fields["password"].help_text = "Set an initial password."

        # Bootstrap classes
        for name, field in self.fields.items():
            if not isinstance(
                field.widget,
                (forms.CheckboxInput, forms.RadioSelect, forms.FileInput),
            ):
                field.widget.attrs.setdefault("class", "form-control")

    def clean_username(self):
        username = self.cleaned_data["username"]
        qs = UserModel.objects.filter(username=username)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if qs.exists():
            raise forms.ValidationError("This username is already taken.")
        return username

    def clean_email(self):
        email = self.cleaned_data["email"]
        if email:
            qs = UserModel.objects.filter(email=email)
            if self.instance and self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise forms.ValidationError("This email address is already in use.")
        return email

    def clean_password(self):
        """
        Required on create, optional on edit. Return None when left blank on edit
        so the view can easily check `if pwd: set_password(pwd)`.
        """
        pwd = (self.cleaned_data.get("password") or "").strip()
        if self.instance and self.instance.pk:
            # Editing: blank is allowed
            return pwd or None
        # Creating: must provide a password
        if not pwd:
            raise forms.ValidationError("Please set an initial password.")
        return pwd


class ProfileForm(forms.ModelForm):
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label="Department",
    )
    permissions = forms.MultipleChoiceField(
        choices=get_permission_choices(),
        required=False,
        widget=forms.CheckboxSelectMultiple(attrs={"class": "form-check-input"}),
        label="Permissions",
    )

    class Meta:
        model = Profile
        fields = [
            "phone",
            "role",
            "branch",
            "department",
            "team_leader",
            "permissions",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Make selects/inputs pretty
        self.fields["role"].widget.attrs.setdefault("class", "form-select")
        self.fields["team_leader"].widget.attrs.setdefault("class", "form-select")
        for name in ("phone", "branch"):
            self.fields[name].widget.attrs.setdefault("class", "form-control")

        # Team leader choices to active users ordered
        if hasattr(self.fields["team_leader"], "queryset"):
            self.fields["team_leader"].queryset = (
                self.fields["team_leader"]
                .queryset.filter(is_active=True)
                .order_by("first_name", "last_name", "username")
            )

        # Pre-populate permissions from instance
        instance = getattr(self, "instance", None)
        if instance and getattr(instance, "pk", None) and getattr(instance, "permissions", None):
            self.initial["permissions"] = instance.permissions

    def clean_phone(self):
        phone = (self.cleaned_data.get("phone") or "").strip()
        qs = Profile.objects.filter(phone=phone)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if phone and qs.exists():
            raise forms.ValidationError("This phone number is already registered.")
        if not phone:
            raise forms.ValidationError("Phone is required.")
        if not phone.isdigit() or len(phone) != 10:
            raise forms.ValidationError("Enter a valid 10-digit phone number.")
        return phone

    def save(self, commit: bool = True) -> Profile:
        instance: Profile = super().save(commit=False)
        instance.permissions = self.cleaned_data.get("permissions") or []
        if commit:
            instance.save()
            self._maybe_mark_user_staff(instance)
        return instance

    def _maybe_mark_user_staff(self, instance: Profile) -> None:
        try:
            if instance.role == "Admin" and instance.user and not instance.user.is_staff:
                instance.user.is_staff = True
                instance.user.save(update_fields=["is_staff"])
        except Exception:
            pass
