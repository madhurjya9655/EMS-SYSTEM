# apps/users/forms.py
from __future__ import annotations

from typing import List, Tuple, Set

from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import AuthenticationForm
from django.core.exceptions import ValidationError

from .models import Profile, normalize_phone
from .permissions import PERMISSIONS_STRUCTURE

User = get_user_model()

# Keep department choices aligned with your template <select>
DEPARTMENT_CHOICES: List[Tuple[str, str]] = [
    ("", "Select One"),
    ("FINANCE", "FINANCE"),
    ("MARKETING", "MARKETING"),
    ("MDO TEAM", "MDO TEAM"),
    ("SALES OPERATION TEAM", "SALES OPERATION TEAM"),
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _permission_choices() -> List[Tuple[str, str]]:
    """
    Flatten PERMISSIONS_STRUCTURE into (code, "Group – Label") tuples.
    """
    out: List[Tuple[str, str]] = []
    for group, items in PERMISSIONS_STRUCTURE.items():
        for code, label in items:
            out.append((code, f"{group} – {label}"))
    return out


# ---------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------
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
                user = User._default_manager.get_by_natural_key(username)
            except User.DoesNotExist:
                user = None
            else:
                if user.check_password(password) and not user.is_active:
                    raise forms.ValidationError(self.error_messages["inactive"], code="inactive")
        return super().clean()


# ---------------------------------------------------------------------
# User create/update
# ---------------------------------------------------------------------
class UserForm(forms.ModelForm):
    """
    - On create: password required.
    - On edit: password optional; if left blank, keep existing password.

    IMPORTANT: We DO NOT include 'password' in Meta.fields so that
    ModelForm does not assign it to instance.password during save().
    We handle password only in the view via set_password().
    """
    password = forms.CharField(
        label="Password",
        widget=forms.PasswordInput(render_value=False),
        required=False,  # enforced in clean_password()
        help_text="Leave blank to keep current",
    )

    class Meta:
        model = User
        fields = ["first_name", "last_name", "username", "email"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        is_edit = bool(getattr(self.instance, "pk", None))
        self.fields["password"].required = not is_edit
        if is_edit:
            self.fields["password"].widget.attrs["placeholder"] = "Leave blank to keep current"

        # Accessibility hints
        self.fields["first_name"].widget.attrs.update({"autocomplete": "given-name", "class": "form-control"})
        self.fields["last_name"].widget.attrs.update({"autocomplete": "family-name", "class": "form-control"})
        self.fields["username"].widget.attrs.update({"autocomplete": "username", "class": "form-control"})
        self.fields["email"].widget.attrs.update({"autocomplete": "email", "class": "form-control"})

    def clean_username(self) -> str:
        username = (self.cleaned_data.get("username") or "").strip()
        if not username:
            raise ValidationError("Username is required.")
        qs = User.objects.exclude(pk=getattr(self.instance, "pk", None)).filter(username__iexact=username)
        if qs.exists():
            raise ValidationError("This username is already taken.")
        return username

    def clean_email(self) -> str:
        email = (self.cleaned_data.get("email") or "").strip()
        if not email:
            raise ValidationError("Email is required.")
        qs = User.objects.exclude(pk=getattr(self.instance, "pk", None)).filter(email__iexact=email)
        if qs.exists():
            raise ValidationError("This email address is already in use.")
        return email

    def clean_password(self):
        pwd = (self.cleaned_data.get("password") or "").strip()
        if getattr(self.instance, "pk", None):
            # Editing: blank allowed → return None so view can skip set_password()
            return pwd or None
        if not pwd:
            raise ValidationError("Please set an initial password.")
        return pwd


# ---------------------------------------------------------------------
# Profile create/update (drives your permission grid)
# ---------------------------------------------------------------------
class ProfileForm(forms.ModelForm):
    """
    We declare 'permissions' explicitly as a MultipleChoiceField so the
    manually-rendered checkboxes (name='permissions') bind & validate.
    """
    department = forms.ChoiceField(
        choices=DEPARTMENT_CHOICES,
        required=False,
        label="Department",
    )

    permissions = forms.MultipleChoiceField(
        required=False,
        choices=_permission_choices(),
        widget=forms.CheckboxSelectMultiple,
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
            # Optional fields available on model; they can be added to the form later if you render them
            "photo",
            "employee_id",
            "manager_override_email",
            "cc_override_emails",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Styling / UX
        self.fields["role"].widget.attrs.setdefault("class", "form-select")
        self.fields["team_leader"].widget.attrs.setdefault("class", "form-select")
        self.fields["phone"].widget.attrs.update({"class": "form-control", "inputmode": "numeric", "autocomplete": "tel"})
        self.fields["branch"].widget.attrs.setdefault("class", "form-control")

        # Team leader: active users ordered by name
        if hasattr(self.fields["team_leader"], "queryset"):
            self.fields["team_leader"].queryset = (
                self.fields["team_leader"].queryset.order_by("first_name", "last_name", "username")
            )

        # Pre-populate MultiChoice from instance JSON when editing
        inst_perms = []
        if self.instance and getattr(self.instance, "permissions", None):
            try:
                inst_perms = [str(c) for c in (self.instance.permissions or [])]
            except Exception:
                inst_perms = []
        if inst_perms:
            self.initial["permissions"] = inst_perms

    # ---- field validation ----
    def clean_phone(self) -> str | None:
        """
        Normalize phone to digits-only; allow blank.
        Enforce 10–13 digits when provided (to allow country codes).
        Also enforce uniqueness if provided.
        """
        phone = normalize_phone(self.cleaned_data.get("phone"))
        if phone and not (10 <= len(phone) <= 13):
            raise ValidationError("Enter a valid phone number (10–13 digits).")
        if phone:
            qs = Profile.objects.filter(phone=phone)
            if getattr(self.instance, "pk", None):
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise ValidationError("This phone number is already registered.")
        return phone

    def clean_permissions(self) -> List[str]:
        """
        Ensure only known codes are saved; store as a sorted list for stability.
        """
        selected = self.cleaned_data.get("permissions") or []
        valid: Set[str] = {code for grp in PERMISSIONS_STRUCTURE.values() for code, _ in grp}
        cleaned = sorted({c for c in (str(x).strip() for x in selected) if c and c in valid})
        return cleaned

    def save(self, commit: bool = True) -> Profile:
        instance: Profile = super().save(commit=False)
        # Persist the cleaned list (JSONField)
        instance.permissions = self.cleaned_data.get("permissions") or []
        if commit:
            instance.save()
            self._maybe_mark_user_staff(instance)
        return instance

    def _maybe_mark_user_staff(self, instance: Profile) -> None:
        """
        Convenience: if role is Admin, ensure user.is_staff so they can access Django admin.
        """
        try:
            user = getattr(instance, "user", None)
            if instance.role == "Admin" and user and not user.is_staff:
                user.is_staff = True
                user.save(update_fields=["is_staff"])
        except Exception:
            # Never fail a save() for this convenience toggle
            pass
