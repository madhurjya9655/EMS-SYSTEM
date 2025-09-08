# apps/users/admin.py
from __future__ import annotations

from typing import List, Tuple

from django.contrib import admin
from django.contrib.auth import get_user_model
from django.utils.html import format_html

from .models import Profile

User = get_user_model()


def _field_names(model) -> set[str]:
    try:
        return {f.name for f in model._meta.get_fields() if hasattr(f, "name")}
    except Exception:
        return set()


class SuperuserOnlyOverridesMixin:
    """
    Make manager/CC override fields editable only by superusers.
    For normal staff, these fields are read-only in Django Admin.
    """
    override_fields = ("manager_override_email", "cc_override_emails")

    def get_readonly_fields(self, request, obj=None) -> Tuple[str, ...]:
        base = getattr(super(), "get_readonly_fields", lambda *a, **k: [])(request, obj) or []
        ro: List[str] = list(base)
        names = _field_names(self.model)

        if not request.user.is_superuser:
            for f in self.override_fields:
                if f in names and f not in ro:
                    ro.append(f)

        # If editing existing, keep 'user' immutable to avoid reassignment accidents.
        if obj and "user" in names and "user" not in ro:
            ro.append("user")
        return tuple(ro)


@admin.register(Profile)
class ProfileAdmin(SuperuserOnlyOverridesMixin, admin.ModelAdmin):
    """
    Admin for employee profiles with safe handling of optional fields.

    EXACT behavior:
      • If Profile.role == "Admin", set user.is_staff = True on save.
      • Never auto-unset is_staff if role changes away from Admin.
    """

    list_display = (
        "user_link",
        "role_display",
        "employee_id_display",
        "designation_display",
        "department_display",
        "team_leader_display",
        "resolved_manager_email",
        "resolved_cc_preview",
        "is_staff_badge",
    )
    list_select_related = ("user", "team_leader")
    search_fields = (
        "user__username",
        "user__first_name",
        "user__last_name",
        "user__email",
        "employee_id",
        "manager_override_email",
        "cc_override_emails",
    )
    list_filter = ("role", "team_leader")
    ordering = ("user__date_joined", "user__id")

    fieldsets = (
        ("User", {"fields": ("user", "employee_id", "role")}),
        (
            "Org",
            {
                "fields": tuple(
                    f for f in ("designation", "department", "team_leader") if f in _field_names(Profile)
                )
            },
        ),
        (
            "Contact",
            {"fields": tuple(f for f in ("phone", "photo") if f in _field_names(Profile))},
        ),
        (
            "Routing Overrides (Admin only)",
            {
                "fields": tuple(
                    f for f in ("manager_override_email", "cc_override_emails") if f in _field_names(Profile)
                ),
                "description": "These override the JSON routing file and team leader mapping. Editable only by superusers.",
            },
        ),
        ("Permissions (app-level)", {"fields": ("permissions",)}),
    )

    def get_fields(self, request, obj=None):
        # Fall back to all concrete fields if fieldsets strip everything (e.g., missing optional fields)
        fields = [f for fs in self.fieldsets for f in fs[1].get("fields", [])]
        return fields or [f.name for f in Profile._meta.fields]

    # ---------- list_display helpers ----------

    @admin.display(description="User", ordering="user__username")
    def user_link(self, obj: Profile) -> str:
        name = (obj.user.get_full_name() or obj.user.username or "").strip()
        email = (obj.user.email or "").strip()
        return format_html('<strong>{}</strong><br><span style="color:#6b7280">{}</span>', name, email or "—")

    @admin.display(description="Role", ordering="role")
    def role_display(self, obj: Profile) -> str:
        return obj.role or "—"

    @admin.display(description="Employee ID", ordering="employee_id")
    def employee_id_display(self, obj: Profile) -> str:
        return getattr(obj, "employee_id", "") or "—"

    @admin.display(description="Designation")
    def designation_display(self, obj: Profile) -> str:
        return getattr(obj, "designation", "") or "—"

    @admin.display(description="Department")
    def department_display(self, obj: Profile) -> str:
        return getattr(obj, "department", "") or "—"

    @admin.display(description="Team Leader", ordering="team_leader__username")
    def team_leader_display(self, obj: Profile) -> str:
        tl = getattr(obj, "team_leader", None)
        if not tl:
            return "—"
        return f"{tl.get_full_name() or tl.username} ({tl.email or 'no-email'})"

    @admin.display(description="Resolved Manager Email")
    def resolved_manager_email(self, obj: Profile) -> str:
        try:
            # If your Profile implements resolve_manager_and_cc(), prefer that.
            if hasattr(obj, "resolve_manager_and_cc"):
                mgr, _ = obj.resolve_manager_and_cc()
                return mgr or "—"
        except Exception:
            pass
        # Fallbacks
        v = getattr(obj, "manager_email", None)
        if v:
            return v
        mo = getattr(obj, "manager_override_email", "") or ""
        return mo or "—"

    @admin.display(description="Resolved CC (preview)")
    def resolved_cc_preview(self, obj: Profile) -> str:
        try:
            if hasattr(obj, "resolve_manager_and_cc"):
                _mgr, cc = obj.resolve_manager_and_cc()
                if not cc:
                    return "—"
                if isinstance(cc, (list, tuple)):
                    cc_list = list(dict.fromkeys([c for c in cc if c]))  # dedupe
                else:
                    cc_list = [c.strip() for c in str(cc).split(",") if c.strip()]
                preview = ", ".join(cc_list[:3])
                more = max(0, len(cc_list) - 3)
                return preview + (f" (+{more})" if more else "")
        except Exception:
            pass
        raw = (getattr(obj, "cc_override_emails", "") or "").strip()
        return raw or "—"

    @admin.display(description="Staff?")
    def is_staff_badge(self, obj: Profile) -> str:
        if getattr(obj.user, "is_staff", False):
            return format_html("<span style='color:#16a34a;font-weight:600'>Yes</span>")
        return format_html("<span style='color:#64748b'>No</span>")

    # ---------- save hook: enforce Admin role → staff ----------

    def save_model(self, request, obj: Profile, form, change):
        """
        EXACT rule:
          • If role == 'Admin' → ensure user.is_staff = True
          • Never auto-unset is_staff when changing away from Admin.
        """
        super().save_model(request, obj, form, change)
        try:
            if obj.role == "Admin" and obj.user and not obj.user.is_staff:
                obj.user.is_staff = True
                obj.user.save(update_fields=["is_staff"])
        except Exception:
            # Avoid admin save failures if user update has issues
            pass

    # ----- perms / “add” safety -----

    def has_add_permission(self, request):
        # Profiles are typically created via signals/import; allow superusers only
        return request.user.is_superuser
