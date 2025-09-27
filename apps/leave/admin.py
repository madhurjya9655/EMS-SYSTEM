# apps/leave/admin.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.options import TabularInline
from django.contrib.auth import get_user_model
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from .models import (
    LeaveRequest,
    LeaveDecisionAudit,
    LeaveStatus,
    ApproverMapping,
    LeaveType,
    CCConfiguration,
)

User = get_user_model()
IST = ZoneInfo("Asia/Kolkata")


# ---------------------------------------------------------------------
# Inlines
# ---------------------------------------------------------------------
class LeaveDecisionAuditInline(TabularInline):
    model = LeaveDecisionAudit
    extra = 0
    can_delete = False
    ordering = ("-decided_at",)
    readonly_fields = (
        "action",
        "decided_by",
        "decided_at",
        "token_used",
        "token_manager_email",
        "token_hash",
        "ip_address",
        "user_agent_short",
        "extra_pretty",
    )
    fields = (
        "action",
        "decided_by",
        "decided_at",
        "token_used",
        "token_manager_email",
        "ip_address",
        "user_agent_short",
        "extra_pretty",
    )

    @admin.display(description="User Agent")
    def user_agent_short(self, obj: LeaveDecisionAudit) -> str:
        ua = (obj.user_agent or "").strip()
        return ua if len(ua) <= 120 else ua[:117] + "…"

    @admin.display(description="Context (JSON)")
    def extra_pretty(self, obj: LeaveDecisionAudit) -> str:
        try:
            data = obj.extra or {}
            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            return format_html("<pre style='white-space:pre-wrap'>{}</pre>", pretty)
        except Exception:
            return "—"


# ---------------------------------------------------------------------
# CC Configuration Admin
# ---------------------------------------------------------------------
@admin.register(CCConfiguration)
class CCConfigurationAdmin(admin.ModelAdmin):
    list_display = (
        "user_display",
        "department",
        "is_active",
        "sort_order",
        "updated_at",
    )
    list_filter = (
        "is_active",
        "department",
        ("updated_at", admin.DateFieldListFilter),
    )
    search_fields = (
        "user__username",
        "user__first_name", 
        "user__last_name",
        "user__email",
        "display_name",
        "department",
    )
    ordering = ("sort_order", "department", "user__first_name", "user__last_name")
    
    fields = (
        "user",
        "is_active", 
        "display_name",
        "department",
        "sort_order",
    )
    
    readonly_fields = ("created_at", "updated_at")

    @admin.display(description="User", ordering="user__first_name")
    def user_display(self, obj: CCConfiguration) -> str:
        user = obj.user
        name = user.get_full_name() or user.username
        email = user.email or "no-email"
        display = obj.display_name or name
        active_indicator = "✓" if obj.is_active else "✗"
        return format_html(
            "<strong>{}</strong> ({})<br><span style='color:#6b7280'>{}</span>",
            active_indicator + " " + display,
            email,
            name if obj.display_name else ""
        )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user")


# ---------------------------------------------------------------------
# LeaveRequest admin
# ---------------------------------------------------------------------
@admin.register(LeaveRequest)
class LeaveRequestAdmin(admin.ModelAdmin):
    """
    Production-minded admin:
      • Rich list with filters/search
      • Inline audits
      • Safe bulk approve/reject (model validations & 10:00 IST respected)
      • Routing-map viewer endpoint under this model
    """

    date_hierarchy = "applied_at"
    list_select_related = ("employee", "reporting_person", "approver", "leave_type")
    ordering = ("-applied_at", "-id")

    list_display = (
        "id",
        "employee_name_col",
        "leave_type",
        "start_ist",
        "end_ist",
        "is_half_day",
        "status_badge",
        "manager_col",   # reporting_person
        "cc_display",    # cc users
        "approver_col",
        "decided_at_ist",
        "blocked_days",
    )

    list_filter = (
        "status",
        "is_half_day",
        "leave_type",
        ("reporting_person", admin.RelatedOnlyFieldListFilter),
        ("approver", admin.RelatedOnlyFieldListFilter),
        ("applied_at", admin.DateFieldListFilter),
    )

    search_fields = (
        "id",
        "reason",
        "employee__username",
        "employee__first_name",
        "employee__last_name",
        "employee__email",
        "reporting_person__username",
        "reporting_person__email",
        "approver__username",
        "approver__email",
    )

    readonly_fields = (
        "applied_at",
        "updated_at",
        "employee_name",
        "employee_email",
        "employee_designation",
        "blocked_days",
        "start_date",
        "end_date",
    )

    fields = (
        # Primary
        ("employee", "reporting_person", "leave_type"),
        ("start_at", "end_at", "is_half_day"),
        "reason",
        "attachment",
        # CC Users
        "cc_users",
        # Status/decision
        ("status", "approver", "decided_at"),
        "decision_comment",
        # Snapshots / computed
        ("employee_name", "employee_email", "employee_designation"),
        ("blocked_days", "start_date", "end_date"),
        # System
        ("applied_at", "updated_at"),
    )

    filter_horizontal = ("cc_users",)
    inlines = [LeaveDecisionAuditInline]

    # ---------- displays ----------
    @admin.display(description="Employee", ordering="employee__username")
    def employee_name_col(self, obj: LeaveRequest) -> str:
        u = obj.employee
        name = (u.get_full_name() or u.username or "").strip()
        email = (u.email or "").strip()
        return format_html("<strong>{}</strong><br><span style='color:#6b7280'>{}</span>", name, email or "—")

    @admin.display(description="Manager", ordering="reporting_person__username")
    def manager_col(self, obj: LeaveRequest) -> str:
        m = obj.reporting_person
        if not m:
            return "—"
        return f"{m.get_full_name() or m.username} ({m.email or 'no-email'})"

    @admin.display(description="CC Recipients")
    def cc_display(self, obj: LeaveRequest) -> str:
        cc_users = obj.cc_users.all()
        if not cc_users:
            return "—"
        names = [u.get_full_name() or u.username for u in cc_users[:3]]
        result = ", ".join(names)
        if cc_users.count() > 3:
            result += f" (+{cc_users.count() - 3} more)"
        return result

    @admin.display(description="Approver", ordering="approver__username")
    def approver_col(self, obj: LeaveRequest) -> str:
        a = obj.approver
        if not a:
            return "—"
        return f"{a.get_full_name() or a.username}"

    @admin.display(description="Start (IST)", ordering="start_at")
    def start_ist(self, obj: LeaveRequest) -> str:
        try:
            return timezone.localtime(obj.start_at, IST).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(obj.start_at)

    @admin.display(description="End (IST)", ordering="end_at")
    def end_ist(self, obj: LeaveRequest) -> str:
        try:
            return timezone.localtime(obj.end_at, IST).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(obj.end_at)

    @admin.display(description="Decided (IST)", ordering="decided_at")
    def decided_at_ist(self, obj: LeaveRequest) -> str:
        if not obj.decided_at:
            return "—"
        try:
            return timezone.localtime(obj.decided_at, IST).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(obj.decided_at)

    @admin.display(description="Status", ordering="status")
    def status_badge(self, obj: LeaveRequest) -> str:
        color = {
            LeaveStatus.PENDING: "#f59e0b",
            LeaveStatus.APPROVED: "#10b981",
            LeaveStatus.REJECTED: "#ef4444",
        }.get(obj.status, "#64748b")
        return format_html(
            "<span style='display:inline-block;padding:2px 8px;border-radius:999px;color:#fff;background:{}'>{}</span>",
            color,
            obj.get_status_display(),
        )

    # ---------- read-only locks after decision ----------
    def get_readonly_fields(self, request: HttpRequest, obj: LeaveRequest | None = None) -> Tuple[str, ...]:
        ro = list(super().get_readonly_fields(request, obj))
        if obj and obj.is_decided:
            ro.extend([
                "employee", "reporting_person", "leave_type", "start_at", "end_at",
                "is_half_day", "reason", "attachment", "status", "approver",
                "decided_at", "decision_comment", "cc_users",
            ])
        # dedupe preserving order
        seen, out = set(), []
        for f in ro:
            if f not in seen:
                seen.add(f)
                out.append(f)
        return tuple(out)

    # ---------- bulk actions ----------
    actions = ("action_bulk_approve", "action_bulk_reject")

    @admin.action(description="Approve selected (respects 10:00 AM IST cutoff)")
    def action_bulk_approve(self, request: HttpRequest, queryset):
        """
        Use the model helper (approve) so audits + decision emails are sent,
        and the 10:00 AM IST gate is enforced consistently.
        """
        changed, blocked = 0, 0
        for lr in queryset:
            if lr.is_decided:
                continue
            try:
                lr.approve(by_user=request.user, comment=(lr.decision_comment or "Approved via admin action."))
                changed += 1
            except Exception:
                blocked += 1
        if changed:
            self.message_user(request, f"Approved {changed} leave(s).", level=messages.SUCCESS)
        if blocked:
            self.message_user(
                request,
                f"{blocked} item(s) could not be approved (cutoff/validation).",
                level=messages.WARNING,
            )

    @admin.action(description="Reject selected (respects 10:00 AM IST cutoff)")
    def action_bulk_reject(self, request: HttpRequest, queryset):
        """
        Use the model helper (reject) so audits + decision emails are sent,
        and the 10:00 AM IST gate is enforced consistently.
        """
        changed, blocked = 0, 0
        for lr in queryset:
            if lr.is_decided:
                continue
            try:
                lr.reject(by_user=request.user, comment=(lr.decision_comment or "Rejected via admin action."))
                changed += 1
            except Exception:
                blocked += 1
        if changed:
            self.message_user(request, f"Rejected {changed} leave(s).", level=messages.SUCCESS)
        if blocked:
            self.message_user(
                request,
                f"{blocked} item(s) could not be rejected (cutoff/validation).",
                level=messages.WARNING,
            )

    # ---------- routing-map viewer/editor ----------
    def get_urls(self):
        """Expose /admin/leave/leaverequest/routing-map/ under this model."""
        urls = super().get_urls()
        extra = [
            path(
                "routing-map/",
                self.admin_site.admin_view(self.routing_map_view),
                name="leave_leaverequest_routing_map",
            ),
        ]
        return extra + urls

    def routing_map_view(self, request: HttpRequest) -> HttpResponse:
        file_setting = getattr(settings, "LEAVE_ROUTING_FILE", "apps/users/data/leave_routing.json")
        abs_path = Path(file_setting)
        if not abs_path.is_absolute():
            abs_path = Path(settings.BASE_DIR) / file_setting

        if request.method == "POST":
            if not request.user.is_superuser:
                return HttpResponseForbidden("Only superusers can modify the routing map.")
            new_content = request.POST.get("content", "")
            try:
                parsed = json.loads(new_content or "{}")
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                abs_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8")
                self.message_user(request, "Routing map saved.", level=messages.SUCCESS)
            except json.JSONDecodeError as e:
                self.message_user(request, f"Invalid JSON: {e}", level=messages.ERROR)
            except Exception as e:
                self.message_user(request, f"Could not write file: {e}", level=messages.ERROR)

        content = "{}"
        exists = abs_path.exists()
        if exists:
            try:
                content = abs_path.read_text(encoding="utf-8")
            except Exception:
                content = "{}"

        context = dict(
            self.admin_site.each_context(request),
            title="Leave Routing Map",
            file_path_display=str(file_setting),
            file_exists=exists,
            content=content,
            can_edit=request.user.is_superuser,
            opts=self.model._meta,
            changelist_url=reverse("admin:leave_leaverequest_changelist"),
        )
        return TemplateResponse(request, "leave/admin/routing_map.html", context)


# ---------------------------------------------------------------------
# LeaveDecisionAudit admin (read-only)
# ---------------------------------------------------------------------
@admin.register(LeaveDecisionAudit)
class LeaveDecisionAuditAdmin(admin.ModelAdmin):
    date_hierarchy = "decided_at"
    list_select_related = ("leave", "decided_by")
    ordering = ("-decided_at", "-id")
    actions = None

    list_display = (
        "id",
        "leave_link",
        "action",
        "decided_by",
        "decided_at_ist",
        "token_used",
        "token_manager_email",
        "ip_address",
    )

    readonly_fields = (
        "leave",
        "action",
        "decided_by",
        "decided_at",
        "token_hash",
        "token_manager_email",
        "token_used",
        "ip_address",
        "user_agent",
        "extra_json",
    )

    fields = (
        "leave",
        ("action", "decided_by", "decided_at"),
        ("token_used", "token_manager_email"),
        "ip_address",
        "user_agent",
        "extra_json",
    )

    search_fields = ("leave__id", "action", "token_manager_email", "decided_by__username", "decided_by__email")

    list_filter = (
        "action",
        "token_used",
        ("decided_by", admin.RelatedOnlyFieldListFilter),
        ("decided_at", admin.DateFieldListFilter),
    )

    @admin.display(description="Leave")
    def leave_link(self, obj: LeaveDecisionAudit) -> str:
        url = reverse("admin:leave_leaverequest_change", args=[obj.leave_id])
        return format_html('<a href="{}">#{}</a>', url, obj.leave_id)

    @admin.display(description="Decided (IST)")
    def decided_at_ist(self, obj: LeaveDecisionAudit) -> str:
        try:
            return timezone.localtime(obj.decided_at, IST).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(obj.decided_at)

    @admin.display(description="Context (JSON)")
    def extra_json(self, obj: LeaveDecisionAudit) -> str:
        try:
            return format_html(
                "<pre style='white-space:pre-wrap'>{}</pre>",
                json.dumps(obj.extra or {}, indent=2, ensure_ascii=False),
            )
        except Exception:
            return "—"


# ---------------------------------------------------------------------
# ApproverMapping admin (central routing)
#   - staff can view, only superusers can modify
#   - uses a simple custom change form template (clean UI)
# ---------------------------------------------------------------------
@admin.register(ApproverMapping)
class ApproverMappingAdmin(admin.ModelAdmin):
    change_form_template = "leave/admin/approver_mapping_form.html"

    list_select_related = ("employee", "reporting_person", "cc_person")
    ordering = ("employee__username", "employee__id")

    list_display = (
        "employee_col",
        "reporting_person_col",
        "cc_person_col",
        "updated_at",
        "notes_short",
    )

    list_filter = (
        ("reporting_person", admin.RelatedOnlyFieldListFilter),
        ("cc_person", admin.RelatedOnlyFieldListFilter),
        ("updated_at", admin.DateFieldListFilter),
    )

    search_fields = (
        # Employee
        "employee__username", "employee__first_name", "employee__last_name", "employee__email",
        # Reporting Person (RP)
        "reporting_person__username", "reporting_person__first_name", "reporting_person__last_name", "reporting_person__email",
        # CC
        "cc_person__username", "cc_person__first_name", "cc_person__last_name", "cc_person__email",
    )

    fields = ("employee", "reporting_person", "cc_person", "notes", "updated_at")
    readonly_fields = ("updated_at",)

    @admin.display(description="Employee", ordering="employee__username")
    def employee_col(self, obj: ApproverMapping) -> str:
        u = obj.employee
        name = (u.get_full_name() or u.username or "").strip()
        email = (u.email or "").strip()
        return format_html("<strong>{}</strong><br><span style='color:#6b7280'>{}</span>", name, email or "—")

    @admin.display(description="Reporting Person", ordering="reporting_person__username")
    def reporting_person_col(self, obj: ApproverMapping) -> str:
        m = obj.reporting_person
        if not m:
            return "—"
        return f"{m.get_full_name() or m.username} ({m.email or 'no-email'})"

    @admin.display(description="CC Person", ordering="cc_person__username")
    def cc_person_col(self, obj: ApproverMapping) -> str:
        c = obj.cc_person
        if not c:
            return "—"
        return f"{c.get_full_name() or c.username} ({c.email or 'no-email'})"

    @admin.display(description="Notes")
    def notes_short(self, obj: ApproverMapping) -> str:
        n = (obj.notes or "").strip()
        return n if len(n) <= 80 else n[:77] + "…"

    # --- permissions: staff can view, only superusers can modify ---
    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(getattr(request.user, "is_superuser", False))

    def has_add_permission(self, request: HttpRequest) -> bool:
        return bool(getattr(request.user, "is_superuser", False))

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(getattr(request.user, "is_superuser", False))


# ---------------------------------------------------------------------
# LeaveType admin (simple)
# ---------------------------------------------------------------------
@admin.register(LeaveType)
class LeaveTypeAdmin(admin.ModelAdmin):
    list_display = ("name", "default_days")
    search_fields = ("name",)
    ordering = ("name",)