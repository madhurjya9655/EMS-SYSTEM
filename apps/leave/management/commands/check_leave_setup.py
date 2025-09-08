from __future__ import annotations

import json
from pathlib import Path

from django.conf import settings
from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from .models import LeaveRequest  # adjust if your model name differs


@admin.register(LeaveRequest)
class LeaveRequestAdmin(admin.ModelAdmin):
    list_display = ("id", "employee", "start_date", "end_date", "status")  # tweak to your fields

    # ---- Custom URL for routing map page
    def get_urls(self):
        urls = super().get_urls()
        name = f"{self.model._meta.app_label}_{self.model._meta.model_name}_routing_map"
        my_urls = [
            path(
                "routing-map/",
                self.admin_site.admin_view(self.routing_map_view),
                name=name,
            )
        ]
        return my_urls + urls

    def routing_map_view(self, request):
        # Resolve target path
        default_path = Path(settings.BASE_DIR) / "apps" / "users" / "data" / "leave_routing.json"
        routing_rel = getattr(settings, "LEAVE_ROUTING_FILE", default_path)
        routing_path = Path(routing_rel)
        if not routing_path.is_absolute():
            routing_path = Path(settings.BASE_DIR) / routing_path

        content = ""
        exists = routing_path.exists()
        if exists:
            try:
                content = routing_path.read_text(encoding="utf-8")
            except Exception as e:
                messages.error(request, f"Failed to read file: {e}")

        # Save on POST (superusers only)
        if request.method == "POST":
            if not request.user.is_superuser:
                messages.error(request, "You are not allowed to edit the routing map.")
                return redirect(
                    f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_routing_map"
                )
            new_body = request.POST.get("content", "").strip()
            try:
                json.loads(new_body or "{}")  # validate JSON
            except Exception as e:
                messages.error(request, f"JSON invalid: {e}")
            else:
                try:
                    routing_path.parent.mkdir(parents=True, exist_ok=True)
                    routing_path.write_text(new_body, encoding="utf-8")
                    messages.success(request, f"Saved {routing_path}")
                    return redirect(
                        f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_routing_map"
                    )
                except Exception as e:
                    messages.error(request, f"Failed to save: {e}")

            # fall through to re-render with the attempted body
            content = new_body

        context = dict(
            self.admin_site.each_context(request),
            title="Leave Routing Map",
            file_path_display=str(routing_path),
            file_exists=exists,
            content=content,
            can_edit=request.user.is_superuser,
            opts=self.model._meta,
            changelist_url=reverse(f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_changelist"),
        )
        return TemplateResponse(request, "leave/admin/routing_map.html", context)
