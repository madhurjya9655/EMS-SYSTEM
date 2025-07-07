from django.contrib import admin
from .models import AuthorizedNumber, Holiday, SystemSetting

@admin.register(AuthorizedNumber)
class AuthorizedNumberAdmin(admin.ModelAdmin):
    list_display  = ("label","number","created_at")
    search_fields = ("label","number")

@admin.register(Holiday)
class HolidayAdmin(admin.ModelAdmin):
    list_display  = ("date","name")
    search_fields = ("name",)

@admin.register(SystemSetting)
class SystemSettingAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False  # only one row allowed
    def has_delete_permission(self, request, obj=None):
        return False
    fieldsets = (
        (None, {
            "fields": ("whatsapp_vendor","whatsapp_api_key","whatsapp_sender_id","whatsapp_webhook_url")
        }),
        # ... you can organize into collapsible fieldsets ...
    )
