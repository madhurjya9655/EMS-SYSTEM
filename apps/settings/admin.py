from django.contrib import admin
from .models import AuthorizedNumber, Holiday

@admin.register(AuthorizedNumber)
class AuthorizedNumberAdmin(admin.ModelAdmin):
    list_display  = ("number", "created_at")
    ordering      = ("-created_at",)
    search_fields = ("number",)

@admin.register(Holiday)
class HolidayAdmin(admin.ModelAdmin):
    list_display  = ("name", "date")
    ordering      = ("date",)
    list_filter   = ("date",)
    search_fields = ("name",)
