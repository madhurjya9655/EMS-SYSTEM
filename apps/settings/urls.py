# apps/settings/urls.py
from django.urls import path
from . import views

app_name = "settings"

urlpatterns = [
    # Match the paths you already use (per your logs)
    path("authorized/", views.authorized_list, name="authorized_list"),
    path("holidays/", views.holiday_list, name="holiday_list"),
    path("system-settings/", views.system_settings, name="system_settings"),
]
