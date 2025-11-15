# apps/settings/urls.py
from django.urls import path
from . import views

app_name = "settings"

urlpatterns = [
    path("authorized/", views.authorized_list, name="authorized_list"),
    path("holidays/", views.holiday_list, name="holiday_list"),
    path("holidays/delete/<int:pk>/", views.holiday_delete, name="holiday_delete"),
    path("system-settings/", views.system_settings, name="system_settings"),
]

