from django.urls import path
from . import views

app_name = "settings"

urlpatterns = [
    path("authorized/",            views.authorized_list,   name="authorized_list"),
    path("authorized/delete/<int:pk>/", views.authorized_delete, name="authorized_delete"),
    path("holidays/",              views.holiday_list,      name="holiday_list"),
    path("holidays/delete/<int:pk>/",   views.holiday_delete,   name="holiday_delete"),
    path("system-settings/",       views.system_settings,   name="system_settings"),
]
