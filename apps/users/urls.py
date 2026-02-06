# apps/users/urls.py
from __future__ import annotations

from django.urls import path

from . import views

app_name = "users"

urlpatterns = [
    # User management (admin-only via decorators in views)
    path("", views.list_users, name="list_users"),
    path("add/", views.add_user, name="add_user"),
    path("<int:pk>/edit/", views.edit_user, name="edit_user"),
    path("<int:pk>/delete/", views.delete_user, name="delete_user"),
    path("<int:pk>/toggle-active/", views.toggle_active, name="toggle_active"),

    # Permission debugging
    path("debug-permissions/", views.debug_permissions, name="debug_permissions"),
]
