# apps/users/apps.py
from __future__ import annotations

from django.apps import AppConfig


class UsersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.users"
    verbose_name = "Users"

    def ready(self) -> None:
        """
        Import-time hook — registers all signals on server startup.

        Signals registered here:
          1. Profile.post_save  → ensure Admin role marks user.is_staff = True
          2. User.post_save     → sync Employee.is_active from User.is_active
                                  (single source of truth enforcement)
        """
        import apps.users.signals  # noqa: F401  — registers all receivers