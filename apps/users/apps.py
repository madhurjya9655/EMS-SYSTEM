from __future__ import annotations

from django.apps import AppConfig


class UsersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.users"
    verbose_name = "Users"

    def ready(self) -> None:
        """
        Import-time hook for signals or other startup wiring.

        NOTE: We are NOT auto-creating Profile instances here because
        Profile.phone is unique/required — silently creating profiles
        without a valid phone would cause integrity/UX issues.
        """
        # from . import signals  # Enable when you actually add signals
        return
