from django.apps import AppConfig


class SalesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.sales'

    def ready(self):
        # Import signal handlers for live Google Sheets sync.
        # Safe import guarded inside function so migrations/admin won't explode.
        try:
            from . import signals  # noqa: F401
        except Exception:
            # Never block app loading because of optional integrations.
            pass
