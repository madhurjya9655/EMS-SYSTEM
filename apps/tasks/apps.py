from django.apps import AppConfig


class TasksConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.tasks"

    def ready(self):
        """
        Register signal receivers by importing the module that defines them.
        We keep receivers centralized in apps.tasks.utils to avoid duplicates.
        """
        # Side-effect import: connects receivers defined in utils.py
        from . import utils  # noqa: F401
