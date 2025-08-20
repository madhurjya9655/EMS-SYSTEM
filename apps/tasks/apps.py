# apps/tasks/apps.py
from django.apps import AppConfig


class TasksConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.tasks"

    def ready(self):
        """
        Register signal receivers by importing the module that defines them.
        Receivers are centralized in apps.tasks.signals.
        """
        # Side-effect import: connects receivers defined in signals.py
        from . import signals  # noqa: F401
