# apps/tasks/apps.py

from django.apps import AppConfig

class TasksConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.tasks'

    def ready(self):
        # This ensures signals are imported when the app is ready
        import apps.tasks.signals  # noqa
