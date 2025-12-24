from django.apps import AppConfig

class ReimbursementConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.reimbursement'
    verbose_name = "Reimbursement"

    def ready(self):
        from . import signals  # noqa: F401
