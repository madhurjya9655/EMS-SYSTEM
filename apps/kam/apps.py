from django.apps import AppConfig


class KamConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    # IMPORTANT: this must be the *module path* to the app package
    name = "apps.kam"
    # Optional stable app label (keeps migrations referencing 'kam' working)
    label = "kam"
    verbose_name = "KAM (Sales Performance)"
