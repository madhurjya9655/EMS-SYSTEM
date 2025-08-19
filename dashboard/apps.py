# E:\CLIENT PROJECT\employee management system bos\employee_management_system\dashboard\apps.py
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class DashboardConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "dashboard"

    def ready(self):
        """
        Register our template tag library as a builtin for all template engines,
        so templates can use the filter |delay_since WITHOUT `{% load %}`.
        Works on Django 5.x.
        """
        try:
            # Import the library so it registers its filters
            from django.template.library import import_library
            lib = import_library("dashboard.templatetags.dashboard_extras")

            # Attach to all configured engines (e.g. "django" & any others)
            from django.template.loader import engines
            for eng in engines.all():
                try:
                    # Django's Engine keeps a list of Library objects in template_builtins
                    builtins = getattr(eng, "template_builtins", None)
                    if isinstance(builtins, list) and lib not in builtins:
                        builtins.append(lib)
                except Exception as e:
                    logger.error("Failed to append dashboard_extras to template_builtins: %s", e)
        except Exception as e:
            logger.error("Failed to register dashboard_extras builtin: %s", e)
