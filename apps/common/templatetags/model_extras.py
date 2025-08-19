# apps/common/templatetags/model_extras.py
from django import template

register = template.Library()

@register.filter
def model_name(obj):
    """
    Safe way to get a model's name in templates.
    Usage: {{ myobj|model_name }}
    """
    try:
        # ._meta is blocked in templates, but OK here in Python
        return obj._meta.model_name
    except Exception:
        try:
            return obj.__class__.__name__
        except Exception:
            return ""
