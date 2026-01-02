# --- dict_get: safely fetch dict[key] with a variable key in templates ---
from django import template
register = template.Library()

@register.filter
def dict_get(d, key):
    """
    Usage in templates:
        {{ my_dict|dict_get:some_key|default:"Fallback" }}
    Works when `some_key` is a variable (not a literal).
    """
    try:
        return (d or {}).get(key, "")
    except Exception:
        return ""
