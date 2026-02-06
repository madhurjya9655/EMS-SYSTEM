# apps/kam/templatetags/kam_extras.py
from django import template
register = template.Library()

@register.filter
def get(d, key):
    try:
        return d.get(key)
    except Exception:
        return None

@register.filter
def index(seq, i):
    try:
        return seq[int(i)]
    except Exception:
        return None
