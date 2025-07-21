from django import template

register = template.Library()

@register.filter
def modulo(value, arg):
    try:
        return int(value) % int(arg)
    except (ValueError, ZeroDivisionError, TypeError):
        return ''
