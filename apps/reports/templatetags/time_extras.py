#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reports\templatetags\time_extras.py
from django import template

register = template.Library()

@register.filter
def hhmm(minutes):
    try:
        minutes = int(minutes)
        return f"{minutes//60:02d}:{minutes%60:02d}"
    except:
        return "00:00"
