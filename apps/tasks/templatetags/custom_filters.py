# apps/tasks/templatetags/custom_filters.py
from django import template
from django.utils import timezone
from datetime import datetime
import pytz

register = template.Library()

@register.filter
def subtract(value, arg):
    """Subtract arg from value"""
    try:
        return int(value) - int(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def multiply(value, arg):
    """Multiply value by arg"""
    try:
        return int(value) * int(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def divide(value, arg):
    """Divide value by arg"""
    try:
        return int(value) // int(arg) if int(arg) != 0 else 0
    except (ValueError, TypeError):
        return 0

@register.filter
def to_ist(value):
    """Convert datetime to IST"""
    if not value:
        return ""
    ist = pytz.timezone('Asia/Kolkata')
    if timezone.is_aware(value):
        return value.astimezone(ist)
    return ist.localize(value)

@register.filter
def minutes_to_hours(minutes):
    """Convert minutes to hours:minutes format"""
    try:
        total_minutes = int(minutes)
        hours = total_minutes // 60
        mins = total_minutes % 60
        return f"{hours:02d}:{mins:02d}"
    except (ValueError, TypeError):
        return "00:00"