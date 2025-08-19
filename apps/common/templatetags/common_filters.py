from django import template
from django.utils.safestring import mark_safe
from django.utils.html import escape
import json
from datetime import datetime, date

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """Get item from dictionary by key"""
    if hasattr(dictionary, 'get'):
        return dictionary.get(key)
    return None


@register.filter
def get_attr(obj, attr_name):
    """Get attribute from object"""
    try:
        return getattr(obj, attr_name, None)
    except:
        return None


@register.filter
def to_json(value):
    """Convert value to JSON string"""
    try:
        return mark_safe(json.dumps(value))
    except:
        return '{}'


@register.filter
def multiply(value, arg):
    """Multiply value by argument"""
    try:
        return float(value) * float(arg)
    except:
        return 0


@register.filter
def divide(value, arg):
    """Divide value by argument"""
    try:
        return float(value) / float(arg)
    except:
        return 0


@register.filter
def percentage(value, total):
    """Calculate percentage"""
    try:
        if float(total) == 0:
            return 0
        return round((float(value) / float(total)) * 100, 1)
    except:
        return 0


@register.filter
def format_duration(minutes):
    """Format minutes into hours and minutes"""
    try:
        minutes = int(minutes)
        hours = minutes // 60
        mins = minutes % 60
        if hours > 0:
            return f"{hours}h {mins}m"
        return f"{mins}m"
    except:
        return "0m"


@register.filter
def status_badge_class(status):
    """Get CSS class for status badge"""
    status_classes = {
        'Pending': 'badge-warning',
        'Completed': 'badge-success', 
        'Open': 'badge-danger',
        'In Progress': 'badge-info',
        'Closed': 'badge-success',
        'Low': 'badge-success',
        'Medium': 'badge-warning',
        'High': 'badge-danger',
    }
    return status_classes.get(status, 'badge-secondary')


@register.filter
def priority_color(priority):
    """Get color class for priority"""
    colors = {
        'Low': 'text-success',
        'Medium': 'text-warning',
        'High': 'text-danger',
    }
    return colors.get(priority, 'text-secondary')


@register.filter
def is_overdue(planned_date):
    """Check if date is overdue"""
    if not planned_date:
        return False
    from django.utils import timezone
    return timezone.now() > planned_date


@register.filter
def days_until(target_date):
    """Calculate days until target date"""
    if not target_date:
        return 0
    from django.utils import timezone
    if hasattr(target_date, 'date'):
        target_date = target_date.date()
    today = timezone.now().date()
    delta = target_date - today
    return delta.days


@register.filter
def truncate_chars(value, length):
    """Truncate string to specified length"""
    if not value:
        return ''
    value = str(value)
    if len(value) <= length:
        return value
    return value[:length] + '...'