#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\reports\templatetags\dict_extras.py
from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """
    Template filter for dictionary lookup: {{ my_dict|get_item:"key" }}
    """
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return None
