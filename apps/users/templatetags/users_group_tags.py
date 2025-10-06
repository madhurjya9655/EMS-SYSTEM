# apps/users/templatetags/users_group_tags.py
from __future__ import annotations
from django import template

register = template.Library()

@register.filter(name="has_group")
def has_group(user, group_name: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if not group_name:
        return False
    try:
        return user.groups.filter(name=group_name).exists()
    except Exception:
        return False

@register.filter(name="has_any_group")
def has_any_group(user, csv_group_names: str) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if not csv_group_names:
        return False
    groups = [g.strip() for g in str(csv_group_names).split(",") if g.strip()]
    if not groups:
        return False
    try:
        return user.groups.filter(name__in=groups).exists()
    except Exception:
        return False
