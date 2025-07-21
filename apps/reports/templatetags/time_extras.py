from django import template

register = template.Library()

@register.filter
def hhmm(minutes):
    try:
        minutes = int(minutes)
        return f"{minutes//60:02d}:{minutes%60:02d}"
    except:
        return "00:00"
