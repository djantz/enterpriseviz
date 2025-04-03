# Licensed under GPLv3 - See LICENSE file for details.
import datetime

from django import template

register = template.Library()


@register.filter
def split(value, key):
    """
      Returns the value turned into a list.
    """
    return value[1:-1].split(key)


@register.filter
def format_date(value):
    try:
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d")
        return value  # Convert to YYYY-MM-DD
    except ValueError:
        return value  # Return the original value if parsing fails
