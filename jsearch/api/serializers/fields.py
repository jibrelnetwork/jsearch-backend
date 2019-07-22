from marshmallow import fields
from typing import Optional, Set

from jsearch.api.helpers import Tag


class PositiveIntOrTagField(fields.Field):
    """
    Allows positive integer or one of available tags
    """
    tags = {Tag.LATEST}
    default_error_messages = {
        'invalid': 'Not a valid number or tag.',
        'positive': 'Integer value should be positive.'
    }

    def __init__(self, tags: Optional[Set[str]] = None, *args, **kwargs):
        self.tags = tags or self.tags
        super(PositiveIntOrTagField, self).__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return None

        if value.isdigit():
            try:
                value = int(value)
            except (TypeError, ValueError):
                self.fail('invalid')

            if value < 0:
                self.fail('positive')

            return value

        if value and value.lower() in self.tags:
            return value.lower()

        self.fail('invalid')
