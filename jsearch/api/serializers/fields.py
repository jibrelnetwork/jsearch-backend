from marshmallow import fields
from typing import Set


class PositiveIntOrTagField(fields.Field):
    """
    Allows positive integer or one of available tags
    """
    default_error_messages = {
        'invalid': 'Not a valid number or tag.',
        'positive': 'Integer value should be positive.'
    }

    def __init__(self, tags: Set[str], *args, **kwargs):
        if not isinstance(tags, Set):
            raise ValueError(f'Tags should be a set instead of {tags}')

        self.tags = tags
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
