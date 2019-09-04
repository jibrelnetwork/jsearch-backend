from datetime import datetime

from marshmallow import fields
from marshmallow.validate import Range
from typing import Set

INT_MAX = 2147483647
BIGINT_MAX = 9223372036854775807

int_validator = Range(min=INT_MAX * -1, max=INT_MAX)
big_int_validator = Range(min=BIGINT_MAX * -1, max=BIGINT_MAX)


class IntField(fields.Integer):

    def __init__(self, *args, **kwargs):
        super(IntField, self).__init__(*args, **kwargs)

        self.validators.append(int_validator)


class BigIntField(fields.Integer):
    def __init__(self, *args, **kwargs):
        super(BigIntField, self).__init__(*args, **kwargs)

        self.validators.append(big_int_validator)


class PositiveIntOrTagField(fields.Field):
    """
    Allows positive big integer or one of available tags
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

            return int_validator(value)

        if value and value.lower() in self.tags:
            return value.lower()

        self.fail('invalid')


class StrLower(fields.String):

    def _deserialize(self, value, attr, data):
        value = super(StrLower, self)._deserialize(value, attr, data)
        if value:
            return value.lower()


class Timestamp(fields.Integer):
    default_error_messages = {
        'invalid': 'Not a valid timestamp.'
    }

    def _deserialize(self, value, attr, data):
        value = super(Timestamp, self)._deserialize(value, attr, data)
        if value:
            try:
                return datetime.fromtimestamp(value)
            except (ValueError, OverflowError):
                self.fail('invalid')
