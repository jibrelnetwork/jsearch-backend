import logging

from marshmallow import Schema, fields, post_load, validates_schema, ValidationError
from marshmallow.marshalling import SCHEMA
from marshmallow.validate import Range, OneOf, Length
from typing import Dict, Any, List

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import (
    ORDER_ASC,
    ORDER_DESC,
    DEFAULT_LIMIT,
    MAX_LIMIT,
    Tag,
    ApiError
)
from jsearch.api.ordering import get_order_schema, Ordering
from jsearch.api.serializers.fields import PositiveIntOrTagField, StrLower
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


def get_field(field: str) -> str:
    if field == SCHEMA:
        return '__all__'
    return field


def get_error_code(field: str) -> str:
    return {
        'limit': ErrorCode.INVALID_LIMIT_VALUE,
        'order': ErrorCode.INVALID_ORDER_VALUE,
        SCHEMA: ErrorCode.VALIDATION_ERROR,
    }.get(field, ErrorCode.INVALID_VALUE)


def get_flatten_error_messages(messages: Dict[str, List[str]]) -> List[Dict[str, str]]:
    flatten_messages = []
    for field, msgs in messages.items():
        for msg in msgs:
            message = {'field': get_field(field), 'message': msg, 'code': get_error_code(field)}
            flatten_messages.append(message)
    return flatten_messages


class ListSchema(Schema):
    limit = fields.Int(
        missing=DEFAULT_LIMIT,
        validate=Range(min=1, max=MAX_LIMIT)
    )
    order = fields.Str(
        missing=ORDER_DESC,
        validate=OneOf([ORDER_ASC, ORDER_DESC], error='Ordering can be either "asc" or "desc".'),
    )
    # Notes: there are cases when outer filters names don't match
    # with fields in database. When we need a mapping.
    # On left side: field name for outer HTTP interface
    # On right side: field name for table
    mapping = {}
    default_values = {}

    class Meta:
        strict = True

    @post_load
    def update_ordering(self, item: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        order_schema = get_order_schema(item.get('timestamp'))
        ordering = self._get_ordering(scheme=order_schema, direction=item['order'])

        # set default value for missing number or timestamp
        for field in ordering.fields:
            field = self.mapping.get(field) or field
            if item.get(field) is None and field in self.default_values:
                item[field] = self.default_values[field]

        item['order'] = ordering
        return item

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        pass

    def handle_error(self, exc: ValidationError, data: Dict[str, Any]) -> None:
        """
        Notes:
            don't forget to wrap handler to ApiError.catch
        """
        messages = {self.mapping and self.mapping.get(key) or key: value for key, value in exc.messages.items()}
        messages = get_flatten_error_messages(messages)
        raise ApiError(messages)


class BlockRelatedListSchema(ListSchema):
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')

    block_number = PositiveIntOrTagField(
        load_from='block_number',
        tags={Tag.LATEST}
    )
    timestamp = PositiveIntOrTagField(tags={Tag.LATEST})

    default_values = {
        'block_number': Tag.LATEST
    }

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if data.get("block_number") and data.get("timestamp"):
            raise ValidationError("Filtration should be either by number or by timestamp")
