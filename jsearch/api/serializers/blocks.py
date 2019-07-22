import logging

from marshmallow import Schema, fields, post_load, validates_schema, ValidationError
from marshmallow.validate import Range, OneOf
from typing import Dict, Any

from jsearch.api.database_queries.blocks import get_ordering, get_order_schema
from jsearch.api.helpers import (
    ORDER_ASC,
    ORDER_DESC,
    DEFAULT_LIMIT,
    MAX_LIMIT,
    Tag,
    get_flatten_error_messages)
from jsearch.api.serializers.fields import PositiveIntOrTagField

logger = logging.getLogger(__name__)


class BlocksSchema(Schema):
    limit = fields.Int(
        missing=DEFAULT_LIMIT,
        validate=Range(min=1, max=MAX_LIMIT)
    )

    number = PositiveIntOrTagField(tags=Tag.LATEST)
    timestamp = PositiveIntOrTagField(tags=Tag.LATEST)

    order = fields.Str(
        missing=ORDER_DESC,
        validate=OneOf([ORDER_ASC, ORDER_DESC], error='Ordering can be either "asc" or "desc".'),
    )

    class Meta:
        strict = True

    @post_load
    def update_ordering(self, item: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        order_schema = get_order_schema(item.get('timestamp'))
        ordering = get_ordering(scheme=order_schema, direction=item['order'])

        # set default value for missing number or timestamp
        for field in ordering.fields:
            if item.get(field) is None:
                item[field] = Tag.LATEST

        item['order'] = ordering
        return item

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if data.get("number") and data.get("timestamp"):
            raise ValidationError("Filtration should be either by number or by timestamp")

    def handle_error(self, exc: ValidationError, data):
        exc.messages = get_flatten_error_messages(exc.messages)
