import logging

from marshmallow import validates_schema, ValidationError
from marshmallow.validate import Length, Range

from jsearch.api.database_queries.uncles import get_uncles_ordering
from jsearch.api.helpers import Tag
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import ListSchema, is_less_than_two_values_provided, ApiErrorSchema
from jsearch.api.serializers.fields import IntField, PositiveIntOrTagField, StrLower
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class UncleListSchema(ListSchema):
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')

    uncle_number = PositiveIntOrTagField(
        load_from='uncle_number',
        tags={Tag.LATEST}
    )
    timestamp = PositiveIntOrTagField(tags={Tag.LATEST})

    default_values = {
        'uncle_number': None
    }

    mapping = {
        'number': 'uncle_number'
    }

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_uncles_ordering(scheme, direction)

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if is_less_than_two_values_provided(data, 'uncle_number', 'timestamp'):
            return

        raise ValidationError("Filtration should be either by number or by timestamp")


class AccountUncleSchema(UncleListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')


class BlockUnclesIndexSchema(ApiErrorSchema):
    uncle_index = IntField(validate=Range(min=0))
