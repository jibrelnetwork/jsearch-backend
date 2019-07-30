import logging

from marshmallow import validates_schema, ValidationError

from jsearch.api.database_queries.uncles import get_uncles_ordering
from jsearch.api.helpers import (
    Tag,
)
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import TimeRelatedListSchema
from jsearch.api.serializers.fields import PositiveIntOrTagField
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class UncleListSchema(TimeRelatedListSchema):
    uncle_number = PositiveIntOrTagField(
        load_from='uncle_number',
        tags={Tag.LATEST}
    )

    default_values = {
        'uncle_number': Tag.LATEST
    }

    mapping = {
        'number': 'uncle_number'
    }

    class Meta:
        strict = True

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_uncles_ordering(scheme, direction)

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if data.get("uncle_number") and data.get("timestamp"):
            raise ValidationError("Filtration should be either by number or by timestamp")
