import logging

from marshmallow import fields, validates_schema, ValidationError, Schema
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.wallet_events import get_events_ordering
from jsearch.api.helpers import Tag
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema, is_less_than_two_values_provided
from jsearch.api.serializers.fields import PositiveIntOrTagField, StrLower, IntField, BigIntField, JoinedString
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class WalletEventsSchema(BlockRelatedListSchema):
    address = StrLower(
        validate=Length(min=1, max=100),
        load_from='blockchain_address',
        required=True
    )

    block_number = PositiveIntOrTagField(
        load_from='block_number',
        tags={Tag.LATEST, Tag.TIP}
    )
    timestamp = PositiveIntOrTagField(tags={Tag.LATEST, Tag.TIP})

    transaction_index = IntField(validate=Range(min=0))
    event_index = BigIntField(validate=Range(min=0))

    include_pending_txs = fields.Bool(missing=False)

    class Meta:
        strict = True

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if is_less_than_two_values_provided(data, 'block_number', 'timestamp', 'event_index'):
            return

        raise ValidationError("Filtration should be either by number, by timestamp or by event_index")

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_events_ordering(scheme, direction)


class WalletAssetsSchema(Schema):
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')

    assets = JoinedString(to_lower=True)
    addresses = JoinedString(to_lower=True)
