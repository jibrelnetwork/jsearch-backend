import logging

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.wallet_events import get_events_ordering
from jsearch.api.helpers import Tag
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema
from jsearch.api.serializers.fields import PositiveIntOrTagField, StrLower, IntField, BigIntField
from jsearch.common.wallet_events import is_event_belongs_to_block, make_event_index
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

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_events_ordering(scheme, direction)

    @validates_schema
    def validate_event_index_block_number_relation(self, data, **kwargs):
        event_index = data.get("event_index")
        block_number = data.get("block_number")

        if event_index is None or block_number is None or is_event_belongs_to_block(event_index, block_number):
            return

        lower_bound = make_event_index(block_number, 0, 0)
        upper_bound = make_event_index(block_number, 999, 9999)

        raise ValidationError(
            f"Must belong to block {block_number} and be between {lower_bound} "
            f"and {upper_bound}.",
            ["event_index"],
        )
