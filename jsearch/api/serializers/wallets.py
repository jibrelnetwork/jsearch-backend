import logging

from marshmallow import fields
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.wallet_events import get_events_ordering
from jsearch.api.helpers import Tag
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema
from jsearch.api.serializers.fields import PositiveIntOrTagField, StrLower
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
    transaction_index = fields.Int(validate=Range(min=0))
    event_index = fields.Int(validate=Range(min=0))

    include_pending_txs = fields.Bool(missing=False)

    class Meta:
        strict = True

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_events_ordering(scheme, direction)
