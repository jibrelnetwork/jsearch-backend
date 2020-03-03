import logging

from marshmallow.fields import Int
from marshmallow.validate import Length, ContainsOnly, Range

from jsearch.api.database_queries.dex_logs import get_events_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema, ApiErrorSchema
from jsearch.api.serializers.fields import StrLower, JoinedString
from jsearch.common.processing.dex_logs import DexEventType, ORDER_STATUSES
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)

PENDING_EVENTS_DEFAULT_LIMIT = 100


class DexHistorySchema(BlockRelatedListSchema):
    token_address = StrLower(validate=Length(min=1, max=100), location='match_info')
    event_index = Int(validate=Range(min=0))
    event_type = JoinedString(
        validate=ContainsOnly(
            choices=[
                *DexEventType.ORDERS,
                *DexEventType.TRADE
            ],
            error='Available filters: {choices}'
        )
    )

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_events_ordering(scheme, direction)


class DexOrdersSchema(ApiErrorSchema):
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')
    token_address = StrLower(validate=Length(min=1, max=100), location='match_info')
    order_status = JoinedString(
        validate=ContainsOnly(
            choices=ORDER_STATUSES.keys(),
            error='Available filters: {choices}'
        )
    )
    order_creator = StrLower(validate=Length(min=1, max=100))


class DexBlockedAmountsSchema(ApiErrorSchema):
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')
    user_address = StrLower(validate=Length(min=1, max=100), location='match_info')
    token_addresses = JoinedString(load_from='token_address')
