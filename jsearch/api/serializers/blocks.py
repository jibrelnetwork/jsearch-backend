import logging

from marshmallow.validate import Range

from jsearch.api.database_queries.blocks import get_blocks_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema, ApiErrorSchema
from jsearch.api.serializers.fields import IntField
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class BlockListSchema(BlockRelatedListSchema):
    mapping = {
        'number': 'block_number'
    }

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_blocks_ordering(scheme, direction)


class BlockTransactionsIndexSchema(ApiErrorSchema):
    transaction_index = IntField(validate=Range(min=0))
