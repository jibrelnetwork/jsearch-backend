import logging

from jsearch.api.database_queries.blocks import get_blocks_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class BlockListSchema(BlockRelatedListSchema):
    mapping = {
        'number': 'block_number'
    }

    def _get_tx_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_blocks_ordering(scheme, direction)
