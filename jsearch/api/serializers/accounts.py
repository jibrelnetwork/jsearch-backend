import logging

from marshmallow import fields
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.transactions import get_tx_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class AccountsTxsSchema(BlockRelatedListSchema):
    address = fields.Str(validate=Length(min=1, max=100), location='match_info')
    transaction_index = fields.Int(validate=Range(min=0))

    class Meta:
        strict = True

    def _get_tx_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_tx_ordering(scheme, direction)
