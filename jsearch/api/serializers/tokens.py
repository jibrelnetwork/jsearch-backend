from marshmallow import fields
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.token_transfers import get_transfers_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema
from jsearch.api.serializers.fields import StrLower
from jsearch.typing import OrderScheme, OrderDirection


class TokenTransfersSchema(BlockRelatedListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')

    log_index = fields.Integer(validate=Range(min=0))
    transaction_index = fields.Integer(validate=Range(min=0))

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_transfers_ordering(scheme, direction)
