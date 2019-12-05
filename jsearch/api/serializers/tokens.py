from marshmallow import validates_schema, ValidationError
from marshmallow.fields import Integer
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.token_holders import get_token_holders_ordering
from jsearch.api.database_queries.token_transfers import get_transfers_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema, ListSchema
from jsearch.api.serializers.fields import StrLower, IntField, BigIntField
from jsearch.typing import OrderScheme, OrderDirection


class TokenTransfersSchema(BlockRelatedListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')

    log_index = IntField(validate=Range(min=0))
    transaction_index = IntField(validate=Range(min=0))

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_transfers_ordering(scheme, direction)

    @validates_schema
    def validate_filters(self, data, **kwargs):
        timestamp = data.get('timestamp')
        block_number = data.get("block_number")

        there_is_not_pointer_to_block = timestamp is None and block_number is None

        log_index = data.get("log_index")
        transaction_index = data.get("transaction_index")

        if there_is_not_pointer_to_block and transaction_index is not None:
            raise ValidationError("Filter `transaction_index` requires `block_number` or `timestamp` value.")

        if log_index is not None and transaction_index is None:
            raise ValidationError("Filter `log_index` requires `transaction_index` value.")


class TokenHoldersListSchema(ListSchema):
    _id = BigIntField(validate=Range(min=0), load_from='id')
    balance = Integer(validate=Range(min=0))

    address = StrLower(validate=Length(min=1, max=100), location='match_info')

    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')

    mapping = {
        'id': '_id'
    }

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_token_holders_ordering(scheme, direction)
