import logging

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.internal_transactions import get_internal_txs_ordering
from jsearch.api.database_queries.logs import get_logs_ordering
from jsearch.api.database_queries.pending_transactions import get_pending_txs_ordering
from jsearch.api.database_queries.transactions import get_tx_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import BlockRelatedListSchema, ListSchema
from jsearch.api.serializers.fields import StrLower, Timestamp
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class AccountsPendingTxsSchema(ListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    timestamp = Timestamp()
    tx_id = fields.Int(validate=Range(min=0), load_from='id')

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_pending_txs_ordering(scheme, direction)


class AccountsTxsSchema(BlockRelatedListSchema):
    address = fields.Str(validate=Length(min=1, max=100), location='match_info')
    transaction_index = fields.Int(validate=Range(min=0))

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_tx_ordering(scheme, direction)


class AccountsInternalTxsSchema(BlockRelatedListSchema):
    tip_hash = fields.Str(load_from='blockchain_tip')
    address = fields.Str(validate=Length(min=1, max=100), location='match_info')
    transaction_index = fields.Int(validate=Range(min=1))
    parent_transaction_index = fields.Int(validate=Range(min=0), load_from='parent_transaction_index')

    mapping = {
        'parent_tx_index': 'parent_transaction_index'
    }

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_internal_txs_ordering(scheme, direction)

    @validates_schema
    def validate_filters(self, data, **kwargs):
        timestamp = data.get('timestamp')
        block_number = data.get("block_number")

        there_is_not_pointer_to_block = timestamp is None and block_number is None

        transaction_index = data.get("transaction_index")
        parent_transaction_index = data.get("parent_transaction_index")

        if there_is_not_pointer_to_block and parent_transaction_index is not None:
            raise ValidationError("Filter `parent_transaction_index` requires `block_number` or `timestamp` value.")

        if transaction_index is not None and parent_transaction_index is None:
            raise ValidationError("Filter `transaction_index` requires `parent_transaction_index` value.")


class AccountLogsSchema(BlockRelatedListSchema):
    tip_hash = fields.Str(load_from='blockchain_tip')
    address = fields.Str(validate=Length(min=1, max=100), location='match_info')
    transaction_index = fields.Int(validate=Range(min=0))
    log_index = fields.Int(validate=Range(min=0))

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_logs_ordering(scheme, direction)

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
