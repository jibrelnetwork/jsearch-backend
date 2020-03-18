import logging

from marshmallow import validates_schema, ValidationError
from marshmallow.validate import Range, Length

from jsearch.api.database_queries.blocks import get_blocks_ordering
from jsearch.api.database_queries.internal_transactions import get_internal_txs_ordering
from jsearch.api.database_queries.logs import get_logs_ordering
from jsearch.api.database_queries.pending_transactions import get_pending_txs_ordering
from jsearch.api.database_queries.token_transfers import get_transfers_ordering
from jsearch.api.database_queries.transactions import get_tx_ordering
from jsearch.api.database_queries.wallet_events import get_wallet_events_ordering
from jsearch.api.ordering import Ordering
from jsearch.api.serializers.common import (
    BlockRelatedListSchema,
    ListSchema,
    is_less_than_two_values_provided,
    ApiErrorSchema
)
from jsearch.api.serializers.fields import StrLower, Timestamp, IntField, BigIntField, JoinedString, quantity_validator
from jsearch.typing import OrderScheme, OrderDirection

logger = logging.getLogger(__name__)


class AccountsBalancesSchema(ApiErrorSchema):
    addresses = JoinedString(to_lower=True, required=True, validate=quantity_validator(min=1, max=10))
    tip_hash = StrLower(validate=Length(min=1, max=100), load_from='blockchain_tip')


class AccountsPendingTxsSchema(ListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    timestamp = Timestamp()
    tx_id = BigIntField(validate=Range(min=0), load_from='id')

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_pending_txs_ordering(scheme, direction)


class AccountsTxsSchema(BlockRelatedListSchema):
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    transaction_index = IntField(validate=Range(min=0))

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_tx_ordering(scheme, direction)


class AccountsTransfersSchema(BlockRelatedListSchema):
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


class AccountsInternalTxsSchema(BlockRelatedListSchema):
    tip_hash = StrLower(load_from='blockchain_tip')
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    transaction_index = IntField(validate=Range(min=1))
    parent_transaction_index = IntField(validate=Range(min=0), load_from='parent_transaction_index')

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
    tip_hash = StrLower(load_from='blockchain_tip')
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    topics = StrLower(validate=Length(min=1))
    transaction_index = IntField(validate=Range(min=0))
    log_index = IntField(validate=Range(min=0))

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


class AccountMinedBlocksSchema(BlockRelatedListSchema):
    tip_hash = StrLower(load_from='blockchain_tip')
    address = StrLower(validate=Length(min=1, max=100), location='match_info')

    mapping = {
        'number': 'block_number'
    }

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_blocks_ordering(scheme, direction)


class EthTransfersListSchema(BlockRelatedListSchema):
    tip_hash = StrLower(load_from='blockchain_tip')
    address = StrLower(validate=Length(min=1, max=100), location='match_info')
    event_index = BigIntField(validate=Range(min=0))

    @validates_schema
    def validate_numbers(self, data, **kwargs):
        if is_less_than_two_values_provided(data, 'block_number', 'timestamp', 'event_index'):
            return

        raise ValidationError("Filtration should be either by number, by timestamp or by event_index")

    def _get_ordering(self, scheme: OrderScheme, direction: OrderDirection) -> Ordering:
        return get_wallet_events_ordering(scheme, direction)
