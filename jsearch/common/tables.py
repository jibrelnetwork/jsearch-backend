import logging

import sqlalchemy as sa
import sqlalchemy.types as types
from sqlalchemy.dialects import postgresql


class HexInteger(types.TypeDecorator):
    """
    Converts hex string to HexInteger
    """

    impl = types.Integer

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)


class HexBigInteger(types.TypeDecorator):
    """
    Converts hex string to HexInteger
    """

    impl = types.BigInteger

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)


class HexToDecString(types.TypeDecorator):
    """
    Converts hex string to dec string
    """

    impl = types.String

    def process_bind_param(self, value, dialect):
        logging.debug({'value': value})

        if value is None:
            return None

        if isinstance(value, str) and value.startswith('0x'):
            return str(int(value, 16))

        return str(int(value))


metadata = sa.MetaData()

uncles_t = sa.Table(
    'uncles',
    metadata,
    sa.Column('hash', sa.String, primary_key=True),
    sa.Column('number', HexInteger, index=True),
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, primary_key=True),
    sa.Column('parent_hash', sa.String),
    sa.Column('difficulty', HexBigInteger),
    sa.Column('extra_data', sa.String),
    sa.Column('gas_limit', HexBigInteger),
    sa.Column('gas_used', HexBigInteger),
    sa.Column('logs_bloom', sa.String),
    sa.Column('miner', sa.String),
    sa.Column('mix_hash', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('receipts_root', sa.String),
    sa.Column('sha3_uncles', sa.String),
    sa.Column('size', HexInteger),
    sa.Column('state_root', sa.String),
    sa.Column('timestamp', HexInteger),
    sa.Column('total_difficulty', HexBigInteger),
    sa.Column('transactions_root', sa.String),
    sa.Column('reward', postgresql.NUMERIC(32, 0)),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
)

internal_transactions_t = sa.Table(
    'internal_transactions',
    metadata,
    # denormalization for `internal_transaction.transaction.from`
    sa.Column('tx_origin', sa.String),

    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, primary_key=True),
    sa.Column('parent_tx_hash', sa.String, primary_key=True),
    # denormalization for `internal_transactions.transaction.transaction_index`
    sa.Column('parent_tx_index', sa.String, primary_key=True),
    sa.Column('op', sa.String),
    sa.Column('call_depth', HexInteger),
    sa.Column('timestamp', HexInteger),
    sa.Column('from', sa.String),
    sa.Column('to', sa.String),
    sa.Column('value', postgresql.NUMERIC(32, 0)),
    sa.Column('gas_limit', HexBigInteger),
    sa.Column('payload', sa.String),
    sa.Column('status', sa.String),
    sa.Column('transaction_index', sa.Integer, primary_key=True),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
)

pending_transactions_t = sa.Table(
    'pending_transactions',
    metadata,
    sa.Column('id', sa.BigInteger),
    sa.Column('last_synced_id', sa.BigInteger, index=True),
    sa.Column('hash', sa.String(70), primary_key=True),
    sa.Column('status', sa.String),
    sa.Column('timestamp', postgresql.TIMESTAMP),
    sa.Column('removed', sa.Boolean),
    sa.Column('node_id', sa.String(70)),
    sa.Column('r', sa.String),
    sa.Column('s', sa.String),
    sa.Column('v', sa.String),
    sa.Column('to', sa.String, index=True),
    sa.Column('from', sa.String, index=True),
    sa.Column('gas', HexBigInteger),
    sa.Column('gas_price', HexBigInteger),
    sa.Column('input', sa.String),
    sa.Column('nonce', HexBigInteger),
    sa.Column('value', HexToDecString),
)

transactions_t = sa.Table(
    'transactions',
    metadata,
    sa.Column('hash', sa.String, index=True),
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, index=True, primary_key=True),
    sa.Column('transaction_index', HexInteger, primary_key=True),
    sa.Column('timestamp', HexInteger),
    sa.Column('from', sa.String, index=True),
    sa.Column('to', sa.String, index=True),
    sa.Column('gas', sa.String),
    sa.Column('gas_price', sa.String),
    sa.Column('input', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('r', sa.String),
    sa.Column('s', sa.String),
    sa.Column('v', sa.String),
    sa.Column('value', sa.String),
    sa.Column('contract_call_description', postgresql.JSONB),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
    sa.Column('address', sa.String, index=True),
    sa.Column('status', sa.Integer)
)

receipts_t = sa.Table(
    'receipts',
    metadata,
    sa.Column('transaction_hash', sa.String, index=True),
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, primary_key=True),
    sa.Column('contract_address', sa.String, index=True),
    sa.Column('cumulative_gas_used', HexInteger),
    sa.Column('from', sa.String),
    sa.Column('to', sa.String),
    sa.Column('gas_used', HexInteger),
    sa.Column('logs_bloom', sa.String),
    sa.Column('root', sa.String),
    sa.Column('transaction_index', HexInteger, primary_key=True),
    sa.Column('status', HexInteger),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
)

logs_t = sa.Table(
    'logs',
    metadata,
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, index=True, primary_key=True),
    sa.Column('timestamp', HexInteger),
    sa.Column('log_index', HexInteger, primary_key=True),
    sa.Column('address', sa.String),
    sa.Column('data', sa.String),
    sa.Column('removed', sa.Boolean),
    sa.Column('topics', postgresql.ARRAY(sa.String)),
    sa.Column('transaction_hash', sa.String, primary_key=True),
    sa.Column('transaction_index', HexInteger),

    sa.Column('event_type', sa.String),
    sa.Column('event_args', postgresql.JSONB),

    sa.Column('token_amount', postgresql.NUMERIC()),
    sa.Column('token_transfer_from', sa.String, index=True),
    sa.Column('token_transfer_to', sa.String, index=True),

    sa.Column('is_token_transfer', sa.Boolean, index=True, default=False),
    sa.Column('is_processed', sa.Boolean, index=True, default=False),
    sa.Column('is_transfer_processed', sa.Boolean, index=True, default=False),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
)

accounts_state_t = sa.Table(
    'accounts_state',
    metadata,
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, primary_key=True),
    sa.Column('address', sa.String, primary_key=True),
    sa.Column('nonce', HexInteger),
    sa.Column('root', sa.String),
    sa.Column('balance', postgresql.NUMERIC(32, 0)),
    sa.Column('is_forked', sa.Boolean, default=False, index=True),
)

accounts_base_t = sa.Table(
    'accounts_base',
    metadata,
    sa.Column('address', sa.String, primary_key=True),
    sa.Column('code', sa.String),
    sa.Column('code_hash', sa.String),
    sa.Column('last_known_balance', postgresql.NUMERIC(32, 0)),
    sa.Column('root', sa.String),
)

blocks_t = sa.Table(
    'blocks',
    metadata,
    sa.Column('number', HexInteger, index=True),
    sa.Column('hash', sa.String, primary_key=True),
    sa.Column('transactions', postgresql.JSONB),
    sa.Column('uncles', postgresql.JSONB),
    sa.Column('parent_hash', sa.String),
    sa.Column('difficulty', HexBigInteger),
    sa.Column('extra_data', sa.String),
    sa.Column('gas_limit', HexBigInteger),
    sa.Column('gas_used', HexBigInteger),
    sa.Column('logs_bloom', sa.String),
    sa.Column('miner', sa.String, index=True),
    sa.Column('mix_hash', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('receipts_root', sa.String),
    sa.Column('sha3_uncles', sa.String),
    sa.Column('size', HexInteger),
    sa.Column('state_root', sa.String),
    sa.Column('timestamp', HexInteger),
    sa.Column('total_difficulty', HexBigInteger),
    sa.Column('transactions_root', sa.String),
    sa.Column('is_sequence_sync', sa.Boolean),
    sa.Column('static_reward', postgresql.NUMERIC(32, 0)),
    sa.Column('uncle_inclusion_reward', postgresql.NUMERIC(32, 0)),
    sa.Column('tx_fees', postgresql.NUMERIC(32, 0)),
    sa.Column('is_forked', sa.Boolean, nullable=False, index=True, server_default='false'),
)

token_holders_t = sa.Table(
    'token_holders',
    metadata,
    sa.Column('id', sa.BigInteger),
    sa.Column('account_address', sa.String, primary_key=True),
    sa.Column('token_address', sa.String, primary_key=True),
    sa.Column('balance', postgresql.NUMERIC(32, 0), index=True),
    sa.Column('decimals', sa.Integer, index=True),
    sa.Column('block_number', sa.Integer),
    sa.Column('block_hash', sa.String, nullable=True),
    sa.Column('is_forked', sa.Boolean, default=False),
)

reorgs_t = sa.Table(
    'reorgs',
    metadata,
    sa.Column('id', sa.BigInteger, primary_key=True),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', sa.Integer),
    sa.Column('reinserted', sa.Boolean),
    sa.Column('node_id', sa.String),
    sa.Column('split_id', sa.BigInteger),
)

token_transfers_t = sa.Table(
    'token_transfers',
    metadata,
    sa.Column('address', sa.String),
    sa.Column('transaction_hash', sa.String),
    sa.Column('transaction_index', sa.Integer),
    sa.Column('log_index', sa.Integer),
    sa.Column('block_number', sa.Integer),
    sa.Column('block_hash', sa.String),
    sa.Column('timestamp', sa.Integer),
    sa.Column('from_address', sa.String),
    sa.Column('to_address', sa.String),
    sa.Column('token_address', sa.String),
    sa.Column('token_value', postgresql.NUMERIC()),
    sa.Column('token_decimals', sa.Integer),
    sa.Column('token_name', sa.String),
    sa.Column('token_symbol', sa.String),
    sa.Column('is_forked', sa.Boolean),
    sa.Column('status', sa.Integer),
)

chain_splits_t = sa.Table(
    'chain_splits',
    metadata,
    sa.Column('id', sa.BigInteger, primary_key=True),
    sa.Column('common_block_number', sa.Integer),
    sa.Column('common_block_hash', sa.String),
    sa.Column('drop_length', sa.Integer),
    sa.Column('drop_block_hash', sa.String),
    sa.Column('add_length', sa.Integer),
    sa.Column('add_block_hash', sa.String),
    sa.Column('node_id', sa.String),
)

assets_transfers_t = sa.Table(
    'assets_transfers',
    metadata,
    sa.Column('address', sa.String),
    sa.Column('type', sa.String),
    sa.Column('from', sa.String),
    sa.Column('to', sa.String),
    sa.Column('asset_address', sa.String),
    sa.Column('value', postgresql.NUMERIC()),
    sa.Column('decimals', sa.Integer),
    sa.Column('tx_data', postgresql.JSONB),
    sa.Column('is_forked', sa.Boolean),
    sa.Column('block_number', sa.BigInteger),
    sa.Column('block_hash', sa.String),
    sa.Column('ordering', sa.String),
    sa.Column('status', sa.Integer),
)

assets_summary_t = sa.Table(
    'assets_summary',
    metadata,
    sa.Column('address', sa.String),
    sa.Column('asset_address', sa.String),
    sa.Column('value', postgresql.NUMERIC()),
    sa.Column('decimals', sa.Integer),
    sa.Column('tx_number', sa.BigInteger),
    sa.Column('nonce', sa.BigInteger),
    sa.Column('block_number', sa.BigInteger),
    sa.Column('block_hash', sa.String, nullable=True),
    sa.Column('is_forked', sa.Boolean, default=False),
)

wallet_events_t = sa.Table(
    'wallet_events',
    metadata,
    sa.Column('address', sa.String),
    sa.Column('type', sa.String),
    sa.Column('tx_hash', sa.String),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', sa.BigInteger),
    sa.Column('event_index', sa.BigInteger),
    sa.Column('is_forked', sa.Boolean, default=False),
    sa.Column('tx_data', postgresql.JSONB),
    sa.Column('event_data', postgresql.JSONB),
)

chain_events_t = sa.Table(
    'chain_events',
    metadata,
    sa.Column('id', sa.BigInteger),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', sa.BigInteger),
    sa.Column('type', sa.String),
    sa.Column('parent_block_hash', sa.String),
    sa.Column('common_block_number', sa.BigInteger),
    sa.Column('common_block_hash', sa.String),
    sa.Column('drop_length', sa.BigInteger),
    sa.Column('drop_block_hash', sa.String),
    sa.Column('add_length', sa.BigInteger),
    sa.Column('add_block_hash', sa.String),
    sa.Column('node_id', sa.String),
    sa.Column('created_at', sa.TIMESTAMP),
)

erc20_balance_requests_t = sa.Table(
    'erc20_balance_requests',
    metadata,
    sa.Column('token_address', sa.String),
    sa.Column('account_address', sa.String),
    sa.Column('block_number', sa.Integer),
    sa.Column('balance', postgresql.NUMERIC()),
)

TABLES = (
    blocks_t,
    uncles_t,
    transactions_t,
    receipts_t,
    logs_t,
    accounts_state_t,
    accounts_base_t,
    token_holders_t,
    internal_transactions_t,
    pending_transactions_t,
    reorgs_t,
    token_transfers_t,
    chain_splits_t,
    assets_transfers_t,
    assets_summary_t,
    wallet_events_t,
    chain_events_t,
    erc20_balance_requests_t
)
