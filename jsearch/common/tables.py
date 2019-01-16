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
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, primary_key=True),
    sa.Column('parent_tx_hash', sa.String, primary_key=True),
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

transactions_t = sa.Table(
    'transactions',
    metadata,
    sa.Column('hash', sa.String, index=True),
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, index=True, primary_key=True),
    sa.Column('transaction_index', HexInteger, primary_key=True),
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
    sa.Column('transaction_hash', sa.String, primary_key=True),
    sa.Column('block_number', HexInteger, index=True),
    sa.Column('block_hash', sa.String, index=True, primary_key=True),
    sa.Column('log_index', HexInteger, primary_key=True),
    sa.Column('address', sa.String),
    sa.Column('data', sa.String),
    sa.Column('removed', sa.Boolean),
    sa.Column('topics', postgresql.ARRAY(sa.String)),
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
    sa.Column('account_address', sa.String, primary_key=True),
    sa.Column('token_address', sa.String, primary_key=True),
    sa.Column('balance', postgresql.NUMERIC(32, 0), index=True),
    sa.Column('decimals', sa.Integer, index=True),
)

reorgs_t = sa.Table(
    'reorgs',
    metadata,
    sa.Column('id', sa.BigInteger, primary_key=True),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', sa.Integer),
    sa.Column('reinserted', sa.Boolean),
    sa.Column('node_id', sa.String),
)

token_transfers_t = sa.Table(
    'token_transfers',
    metadata,
    sa.Column('address', sa.String),
    sa.Column('transaction_hash', sa.String),
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
    reorgs_t,
    token_transfers_t,
)
