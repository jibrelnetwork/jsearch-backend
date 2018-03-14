import sqlalchemy as sa
import sqlalchemy.types as types
from sqlalchemy.dialects import postgresql


class HexInteger(types.TypeDecorator):
    """
    Converts hex string to HexInteger
    """

    impl = types.Integer

    def process_bind_param(self, value, dialect):
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)


class HexBigInteger(types.TypeDecorator):
    """
    Converts hex string to HexInteger
    """

    impl = types.BigInteger

    def process_bind_param(self, value, dialect):
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)


blocks_t = sa.Table(
    'blocks', sa.MetaData(),
    sa.Column('difficulty', HexInteger),
    sa.Column('extra_data', sa.String),
    sa.Column('gas_limit', HexInteger),
    sa.Column('gas_used', HexInteger),
    sa.Column('hash', sa.String, primary_key=True),
    sa.Column('logs_bloom', sa.String),
    sa.Column('miner', sa.String),
    sa.Column('mix_hash', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('number', HexInteger),
    sa.Column('parent_hash', sa.String),
    sa.Column('receipts_root', sa.String),
    sa.Column('sha3_uncles', sa.String),
    sa.Column('size', HexInteger),
    sa.Column('state_root', sa.String),
    sa.Column('timestamp', HexInteger),
    sa.Column('total_difficulty', HexBigInteger),
    sa.Column('transactions_root', sa.String),
    sa.Column('is_sequence_sync', sa.Boolean)
)

uncles_t = sa.Table(
    'uncles', sa.MetaData(),
    sa.Column('difficulty', HexInteger),
    sa.Column('extra_data', sa.String),
    sa.Column('gas_limit', HexInteger),
    sa.Column('gas_used', HexInteger),
    sa.Column('hash', sa.String, primary_key=True),
    sa.Column('logs_bloom', sa.String),
    sa.Column('miner', sa.String),
    sa.Column('mix_hash', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('number', HexInteger),
    sa.Column('parent_hash', sa.String),
    sa.Column('receipts_root', sa.String),
    sa.Column('sha3_uncles', sa.String),
    sa.Column('size', HexInteger),
    sa.Column('state_root', sa.String),
    sa.Column('timestamp', HexInteger),
    sa.Column('total_difficulty', HexBigInteger),
    sa.Column('transactions_root', sa.String),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', HexInteger)
)


transactions_t = sa.Table(
    'transactions', sa.MetaData(),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', HexInteger),
    sa.Column('from', sa.String),
    sa.Column('gas', sa.String),
    sa.Column('gas_price', sa.String),
    sa.Column('hash', sa.String),
    sa.Column('input', sa.String),
    sa.Column('nonce', sa.String),
    sa.Column('r', sa.String),
    sa.Column('s', sa.String),
    sa.Column('to', sa.String),
    sa.Column('transaction_index', HexInteger),
    sa.Column('v', sa.String),
    sa.Column('value', sa.String),
)


receipts_t = sa.Table(
    'receipts', sa.MetaData(),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', HexInteger),
    sa.Column('contract_address', sa.String),
    sa.Column('cumulative_gas_used', HexInteger),
    sa.Column('from', sa.String),
    sa.Column('gas_used', HexInteger),
    sa.Column('logs_bloom', sa.String),
    sa.Column('root', sa.String),
    sa.Column('to', sa.String),
    sa.Column('transaction_hash', sa.String),
    sa.Column('transaction_index', HexInteger),
    sa.Column('status', HexInteger),
)


logs_t = sa.Table(
    'logs', sa.MetaData(),
    sa.Column('address', sa.String),
    sa.Column('block_hash', sa.String),
    sa.Column('block_number', HexInteger),
    sa.Column('data', sa.String),
    sa.Column('log_index', HexInteger),
    sa.Column('removed', sa.String),
    sa.Column('topics', postgresql.ARRAY(sa.String)),
    sa.Column('transaction_hash', sa.String),
    sa.Column('transaction_index', HexInteger),
)


accounts_t = sa.Table(
    'accounts', sa.MetaData(),
    sa.Column('block_number', HexInteger),
    sa.Column('block_hash', sa.String),
    sa.Column('address', sa.String),
    sa.Column('nonce', HexInteger),
    sa.Column('code', sa.String),
    sa.Column('code_hash', sa.String),
    sa.Column('balance', postgresql.NUMERIC()),
    sa.Column('root', sa.String),
    sa.Column('storage', postgresql.JSONB),
)