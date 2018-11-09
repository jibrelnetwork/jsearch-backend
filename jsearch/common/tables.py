import sqlalchemy as sa
import sqlalchemy.types as types
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import CheckConstraint


Base = declarative_base()


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


class Uncle(Base):
    __tablename__ = 'uncles'

    hash = sa.Column('hash', sa.String, primary_key=True)
    number = sa.Column('number', HexInteger, primary_key=True)

    block_number = sa.Column('block_number', HexInteger, index=True)
    block_hash = sa.Column('block_hash', sa.String)
    parent_hash = sa.Column('parent_hash', sa.String)
    difficulty = sa.Column('difficulty', HexBigInteger)
    extra_data = sa.Column('extra_data', sa.String)
    gas_limit = sa.Column('gas_limit', HexBigInteger)
    gas_used = sa.Column('gas_used', HexBigInteger)
    logs_bloom = sa.Column('logs_bloom', sa.String)
    miner = sa.Column('miner', sa.String)
    mix_hash = sa.Column('mix_hash', sa.String)
    nonce = sa.Column('nonce', sa.String)
    receipts_root = sa.Column('receipts_root', sa.String)
    sha3_uncles = sa.Column('sha3_uncles', sa.String)
    size = sa.Column('size', HexInteger)
    state_root = sa.Column('state_root', sa.String)
    timestamp = sa.Column('timestamp', HexInteger)
    total_difficulty = sa.Column('total_difficulty', HexBigInteger)
    transactions_root = sa.Column('transactions_root', sa.String)
    reward = sa.Column('reward', postgresql.NUMERIC(32, 0))


class InternalTransaction(Base):
    __tablename__ = 'internal_transactions'

    block_number = sa.Column('block_number', HexInteger, primary_key=True)
    parent_tx_hash = sa.Column('parent_tx_hash', sa.String, primary_key=True)
    op = sa.Column('op', sa.String)
    call_depth = sa.Column('call_depth', HexInteger)
    timestamp = sa.Column('timestamp', HexInteger)
    from_addr = sa.Column('from', sa.String)
    to_addr = sa.Column('to', sa.String)
    value = sa.Column('value', postgresql.NUMERIC(32, 0))
    gas_limit = sa.Column('gas_limit', HexBigInteger)
    payload = sa.Column('payload', sa.String)
    status = sa.Column('status', sa.String)
    transaction_index = sa.Column('transaction_index', sa.Integer, primary_key=True)


class Transaction(Base):
    __tablename__ = 'transactions'

    hash = sa.Column('hash', sa.String, primary_key=True)

    block_number = sa.Column('block_number', HexInteger, index=True)
    block_hash = sa.Column('block_hash', sa.String)
    transaction_index = sa.Column('transaction_index', HexInteger)
    from_addr = sa.Column('from', sa.String, index=True)
    to_addr = sa.Column('to', sa.String, index=True)
    gas = sa.Column('gas', sa.String)
    gas_price = sa.Column('gas_price', sa.String)
    input = sa.Column('input', sa.String)
    nonce = sa.Column('nonce', sa.String)
    r = sa.Column('r', sa.String)
    s = sa.Column('s', sa.String)
    v = sa.Column('v', sa.String)
    value = sa.Column('value', sa.String)

    is_token_transfer = sa.Column('is_token_transfer', sa.Boolean, default=False)
    contract_call_description = sa.Column('contract_call_description', postgresql.JSONB)
    token_amount = sa.Column('token_amount', postgresql.NUMERIC())
    token_transfer_from = sa.Column('token_transfer_from', sa.String, index=True)
    token_transfer_to = sa.Column('token_transfer_to', sa.String, index=True)


class Receipt(Base):
    __tablename__ = 'receipts'

    transaction_hash = sa.Column('transaction_hash', sa.String, primary_key=True)
    block_number = sa.Column('block_number', HexInteger, index=True)
    block_hash = sa.Column('block_hash', sa.String)
    contract_address = sa.Column('contract_address', sa.String, index=True)
    cumulative_gas_used = sa.Column('cumulative_gas_used', HexInteger)
    from_addr = sa.Column('from', sa.String)
    to_addr = sa.Column('to', sa.String)
    gas_used = sa.Column('gas_used', HexInteger)
    logs_bloom = sa.Column('logs_bloom', sa.String)
    root = sa.Column('root', sa.String)
    transaction_index = sa.Column('transaction_index', HexInteger)
    status = sa.Column('status', HexInteger)


class Log(Base):
    __tablename__ = 'logs'

    transaction_hash = sa.Column('transaction_hash', sa.String, primary_key=True)
    block_number = sa.Column('block_number', HexInteger)
    block_hash = sa.Column('block_hash', sa.String)
    log_index = sa.Column('log_index', HexInteger, primary_key=True)
    address = sa.Column('address', sa.String)
    data = sa.Column('data', sa.String)
    removed = sa.Column('removed', sa.Boolean)
    topics = sa.Column('topics', postgresql.ARRAY(sa.String))
    transaction_index = sa.Column('transaction_index', HexInteger)
    event_type = sa.Column('event_type', sa.String)
    event_args = sa.Column('event_args', postgresql.JSONB)

    is_token_transfer = sa.Column('is_token_transfer', sa.Boolean, default=False)
    token_amount = sa.Column('token_amount', postgresql.NUMERIC())
    token_transfer_from = sa.Column('token_transfer_from', sa.String, index=True)
    token_transfer_to = sa.Column('token_transfer_to', sa.String, index=True)


class Account(Base):
    __tablename__ = 'accounts'

    block_number = sa.Column('block_number', HexInteger, primary_key=True)
    block_hash = sa.Column('block_hash', sa.String)
    address = sa.Column('address', sa.String, primary_key=True)
    nonce = sa.Column('nonce', HexInteger)
    code = sa.Column('code', sa.String)
    code_hash = sa.Column('code_hash', sa.String)
    balance = sa.Column('balance', postgresql.NUMERIC(32, 0))
    root = sa.Column('root', sa.String)
    storage = sa.Column('storage', postgresql.JSONB)


class MinedBlock(Base):
    __tablename__ = 'mined_blocks'

    block_number = sa.Column('block_number', HexInteger, primary_key=True)
    miner = sa.Column('miner', sa.String, primary_key=True)
    reward = sa.Column('value', sa.String)
    timestamp = sa.Column('timestamp', HexInteger)


class MinedUncle(Base):
    __tablename__ = 'mined_uncles'

    block_number = sa.Column('block_number', HexInteger, primary_key=True)
    miner = sa.Column('miner', sa.String, primary_key=True)
    reward = sa.Column('value', sa.String)
    timestamp = sa.Column('timestamp', HexInteger)


class Block(Base):
    __tablename__ = 'blocks'

    number = sa.Column('number', HexInteger, primary_key=True)
    hash = sa.Column('hash', sa.String, index=True)
    parent_hash = sa.Column('parent_hash', sa.String)
    difficulty = sa.Column('difficulty', HexBigInteger)
    extra_data = sa.Column('extra_data', sa.String)
    gas_limit = sa.Column('gas_limit', HexBigInteger)
    gas_used = sa.Column('gas_used', HexBigInteger)
    logs_bloom = sa.Column('logs_bloom', sa.String)
    miner = sa.Column('miner', sa.String, index=True)
    mix_hash = sa.Column('mix_hash', sa.String)
    nonce = sa.Column('nonce', sa.String)
    receipts_root = sa.Column('receipts_root', sa.String)
    sha3_uncles = sa.Column('sha3_uncles', sa.String)
    size = sa.Column('size', HexInteger)
    state_root = sa.Column('state_root', sa.String)
    timestamp = sa.Column('timestamp', HexInteger)
    total_difficulty = sa.Column('total_difficulty', HexBigInteger)
    transactions_root = sa.Column('transactions_root', sa.String)
    is_sequence_sync = sa.Column('is_sequence_sync', sa.Boolean)
    static_reward = sa.Column('static_reward', postgresql.NUMERIC(32, 0))
    uncle_inclusion_reward = sa.Column('uncle_inclusion_reward', postgresql.NUMERIC(32, 0))
    tx_fees = sa.Column('tx_fees', postgresql.NUMERIC(32, 0))


class Contract(Base):
    __tablename__ = 'contracts'

    address = sa.Column('address', sa.String, primary_key=True)
    name = sa.Column('name', sa.String)
    byte_code = sa.Column('byte_code', sa.Text)
    source_code = sa.Column('source_code', sa.Text)
    abi = sa.Column('abi', postgresql.JSONB)
    compiler_version = sa.Column('compiler_version', sa.String)
    optimization_enabled = sa.Column('optimization_enabled', sa.Boolean)
    optimization_runs = sa.Column('optimization_runs', sa.Integer)
    constructor_args = sa.Column('constructor_args', sa.String)
    metadata_hash = sa.Column('metadata_hash', sa.String)

    is_erc20_token = sa.Column('is_erc20_token', sa.Boolean)
    token_name = sa.Column('token_name', sa.String)
    token_symbol = sa.Column('token_symbol', sa.String)
    token_decimals = sa.Column('token_decimals', sa.Integer)
    token_total_supply = sa.Column('token_total_supply', postgresql.NUMERIC(32, 0))

    grabbed_at = sa.Column(sa.DateTime)
    verified_at = sa.Column(sa.DateTime)


class TokenHolder(Base):
    __tablename__ = 'token_holders'
    __table_args__ = (CheckConstraint('balance >= 0',
                                      name='check_balance_positive'),)

    account_address = sa.Column('account_address', sa.String, primary_key=True)
    token_address = sa.Column('token_address', sa.String, primary_key=True)
    balance = sa.Column('balance', postgresql.NUMERIC())


blocks_t = Block.__table__
uncles_t = Uncle.__table__
transactions_t = Transaction.__table__
receipts_t = Receipt.__table__
logs_t = Log.__table__
accounts_t = Account.__table__
contracts_t = Contract.__table__
token_holders_t = TokenHolder.__table__
internal_transactions_t = InternalTransaction.__table__
