import sqlalchemy as sa
import sqlalchemy.types as types
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


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


class Uncle(Base):
    __tablename__ = 'uncles'

    hash = sa.Column('hash', sa.String, primary_key=True)
    number = sa.Column('number', HexInteger, primary_key=True)

    block_number = sa.Column('block_number', HexInteger)
    block_hash = sa.Column('block_hash', sa.String)
    parent_hash = sa.Column('parent_hash', sa.String)
    difficulty = sa.Column('difficulty', HexBigInteger)
    extra_data = sa.Column('extra_data', sa.String)
    gas_limit = sa.Column('gas_limit', HexInteger)
    gas_used = sa.Column('gas_used', HexInteger)
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


class InternalTransaction(Base):
    __tablename__ = 'internal_transactions'

    block_number = sa.Column('block_number', HexInteger, primary_key=True)
    parent_tx_hash = sa.Column('parent_tx_hash', HexInteger, primary_key=True)
    op = sa.Column('op', sa.String)
    depth = sa.Column('depth', HexInteger)
    timestamp = sa.Column('timestamp', HexInteger)
    from_addr = sa.Column('from', sa.String)
    to_addr = sa.Column('to', sa.String, primary_key=True)
    value = sa.Column('value', sa.String)
    gas_limit = sa.Column('gas_limit', HexInteger)


class Transaction(Base):
    __tablename__ = 'transactions'

    hash = sa.Column('hash', sa.String, primary_key=True)

    block_number = sa.Column('block_number', HexInteger)
    block_hash = sa.Column('block_hash', sa.String)
    transaction_index = sa.Column('transaction_index', HexInteger)
    from_addr = sa.Column('from', sa.String)
    to_addr = sa.Column('to', sa.String)
    gas = sa.Column('gas', sa.String)
    gas_price = sa.Column('gas_price', sa.String)
    input = sa.Column('input', sa.String)
    nonce = sa.Column('nonce', sa.String)
    r = sa.Column('r', sa.String)
    s = sa.Column('s', sa.String)
    v = sa.Column('v', sa.String)
    value = sa.Column('value', sa.String)


class Receipt(Base):
    __tablename__ = 'receipts'

    transaction_hash = sa.Column('transaction_hash', sa.String, primary_key=True)
    block_number = sa.Column('block_number', HexInteger)
    block_hash = sa.Column('block_hash', sa.String)
    contract_address = sa.Column('contract_address', sa.String)
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
    hash = sa.Column('hash', sa.String)
    parent_hash = sa.Column('parent_hash', sa.String)
    difficulty = sa.Column('difficulty', HexBigInteger)
    extra_data = sa.Column('extra_data', sa.String)
    gas_limit = sa.Column('gas_limit', HexInteger)
    gas_used = sa.Column('gas_used', HexInteger)
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
    is_sequence_sync = sa.Column('is_sequence_sync', sa.Boolean)


blocks_t = Block.__table__
uncles_t = Uncle.__table__
transactions_t = Transaction.__table__
receipts_t = Receipt.__table__
logs_t = Log.__table__
accounts_t = Account.__table__
