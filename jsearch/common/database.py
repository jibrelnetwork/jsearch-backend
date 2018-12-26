import logging

import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import and_, false, create_engine as sync_create_engine, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Engine as SyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from jsearch import settings
from jsearch.common.tables import transactions_t, logs_t, token_holders_t
from jsearch.common.utils import as_dicts

MAIN_DB_POOL_SIZE = 22

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """
    Any problem with database operation
    """


class ConnectionError(DatabaseError):
    """
    Any problem with database connection
    """


class DBWrapperSync:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

        if exc_type:
            return False


class MainDBSync(DBWrapperSync):
    engine: SyncEngine

    def connect(self):
        self.engine = sync_create_engine(self.connection_string, poolclass=NullPool)
        self.conn = self.engine.connect()

    def disconnect(self):
        self.conn.close()

    def update_log(self, record, conn=None):
        conn = self.conn or conn

        query = logs_t.update(). \
            where(and_(logs_t.c.transaction_hash == record['transaction_hash'],
                       logs_t.c.block_hash == record['block_hash'],
                       logs_t.c.log_index == record['log_index'])). \
            values(**record)
        conn.execute(query)

    @as_dicts
    def get_transaction_logs(self, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return self.conn.execute(q).fetchall()

    @as_dicts
    def get_logs_to_process_events(self, limit=1000):
        unprocessed_blocks_query = select(
            columns=[logs_t.c.is_processed, logs_t.c.block_number],
            whereclause=logs_t.c.is_processed == false(),
        ) \
            .order_by(logs_t.c.is_processed.asc(), logs_t.c.block_number.asc()) \
            .limit(limit)
        unprocessed_blocks = {row[1] for row in self.conn.execute(unprocessed_blocks_query).fetchall()}

        query = select(
            columns=[logs_t],
            whereclause=and_(
                logs_t.c.is_processed == false(),
                logs_t.c.block_number.in_(unprocessed_blocks)
            )
        ) \
            .order_by(logs_t.c.block_number.asc()) \
            .limit(limit)
        return self.conn.execute(query).fetchall()

    @as_dicts
    def get_logs_to_process_operations(self, limit=1000):
        unprocessed_blocks_query = select(
            columns=[logs_t.c.is_token_transfer, logs_t.c.is_transfer_processed, logs_t.c.block_number],
            whereclause=and_(
                logs_t.c.is_token_transfer == true(),
                logs_t.c.is_transfer_processed == false()
            ),
        ) \
            .order_by(logs_t.c.is_token_transfer.asc(),
                      logs_t.c.is_transfer_processed.asc(),
                      logs_t.c.block_number.asc()) \
            .limit(limit)
        unprocessed_blocks = {row[2] for row in self.conn.execute(unprocessed_blocks_query).fetchall()}

        query = select(
            columns=[logs_t],
            whereclause=and_(
                logs_t.c.is_token_transfer == true(),
                logs_t.c.is_transfer_processed == false(),
                logs_t.c.block_number.in_(unprocessed_blocks),
            )
        ) \
            .order_by(logs_t.c.block_number.asc()) \
            .limit(limit)
        return self.conn.execute(query).fetchall()

    def get_contract_transactions(self, address):
        q = select([transactions_t]).where(transactions_t.c.to == address)
        return self.conn.execute(q).fetchall()

    def reset_processing_on_logs(self, contract_address):
        """
        Activate pipeline:
            - jsearch-post-processing events (decode events)
            - jsearch-post-processing operations (apply update of balance token holders)
        """
        query = f"UPDATE logs SET is_processed = false WHERE address = '{contract_address}';"
        self.conn.execute(query)

    def update_token_holder_balance(self, token_address, account_address, balance):
        insert_query = insert(token_holders_t).values(
            token_address=token_address,
            account_address=account_address,
            balance=balance)
        do_update_query = insert_query.on_conflict_do_update(
            index_elements=['token_address', 'account_address'],
            set_=dict(balance=balance)
        )
        self.conn.execute(do_update_query)


def get_main_db():
    db = MainDBSync(settings.JSEARCH_MAIN_DB)
    return db
