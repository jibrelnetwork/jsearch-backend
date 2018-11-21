import logging
import re
from typing import Dict

import aiopg
import psycopg2
import requests
from aiopg.sa import create_engine, Engine as AsyncEngine
from psycopg2.extras import DictCursor
from sqlalchemy import and_, false
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Connection, Engine as SyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from jsearch import settings
from jsearch.common import contracts
from jsearch.common.processing.transactions import process_transaction
from jsearch.common.tables import *
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


class DBWrapper:
    connection_string: str
    params: Dict[any, any]
    conn: Connection

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    async def connect(self):
        self.conn = await aiopg.connect(
            self.connection_string)

    def disconnect(self):
        self.conn.close()


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


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """

    async def get_blocks_to_sync(self, start_block_num=0, end_block_num=None):
        q = """SELECT block_number FROM headers WHERE block_number BETWEEN %s AND %s"""
        async with self.conn.cursor() as cur:
            await cur.execute(q, [start_block_num, end_block_num])
            rows = await cur.fetchall()
        return rows


class RawDBSync(DBWrapperSync):
    def get_header_by_hash(self, block_number):
        q = """SELECT * FROM headers WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_block_accounts(self, block_number):
        q = """SELECT * FROM accounts WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        return rows

    def get_block_body(self, block_number):
        q = """SELECT * FROM bodies WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_block_receipts(self, block_number):
        q = """SELECT * FROM receipts WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_reward(self, block_number):
        q = """SELECT * FROM rewards WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        if len(rows) > 1:
            for r in rows:
                if r['address'] != contracts.NULL_ADDRESS:
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    def get_internal_transactions(self, block_number):
        q = """SELECT * FROM internal_transactions WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        return rows


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    engine: AsyncEngine

    async def connect(self):
        self.engine = await create_engine(self.connection_string, minsize=1, maxsize=MAIN_DB_POOL_SIZE)

    async def disconnect(self):
        self.engine.close()
        await self.engine.wait_closed()

    async def get_latest_sequence_synced_block_number(self, blocks_range):
        """
        Get latest block writed in main DB during sequence sync
        """
        if blocks_range[1] is None:
            condition = 'number >= %s'
            params = (blocks_range[0],)
        else:
            condition = 'number BETWEEN %s AND %s'
            params = blocks_range
        q = """SELECT l.number + 1 as start 
                FROM (SELECT * FROM blocks WHERE {cond}) as l 
                LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number
                WHERE r.number IS NULL order by start""".format(cond=condition)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            rows = await res.fetchall()
            row = rows[0] if len(rows) > 0 else None
        return row['start'] - 1 if row else None

    async def get_contact_creation_code(self, address):
        q = select([transactions_t.c.input]).select_from(
            transactions_t.join(receipts_t, and_(receipts_t.c.transaction_hash == transactions_t.c.hash,
                                                 receipts_t.c.contract_address == address)))
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
        return row['input']


class MainDBSync(DBWrapperSync):
    engine: SyncEngine

    def connect(self):
        self.engine = sa.create_engine(self.connection_string, poolclass=NullPool)
        self.conn = self.engine.connect()

    def disconnect(self):
        self.conn.close()

    def is_block_exist(self, block_number):
        q = """SELECT number from blocks WHERE number=%s"""
        row = self.conn.execute(q, [block_number]).fetchone()
        return row['number'] == block_number if row else False

    def write_block(self, header, uncles, transactions, receipts,
                    accounts, reward, internal_transactions):
        """
        Write block and all related items in main database
        """
        logger.debug('H: %s', header)
        logger.debug('U: %s', uncles)
        logger.debug('A: %s', accounts)
        logger.debug('T: %s', transactions)
        logger.debug('R: %s', receipts)
        logger.debug('RW: %s', reward)
        logger.debug('IT: %s', internal_transactions)

        block_number = header['block_number']
        block_hash = header['block_hash']

        if block_number == 0:
            block_reward = {'static_reward': 0, 'uncle_inclusion_reward': 0, 'tx_fees': 0}
            uncles_rewards = []
        else:
            reward_data = reward['fields']
            block_reward = {
                'static_reward': reward_data['BlockReward'],
                'uncle_inclusion_reward': reward_data['UncleInclusionReward'],
                'tx_fees': reward_data['TxsReward']
            }
            uncles_rewards = reward_data['Uncles']

        with self.engine.begin() as conn:
            self.insert_header(conn, header, block_reward)
            self.insert_uncles(conn, block_number, block_hash, uncles, uncles_rewards)
            self.insert_transactions_and_receipts(conn, block_number, block_hash, receipts, transactions)
            self.insert_accounts(conn, block_number, block_hash, accounts)
            self.insert_internal_transactions(conn, block_number, block_hash, internal_transactions)

    def insert_header(self, conn, header, reward):
        data = dict_keys_case_convert(header['fields'])
        data.update(reward)
        q = make_insert_query('blocks', data.keys())
        hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
        conn.execute(q, data)

    def insert_uncles(self, conn, block_number, block_hash, uncles, reward):
        if not uncles:
            return
        items = []
        for i, uncle in enumerate(uncles):
            rwd = reward[i]
            data = dict_keys_case_convert(uncle)
            assert rwd['UnclePosition'] == i
            data['reward'] = rwd['UncleReward']
            data['block_hash'] = block_hash
            data['block_number'] = block_number
            hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
            items.append(data)
        q, t = make_insert_query_batch('uncles', items[0].keys())
        conn.execute(uncles_t.insert(), *items)

    def insert_transactions_and_receipts(self, conn, block_number, block_hash, receipts, transactions):
        if not transactions:
            return
        rdata = receipts['fields']['Receipts'] or []
        tx_items = []
        recpt_items = []
        logs_items = []
        for i, receipt in enumerate(rdata):
            recpt_data = dict_keys_case_convert(receipt)

            tx = transactions[i]
            assert tx['hash'] == recpt_data['transaction_hash']

            if tx['to'] is None:
                tx['to'] = contracts.NULL_ADDRESS

            recpt_data.update({
                'transaction_hash': tx['hash'],
                'transaction_index': i,
                'to': tx['to'],
                'from': tx['from'],
                'block_hash': block_hash,
                'block_number': block_number
            })
            hex_vals_to_int(recpt_data, ['cumulative_gas_used', 'gas_used', 'status'])
            recpt_items.append(recpt_data)

            logs = recpt_data.pop('logs') or []
            logs_items.extend(logs)

            tx_data = dict_keys_case_convert(tx)
            tx_data.update(
                is_token_transfer=False,
                contract_call_description=None,
                token_amount=None,
                token_transfer_from=None,
                token_transfer_to=None,
                transaction_index=i,
                block_hash=block_hash,
                block_number=block_number
            )
            tx_data = process_transaction(tx_data=tx_data, receipt=receipt)

            tx_items.append(tx_data)

        conn.execute(receipts_t.insert(), *recpt_items)
        conn.execute(transactions_t.insert(), *tx_items)

        self.insert_logs(conn, logs_items)

    @staticmethod
    def insert_logs(conn, logs):
        items = []
        if not logs:
            return

        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            data = hex_vals_to_int(data, keys=['log_index', 'transaction_index', 'block_number'])
            data['event_args'] = data.get('event_args')
            items.append(data)

        conn.execute(logs_t.insert(), *items)

    @staticmethod
    def update_logs(conn, logs):
        for rec in logs:
            query = logs_t.update(). \
                where(and_(logs_t.c.transaction_hash == rec['transaction_hash'],
                           logs_t.c.log_index == rec['log_index'])). \
                values(**rec)
            conn.execute(query)

    def insert_accounts(self, conn, block_number, block_hash, accounts):
        items = []
        if not accounts:
            return

        for account in accounts:
            data = dict_keys_case_convert(account['fields'])
            data['storage'] = None  # FIXME!!!
            data['address'] = account['address'].lower()
            data['block_number'] = block_number
            data['block_hash'] = block_hash
            items.append(data)
        q, t = make_insert_query_batch('accounts', items[0].keys())
        conn.execute(accounts_t.insert(), *items)

    def insert_internal_transactions(self, conn, block_number, block_hash, internal_transactions):
        items = []
        if not internal_transactions:
            return
        for i, tx in enumerate(internal_transactions, 1):
            data = dict_keys_case_convert(tx['fields'])
            data['timestamp'] = data.pop('time_stamp')
            data['transaction_index'] = i
            del data['operation']
            data['op'] = tx['type']
            items.append(data)
        q, t = make_insert_query_batch('internal_transactions', items[0].keys())
        conn.execute(internal_transactions_t.insert(), *items)

    # @alru_cache(maxsize=1024)
    def get_contract(self, address):
        resp = requests.get(settings.JSEARCH_CONTRACTS_API + '/v1/contracts/{}'.format(address))
        if resp.status_code == 200:
            logger.debug('Got Contract %s: %s', address, resp.status_code)
            contract = resp.json()
            return contract
        logger.debug('Miss Contract %s: %s', address, resp.status_code)

    def update_contract(self, address, data):
        q = contracts_t.update().where(
            contracts_t.c.address == address).values(**data)
        res = self.conn.execute(q)

    @staticmethod
    @as_dicts
    def get_transaction_logs(conn, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return conn.execute(q).fetchall()

    @staticmethod
    @as_dicts
    def get_logs_for_post_processing(conn, limit=1000):
        query = select([logs_t])\
            .where(logs_t.c.is_processed == false())\
            .order_by(logs_t.c.block_number) \
            .limit(limit)
        return conn.execute(query).fetchall()

    def get_contract_transactions(self, address):
        q = select([transactions_t]).where(transactions_t.c.to == address)
        return self.conn.execute(q).fetchall()

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


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}


def get_main_db():
    db = MainDBSync(settings.JSEARCH_MAIN_DB)
    return db


def get_engine():
    db = sa.create_engine(settings.JSEARCH_MAIN_DB)
    db.connect()
    return db


def make_insert_query(table_name, fields):
    q = """INSERT INTO {table} ({fields}) VALUES ({values})"""
    f = ','.join(['"{}"'.format(k) for k in fields])
    v = ','.join(['%({})s'.format(k) for k in fields])
    return q.format(table=table_name, fields=f, values=v)


def make_insert_query_batch(table_name, fields):
    q = """INSERT INTO {table} ({fields}) VALUES %s"""
    f = ','.join(['"{}"'.format(k) for k in fields])
    v = ','.join(['%({})s'.format(k) for k in fields])
    return q.format(table=table_name, fields=f), '({})'.format(v)


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
    return d
