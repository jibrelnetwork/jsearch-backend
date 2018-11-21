import asyncio
import logging
import re
import os
import time
import json
from datetime import datetime
from collections import OrderedDict

import aiopg
from aiopg.sa import create_engine
from psycopg2.extras import DictCursor
import sqlalchemy as sa
from sqlalchemy.sql import select, update
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_
import aiohttp
import psycopg2
from psycopg2.extras import execute_values
import requests

from jsearch.common.tables import *
from jsearch.common import contracts
from jsearch.common import tasks
from jsearch import settings


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

    def connect(self):
        self.engine = sa.create_engine(self.connection_string, poolclass=NullPool)
        self.conn = self.engine.connect()

    def disconnect(self):
        self.conn.close()

    def update_logs(self, conn, logs):
        for rec in logs:
            query = logs_t.update().\
                where(and_(logs_t.c.transaction_hash == rec['transaction_hash'],
                           logs_t.c.log_index == rec['log_index'])).\
                values(**rec)
            conn.execute(query)

    def process_logs(self, contract, logs):
        contracts_cache = {}
        if contract is not None:
            contracts_cache[contract['address']] = contract
        for log in logs:
            try:
                if log['address'] in contracts_cache:
                    log_contract = contracts_cache[log['address']]
                else:
                    log_contract = self.get_contract(log['address'])
                    if log_contract is None:
                        log['event_type'] = None
                        log['event_args'] = None
                        continue
                    contracts_cache[log['address']] = log_contract
                abi = log_contract['abi']
                event = contracts.decode_event(abi, log)
            except Exception as e:
                # ValueError: Unknown log type
                logger.debug('Log decode error: <%s>\n ', log)
                log['event_type'] = None
                log['event_args'] = None
            else:
                event_type = event.pop('_event_type')
                log['event_type'] = event_type
                log['event_args'] = event
                if event_type == 'Transfer':
                    # TODO: maybe move this to process_transaction?
                    args_list = []
                    # some contracts (for example 0xaae81c0194d6459f320b70ca0cedf88e11a242ce) may have
                    # several Transfer events with different signatures, so we try to find ERS20 copilent event (with 3 args)
                    event_inputs = [i['inputs'] for i in abi
                                    if i.get('name') == event_type and
                                    i['type'] == 'event' and len(i['inputs']) == 3][0]
                    for i in event_inputs:
                        args_list.append(event[i['name']])
                    token_address = log['address']
                    to_address = args_list[1]
                    from_address = args_list[0]
                    block_number = log.get('block_number') or int(log['blockNumber'], 16)  # FIXME
                    tasks.update_token_holder_balance_task.delay(token_address, to_address, block_number)
                    tasks.update_token_holder_balance_task.delay(token_address, from_address, block_number)
        return logs

    def process_transaction(self, contract, tx_data, logs):
        if contract is not None:
            transfer_events = [l for l in logs if l['event_type'] == 'Transfer']
            if len(transfer_events) > 1:
                logger.warn('Multiple transfer events at %s', tx_data['hash'])
            try:
                call = contracts.decode_contract_call(contract['abi'], tx_data['input'])
            except Exception as e:
                logger.exception('Call decode error: <%s>', tx_data)
            else:
                if call:
                    tx_data['contract_call_description'] = call
                    if call['function'] in {'transfer', 'transferFrom'} and transfer_events:
                        tx_data['is_token_transfer'] = True
                        tx_data['token_transfer_to'] = call['args'][-2]
                        if call['function'] == 'transferFrom':
                            tx_data['token_transfer_from'] = call['args'][0]
                        else:
                            tx_data['token_transfer_from'] = tx_data['from']
                        if contract['token_decimals']:
                            tx_data['token_amount'] = call['args'][-1] / (10 ** contract['token_decimals'])
        elif tx_data['to'] == contracts.NULL_ADDRESS:
            # contract creation, check Transfer events
            pass
        return tx_data

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

    def get_transaction_logs(self, conn, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return conn.execute(q).fetchall()

    def process_token_transfers(self, tx_hash):
        logger.info('Processing token transfer for TX %s', tx_hash)
        q = select([transactions_t]).where(transactions_t.c.hash == tx_hash)
        tx = self.conn.execute(q).fetchone()
        if tx is None:
            logger.warn('TX with hash %s not found', tx_hash)
            return
        with self.engine.begin() as conn:
            logs = self.get_transaction_logs(conn, tx['hash'])
            if len(logs) == 0:
                logger.info('No logs - no transfers')
                return
            if tx['to'] == contracts.NULL_ADDRESS:
                contract_address = logs[0]['address']
            else:
                contract_address = tx['to']
            contract = self.get_contract(contract_address)
            logs = self.process_logs(contract, [dict(l) for l in logs])
            tx_data = dict(tx)
            data = self.process_transaction(contract, tx_data, logs)
            query = update(transactions_t).where(transactions_t.c.hash == tx_hash).values(**data)
            conn.execute(query)
            self.update_logs(conn, logs)

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
    db.connect()
    return db


def get_engine():
    db = sa.create_engine(settings.JSEARCH_MAIN_DB)
    db.connect()
    return db


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
