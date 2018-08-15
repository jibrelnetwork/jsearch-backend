import asyncio
import logging
import re
import os
import time
import json
from datetime import datetime
from collections import OrderedDict

import asyncpg
from asyncpgsa import pg
import sqlalchemy as sa
from sqlalchemy.sql import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_

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


class LoggingConnection(asyncpg.connection.Connection):
    """
    Connection subclass with query logging
    """

    async def _execute(self, query, args, limit, timeout, return_status=False):
        start_time = time.monotonic()
        res = await super()._execute(query, args, limit, timeout, return_status)
        query_time = time.monotonic() - start_time
        logger.debug("%s params %s [%s]", query, args, query_time)
        return res


class DBWrapper:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            self.connection_string, connection_class=LoggingConnection)

    async def disconnect(self):
        await self.pool.close()


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """
    async def get_blocks_to_sync(self, start_block_num=0, chunk_size=10):
        q = """SELECT * FROM headers WHERE block_number BETWEEN $1 AND $2"""
        end_num = start_block_num + chunk_size - 1
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(q, start_block_num, end_num)
        return rows

    async def get_header_by_hash(self, block_number):
        q = """SELECT * FROM headers WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(q, block_number)
        return row

    async def get_block_accounts(self, block_number):
        q = """SELECT * FROM accounts WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(q, block_number)
        return rows

    async def get_block_body(self, block_number):
        q = """SELECT * FROM bodies WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetchrow(q, block_number)
        return rows

    async def get_block_receipts(self, block_number):
        q = """SELECT * FROM receipts WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(q, block_number)
        return row

    async def get_reward(self, block_number):
        q = """SELECT * FROM rewards WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(q, block_number)
        if len(rows) > 1:
            for r in rows:
                if r['address'] != contracts.NULL_ADDRESS:
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    async def get_internal_transactions(self, block_number):
        q = """SELECT * FROM internal_transactions WHERE block_number=$1"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(q, block_number)
        return rows


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    async def connect(self):
        await pg.init(self.connection_string, max_size=MAIN_DB_POOL_SIZE)
        self.pool = await asyncpg.create_pool(self.connection_string, max_size=MAIN_DB_POOL_SIZE)
        self.conn = pg

    async def disconnect(self):
        await pg.pool.close()

    def call_sync(self, coro):
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    async def is_block_exist(self, block_number):
        q = """SELECT number from blocks WHERE number=$1"""
        row = await pg.fetchrow(q, block_number)
        return row['number'] == block_number if row else False

    async def get_latest_sequence_synced_block_number(self):
        """
        Get latest block writed in main DB during sequence sync
        """
        q = """SELECT l.number + 1 as start 
                FROM blocks as l 
                LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number 
                WHERE r.number IS NULL"""
        row = await pg.fetchrow(q)
        return row['start'] - 1 if row else None

    async def write_block(self, header, uncles, transactions, receipts,
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
            reward_data = json.loads(reward['fields'])
            block_reward = {
                'static_reward': reward_data['BlockReward'],
                'uncle_inclusion_reward': reward_data['UncleInclusionReward'],
                'tx_fees': reward_data['TxsReward']
            }
            uncles_rewards = reward_data['Uncles']

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await self.insert_header(conn, header, block_reward)
                await self.insert_uncles(conn, block_number, block_hash, uncles, uncles_rewards)
                await self.insert_transactions_and_receipts(conn, block_number, block_hash, receipts, transactions)
                await self.insert_accounts(conn, block_number, block_hash, accounts)
                await self.insert_internal_transactions(conn, block_number, block_hash, internal_transactions)

    async def insert_header(self, conn, header, reward):
        data = dict_keys_case_convert(json.loads(header['fields']))
        data.update(reward)
        q = make_insert_query('blocks', data.keys())
        hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
        await conn.execute(q, *list(data.values()))

    async def insert_uncles(self, conn, block_number, block_hash, uncles, reward):
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
        q = make_insert_query('uncles', items[0].keys())
        await conn.executemany(q, [list(i.values()) for i in items])

    async def insert_transactions_and_receipts(self, conn, block_number, block_hash, receipts, transactions):
        if not transactions:
            return
        rdata = json.loads(receipts['fields'])['Receipts'] or []
        tx_items = []
        recpt_items = []
        logs_items = []
        for i, receipt in enumerate(rdata):
            recpt_data = dict_keys_case_convert(receipt)
            tx = transactions[i]
            assert tx['hash'] == recpt_data['transaction_hash']
            recpt_data['transaction_hash'] = tx['hash']
            recpt_data['transaction_index'] = i
            if tx['to'] is None:
                tx['to'] == contracts.NULL_ADDRESS
            recpt_data['to'] = tx['to']
            recpt_data['from'] = tx['from']
            logs = recpt_data.pop('logs') or []
            recpt_data['block_hash'] = block_hash
            recpt_data['block_number'] = block_number
            hex_vals_to_int(recpt_data, ['cumulative_gas_used', 'gas_used', 'status'])
            recpt_items.append(recpt_data)

            tx_data = dict_keys_case_convert(tx)
            tx_data['is_token_transfer'] = False
            tx_data['contract_call_description'] = None
            tx_data['token_amount'] = None
            tx_data['token_transfer_from'] = None
            tx_data['token_transfer_to'] = None
            tx_data['transaction_index'] = i
            tx_data['block_hash'] = block_hash
            tx_data['block_number'] = block_number
            if tx['to'] == contracts.NULL_ADDRESS:
                contract_address = data['contract_address']
            else:
                contract_address = tx['to']
            contract = await self.get_contract(contract_address)
            logs = await self.process_logs(conn, contract, logs)
            logs_items.extend(logs)
            tx_data = self.process_transaction(contract, tx_data, logs)

            tx_items.append(tx_data)

        q = make_insert_query('receipts', recpt_items[0].keys())
        await conn.executemany(q, [list(i.values()) for i in recpt_items])

        tx_keys = tx_items[0].keys()
        q = make_insert_query('transactions', tx_keys)
        await conn.executemany(q, [[i[k] for k in tx_keys] for i in tx_items])

        await self.insert_logs(conn, block_number, block_hash, logs_items)

    async def insert_logs(self, conn, block_number, block_hash, logs):
        items = []
        if not logs:
            return
        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            hex_vals_to_int(data, ['log_index', 'transaction_index', 'block_number'])
            data['event_args'] = json.dumps(data['event_args'])
            items.append(data)
        q = make_insert_query('logs', items[0].keys())
        await conn.executemany(q, [list(i.values()) for i in items])

    async def update_logs(self, conn, logs):
        for rec in logs:
            query = logs_t.update().\
                where(and_(logs_t.c.transaction_hash == rec['transaction_hash'],
                           logs_t.c.log_index == rec['log_index'])).\
                values(**rec)
            await conn.execute(query)

    async def insert_accounts(self, conn, block_number, block_hash, accounts):
        items = []
        for account in accounts:
            data = dict_keys_case_convert(json.loads(account['fields']))
            data['storage'] = None  # FIXME!!!
            data['address'] = account['address'].lower()
            data['block_number'] = block_number
            data['block_hash'] = block_hash
            items.append(data)
        q = make_insert_query('accounts', items[0].keys())
        await conn.executemany(q, [list(i.values()) for i in items])

    async def insert_internal_transactions(self, conn, block_number, block_hash, internal_transactions):
        items = []
        if not internal_transactions:
            return
        for i, tx in enumerate(internal_transactions, 1):
            data = dict_keys_case_convert(json.loads(tx['fields']))
            data['timestamp'] = data.pop('time_stamp')
            data['transaction_index'] = i
            del data['operation']
            data['op'] = data['type']
        q = make_insert_query('internal_transactions', items[0].keys())
        await conn.executemany(q, [list(i.values()) for i in items])

    async def process_logs(self, conn, contract, logs):
        contracts_cache = {}
        if contract is not None:
            contracts_cache[contract['address']] = contract
        for log in logs:
            try:
                if log['address'] in contracts_cache:
                    log_contract = contracts_cache[log['address']]
                else:
                    log_contract = await self.get_contract(log['address'])
                    if log_contract is None:
                        log['event_type'] = None
                        log['event_args'] = None
                        continue
                    contracts_cache[log['address']] = log_contract
                abi = json.loads(log_contract['abi'])
                event = contracts.decode_event(abi, log)
            except Exception as e:
                # ValueError: Unknown log type
                logger.exception('Log decode error: <%s>', log)
                log['event_type'] = None
                log['event_args'] = None
            else:
                event_type = event.pop('_event_type')
                log['event_type'] = event_type
                log['event_args'] = event
                if event_type == 'Transfer':
                    args_list = []
                    event_inputs = [i['inputs'] for i in abi
                                    if i.get('name') == event_type and
                                    i['type'] == 'event'][0]
                    for i in event_inputs:
                        args_list.append(event[i['name']])
                    token_address = log['address']
                    to_address = args_list[1]
                    from_address = args_list[0]
                    block_number = log.get('block_number') or int(log['blockNumber'], 16)  # FIXME
                    tasks.update_token_holder_balance.delay(token_address, to_address, block_number)
                    tasks.update_token_holder_balance.delay(token_address, from_address, block_number)
        return logs

    def process_transaction(self, contract, tx_data, logs):
        if contract is not None:
            transfer_events = [l for l in logs if l['event_type'] == 'Transfer']
            if len(transfer_events) > 1:
                logger.warn('Multiple transfer events at %s', tx_data['hash'])
            try:
                call = contracts.decode_contract_call(json.loads(contract['abi']), tx_data['input'])
            except Exception as e:
                logger.exception('Call decode error: <%s>', tx_data)
            else:
                if call:
                    tx_data['contract_call_description'] = json.dumps(call)
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

    async def get_contract(self, address):
        q = select([contracts_t]).where(contracts_t.c.address == address)
        row = await pg.fetchrow(q)
        logger.debug('get_contract %s: %s', address, bool(row))
        return row

    async def update_contract(self, address, data):
        q = contracts_t.update().where(
            contracts_t.c.address == address).values(**data)
        res = await self.conn.execute(q)

    async def get_transaction_logs(self, conn, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return await conn.fetch(q)

    async def save_verified_contract(self, address, contract_creation_code, source_code, contract_name, abi, compiler,
                                     optimization_enabled, mhash, constructor_args, is_erc20_token):
        """
        address = sa.Column('address', sa.String, primary_key=True)
        name = sa.Column('name', sa.String)
        byte_code = sa.Column('byte_code', sa.Text)
        source_code = sa.Column('source_code', sa.Text)
        abi = sa.Column('abi', postgresql.JSONB)
        compiler_version = sa.Column('compiler_version', sa.String)
        optimization_enabled = sa.Column('optimization_enabled', sa.Boolean)
        optimization_runs = sa.Column('optimization_runs', sa.Integer)

        is_erc20_token = sa.Column('is_erc20_token', sa.Boolean)
        token_name = sa.Column('token_name', sa.String)
        token_symbol = sa.Column('token_symbol', sa.String)
        token_decimals = sa.Column('token_decimals', sa.Integer)
        token_total_supply = sa.Column('token_total_supply', postgresql.NUMERIC(32, 0))

        grabbed_at = sa.Column(sa.DateTime)
        verified_at = sa.Column(sa.DateTime)
        """
        query = contracts_t.insert().values(
            address=address,
            name=contract_name,
            byte_code=contract_creation_code,
            source_code=source_code,
            abi=abi,
            compiler_version=compiler,
            optimization_enabled=optimization_enabled,
            optimization_runs=200,
            constructor_args=constructor_args,
            metadata_hash=mhash,
            is_erc20_token=is_erc20_token,
            grabbed_at=None,
            verified_at=datetime.now()
        )
        logger.info('Saving verified contract at %s', address)
        async with pg.transaction() as conn:
            await conn.execute(query)

    async def get_contact_creation_code(self, address):
        q = select([transactions_t.c.input]).select_from(
            transactions_t.join(receipts_t, and_(receipts_t.c.transaction_hash == transactions_t.c.hash,
                                                 receipts_t.c.contract_address == address)))
        row = await pg.fetchrow(q)
        return row['input']

    async def process_token_transfer(self, tx_hash):
        logger.info('Processing token transfer for TX %s', tx_hash)
        q = select([transactions_t]).where(transactions_t.c.hash == tx_hash)
        async with pg.transaction() as conn:
            tx = await conn.fetchrow(q)
            if tx is None:
                logger.warn('TX with hash %s not found', tx_hash)
                return
            logs = await self.get_transaction_logs(conn, tx['hash'])
            if len(logs) == 0:
                logger.info('No logs - no transfers')
                return
            if tx['to'] == contracts.NULL_ADDRESS:
                contract_address = logs[0]['address']
            else:
                contract_address = tx['to']
            contract = await self.get_contract(contract_address)
            logs = await self.process_logs(conn, contract, [dict(l) for l in logs])
            tx_data = dict(tx)
            data = self.process_transaction(contract, tx_data, logs)
            query = update(transactions_t).where(transactions_t.c.hash == tx_hash).values(**data)
            await conn.execute(query)
            await self.update_logs(conn, logs)

    async def get_contract_transactions(self, address):
        q = select([transactions_t]).where(transactions_t.c.to == address)
        rows = await pg.fetch(q)
        return rows

    async def update_token_holder_balance(self, token_address, account_address, balance):
        insert_query = insert(token_holders_t).values(
            token_address=token_address,
            account_address=account_address,
            balance=balance)
        do_update_query = insert_query.on_conflict_do_update(
            index_elements=['token_address', 'account_address'],
            set_=dict(balance=balance)
        )
        await self.conn.execute(do_update_query)



first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}


def get_main_db():
    db = MainDB(settings.JSEARCH_MAIN_DB)
    db.call_sync(db.connect())
    return db


def make_insert_query(table_name, fields):
    q = """INSERT INTO {table} ({fields}) VALUES ({values})"""
    f = ','.join(['"{}"'.format(k) for k in fields])
    v = ','.join(['${}'.format(i) for i in range(1, len(fields) + 1)])
    return q.format(table=table_name, fields=f, values=v)


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
