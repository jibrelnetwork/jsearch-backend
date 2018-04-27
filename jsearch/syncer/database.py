import asyncio
import logging
import re
import time
import json

import asyncpg
from asyncpgsa import pg
import sqlalchemy as sa
from sqlalchemy.sql import select

from jsearch.common.tables import *
from jsearch.common import contract_utils


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

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        start_time = time.monotonic()
        res = await super().execute(query, args, timeout)
        query_time = time.monotonic() - start_time
        logger.debug("%s params %s [%s]", query, args, query_time)
        return res


class DBWrapper:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(
            self.connection_string, connection_class=LoggingConnection)

    async def disconnect(self):
        await self.conn.close()


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """
    async def get_blocks_to_sync(self, start_block_num=0, chunk_size=10):
        q = """SELECT * FROM headers WHERE block_number BETWEEN $1 AND $2"""
        end_num = start_block_num + chunk_size
        rows = await self.conn.fetch(q, start_block_num, end_num)
        return rows

    async def get_header_by_hash(self, block_number):
        q = """SELECT * FROM headers WHERE block_number=$1"""
        row = await self.conn.fetchrow(q, block_number)
        return row

    async def get_block_accounts(self, block_number):
        q = """SELECT * FROM accounts WHERE block_number=$1"""
        rows = await self.conn.fetch(q, block_number)
        return rows

    async def get_block_body(self, block_number):
        q = """SELECT * FROM bodies WHERE block_number=$1"""
        rows = await self.conn.fetchrow(q, block_number)
        return rows

    async def get_block_receipts(self, block_number):
        q = """SELECT * FROM receipts WHERE block_number=$1"""
        rows = await self.conn.fetchrow(q, block_number)
        return rows

    async def get_reward(self, block_number):
        q = """SELECT * FROM rewards WHERE block_number=$1"""
        rows = await self.conn.fetch(q, block_number)
        if len(rows) > 1:
            for r in rows:
                if r['address'] != '0x0000000000000000000000000000000000000000':
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    async def connect(self):
        await pg.init(self.connection_string)

    async def disconnect(self):
        pass

    async def get_latest_sequence_synced_block_number(self):
        """
        Get latest block writed in main DB during sequence sync
        """
        q = """SELECT max(number) as number FROM BLOCKS WHERE is_sequence_sync=true"""
        row = await pg.fetchrow(q)
        return row['number'] if row else None

    async def write_block(self, header, uncles, transactions, receipts, accounts, reward):
        """
        Write block and all related items in main database
        """
        logger.debug('H: %s', header)
        logger.debug('U: %s', uncles)
        logger.debug('A: %s', accounts)
        logger.debug('T: %s', transactions)
        logger.debug('R: %s', receipts)
        logger.debug('RW: %s', reward)

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

        async with pg.transaction() as conn:
            await self.insert_header(conn, header, block_reward)
            await self.insert_uncles(conn, block_number, block_hash, uncles, uncles_rewards)
            # await self.insert_transactions(conn, block_number, block_hash, transactions)
            await self.insert_transactions_and_receipts(conn, block_number, block_hash, receipts, transactions)
            await self.insert_accounts(conn, block_number, block_hash, accounts)

    async def insert_header(self, conn, header, reward):
        data = dict_keys_case_convert(json.loads(header['fields']))
        data.update(reward)
        query = blocks_t.insert().values(is_sequence_sync=True, **data)
        await conn.execute(query)

    async def insert_uncles(self, conn, block_number, block_hash, uncles, reward):
        for i, uncle in enumerate(uncles):
            rwd = reward[i]
            data = dict_keys_case_convert(uncle)
            assert rwd['UnclePosition'] == i
            data['reward'] = rwd['UncleReward']
            query = uncles_t.insert().values(block_number=block_number,
                                             block_hash=block_hash, **data)
            await conn.execute(query)

    # async def insert_transactions(self, conn, block_number, block_hash, transactions):
    #     for transaction in transactions:
    #         data = dict_keys_case_convert(transaction)
    #         data = await self.process_transaction(conn, data)
    #         query = transactions_t.insert().values(block_number=block_number,
    #                                                block_hash=block_hash, **data)
    #         await conn.execute(query)

    async def insert_transactions_and_receipts(self, conn, block_number, block_hash, receipts, transactions):
        rdata = json.loads(receipts['fields'])['Receipts'] or []
        for i, receipt in enumerate(rdata):
            data = dict_keys_case_convert(receipt)
            tx = transactions[i]
            assert tx['hash'] == data['transaction_hash']
            data['transaction_hash'] = tx['hash']
            data['transaction_index'] = i
            data['to'] = tx['to']
            # data['from'] = tx['from']
            logs = data.pop('logs') or []
            query = receipts_t.insert().values(block_number=block_number,
                                               block_hash=block_hash, **data)
            await conn.execute(query)

            tx_data = dict_keys_case_convert(tx)
            
            contract = await self.get_contract(conn, tx_data['to'])
            logs = self.process_logs(contract, logs)
            data = self.process_transaction(contract, tx_data, logs)
            query = transactions_t.insert().values(block_number=block_number,
                                                   block_hash=block_hash, **data)
            await conn.execute(query)
            await self.insert_logs(conn, block_number, block_hash, logs)

    async def insert_logs(self, conn, block_number, block_hash, logs):
        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            query = logs_t.insert().values(**data)
            await conn.execute(query)

    async def insert_accounts(self, conn, block_number, block_hash, accounts):
        for account in accounts:
            data = dict_keys_case_convert(json.loads(account['fields']))
            data['storage'] = None  # FIXME!!! 
            query = accounts_t.insert().values(block_number=block_number,
                                               address=account['address'].lower(),
                                               block_hash=block_hash, **data)
            await conn.execute(query)

    def process_logs(self, contract, logs):
        if contract is not None:
            for log in logs:
                e = contract_utils.decode_event(json.loads(contract['abi']), log)
                event_type = e.pop('_event_type')
                log['event_type'] = event_type.decode()
                log['event_args'] = e
        return logs

    def process_transaction(self, contract, tx_data, logs):
        if contract is not None:
            call = contract_utils.decode_contract_call(json.loads(contract['abi']), tx_data['input'])
            if call:
                tx_data['contract_call_description'] = call
                transfer_events = [l for l in logs if l['event_type'] == 'Transfer']
                if call['function'] in {'transfer', 'transerFrom'} and transfer_events:
                    tx_data['is_token_transfer'] = True
                    if contract['token_decimals']:
                        tx_data['token_amount'] = call['args'][-1] / (10 ** contract['token_decimals'])
        return tx_data

    async def get_contract(self, conn, address):
        q = select([contracts_t]).where(contracts_t.c.address == address)
        row = await pg.fetchrow(q)
        return row

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}
