r"""
Communication scheme for token transfer reorganization.

                     jsearch.last_block
 --------               ------------       -------------------
| raw_db |             |   kafka    |     | jsearch_contracts |
 --------               ------------       -------------------
     |                     |                         /'\
     |  reorg record       |  new last block          |
     |                     |                          | get contracts
     |                     |                          |
    \./                   \./ jsearch.reorganization  |
 --------  reorg event    -----------------------------                balance                    -----------
| syncer | - - - - - >   | handle_block_reorganization | < - - - - - - - - - - - - - - - - - - - | geth node |
 --------  block hash     -----------------------------     request with offset from last block   -----------
                                 /'\    |
                                  |     |
     get token and accounts       |     |   update balances
     get transfers on interval:   |     |
       - last_block - offset      |    \./
       - last_block         ----------------
                           |     main db    |
                            ----------------
"""

import asyncio
import logging
from collections import defaultdict
from functools import partial
from typing import List, Dict

import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine
from mode import Service, Worker

from jsearch import settings
from jsearch.common.last_block import LastBlock
from jsearch.common.logs import configure
from jsearch.common.processing.erc20_balances import fetch_erc20_token_balance
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics
from jsearch.service_bus import service_bus, ROUTE_HANDLE_REORGANIZATION_EVENTS, ROUTE_HANDLE_LAST_BLOCK
from jsearch.syncer.database_queries.token_holders import update_token_holder_balance_q
from jsearch.syncer.database_queries.token_transfers import (
    get_token_address_and_accounts_for_block_q,
    get_transfer_from_since_block_query,
    get_transfer_to_since_block_query,
)
from jsearch.utils import Singleton

logger = logging.getLogger('worker')

metrics = Metrics()


class DatabaseService(Service, Singleton):
    engine: Engine

    def on_init_dependencies(self) -> List[Service]:
        return [service_bus]

    @backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
    async def on_start(self) -> None:
        self.engine = await create_engine(settings.JSEARCH_MAIN_DB)

    async def on_stop(self) -> None:
        self.engine.close()
        await self.engine.wait_closed()


service = DatabaseService()


async def get_positive_changes(connection, account, last_stable_block) -> int:
    query = get_transfer_to_since_block_query(account, last_stable_block)
    result = await connection.execute(query)
    return (await result.fetchone())['value'] or 0


async def get_negative_changes(connection, account, last_stable_block) -> int:
    query = get_transfer_from_since_block_query(account, last_stable_block)
    result = await connection.execute(query)
    return (await result.fetchone())['value'] or 0


async def get_balance(account, token, abi, connection):
    loop = asyncio.get_event_loop()
    last_stable_block = await LastBlock().get_last_stable_block()

    balance = await loop.run_in_executor(
        executor=executor.get(),
        func=partial(
            fetch_erc20_token_balance,
            contract_abi=abi,
            token_address=token,
            account_address=account,
            block=last_stable_block,
        )
    )

    if balance is not None and not isinstance(balance, int):
        logger.info("[GET BALANCE] Balance %s can't be handled for %s holder %s", balance, token, account)
        balance = None
    else:
        positive_changes = await get_positive_changes(connection, account, last_stable_block)
        negative_chagnes = await get_negative_changes(connection, account, last_stable_block)
        changes = positive_changes - negative_chagnes
        balance = (balance or 0) + changes
    return balance


@service_bus.listen_stream(ROUTE_HANDLE_REORGANIZATION_EVENTS, service_name='jsearch-worker')
async def handle_block_reorganization(block_hash, block_number, reinserted):
    logging.info("[REORG] Block number %s, hash %s with reinsert status (%s)", block_number, block_hash, reinserted)

    async with service.engine.acquire() as connection:
        updates = defaultdict(set)
        query = get_token_address_and_accounts_for_block_q(block_hash=block_hash)

        async with connection.execute(query) as cursor:
            for record in await cursor.fetchall():
                token = record['token_address']
                account = record['address']

                updates[token].add(account)

            contract_list = await service_bus.get_contracts(addresses=updates.keys())
            contracts = {contract['address']: contract for contract in contract_list}

        for token, accounts in updates.items():
            for account in accounts:
                contract = contracts.get(token)

                abi = contract['abi']
                decimals = contract['token_decimals']

                logger.info("[REORG] Get balance for token %s holder %s", token, account)
                balance = await get_balance(account, token, abi, connection)

                if balance is not None:
                    logger.info("[REORG] Update balance for token %s holder %s with value %s", token, account, balance)
                    query = update_token_holder_balance_q(
                        token_address=token,
                        account_address=account,
                        balance=balance,
                        decimals=decimals
                    )
                    await connection.execute(query=query)


@service_bus.listen_stream(service_name='jsearch-worker', stream_name=ROUTE_HANDLE_LAST_BLOCK)
async def receive_last_block(record: Dict[str, int]):
    number = record.get('number')

    logger.info("[LAST BLOCK] Receive new last block number %s", number)

    last_block = LastBlock()
    last_block.update(number=number)


@click.command()
@click.option('--log-level', default='INFO')
def main(log_level: str) -> None:
    configure(log_level)
    Worker(service, loglevel=log_level).execute_from_commandline()
