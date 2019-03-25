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
from typing import List, Dict

import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine, SAConnection
from mode import Service, Worker

from jsearch import settings
from jsearch.common.contracts import ERC20_ABI, ERC20_DEFAULT_DECIMALS
from jsearch.common.last_block import LastBlock
from jsearch.common.logs import configure
from jsearch.common.processing.erc20_balances import (
    BalanceUpdate,
    BalanceUpdates,
    fetch_erc20_balance_bulk
)
from jsearch.common.processing.utils import fetch_contracts, prefetch_decimals
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics
from jsearch.service_bus import service_bus, ROUTE_HANDLE_REORGANIZATION_EVENTS, ROUTE_HANDLE_LAST_BLOCK
from jsearch.syncer.database import MainDBSync
from jsearch.syncer.database_queries.token_transfers import get_token_address_and_accounts_for_block_q
from jsearch.utils import Singleton, split

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


async def get_balance_updates(connection: SAConnection, block_hash: str, block_number: int) -> BalanceUpdates:
    loop = asyncio.get_event_loop()
    query = get_token_address_and_accounts_for_block_q(block_hash=block_hash)

    updates = set()
    async with connection.execute(query) as cursor:
        for record in await cursor.fetchall():
            token = record['token_address']
            account = record['address']

            update = BalanceUpdate(
                token_address=token,
                account_address=account,
                block=block_number,
                abi=None,
                decimals=None
            )
            updates.add(update)

        addresses = list({update.token_address for update in updates})

        contracts = await fetch_contracts(addresses)
        contracts = await loop.run_in_executor(executor.get(), prefetch_decimals, contracts)

        for update in updates:
            contract = contracts.get(update.token_address)
            if contract:
                update.abi = contract['abi']
                update.decimals = contract['decimals']
            else:
                update.abi = ERC20_ABI
                update.decimals = ERC20_DEFAULT_DECIMALS

        return [update for update in updates if update.abi is not None]


def worker(updates: BalanceUpdates, last_block: int, batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE) -> None:
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:

        for chunk in split(updates, batch_size):
            updates = fetch_erc20_balance_bulk(chunk, block=last_block)

        for update in updates:
            update.apply(db, last_block)


@service_bus.listen_stream(ROUTE_HANDLE_REORGANIZATION_EVENTS, service_name='jsearch-worker')
async def handle_block_reorganization(record):
    reinserted = record['reinserted']
    block_hash = record['block_hash']
    block_number = record['block_number']

    logging.info("[REORG] Block number %s, hash %s with reinsert status (%s)", block_number, block_hash, reinserted)
    loop = asyncio.get_event_loop()
    last_block = await LastBlock().get()

    async with service.engine.acquire() as connection:
        updates = await get_balance_updates(connection, block_hash, block_number)
        logging.info(
            "[REORG] Block number %s, hash %s with reinsert status (%s): %s updates",
            block_number, block_hash, reinserted, len(updates)
        )
        await loop.run_in_executor(executor.get(), worker, updates, last_block)


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
