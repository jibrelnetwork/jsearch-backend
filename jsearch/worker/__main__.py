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
from aiopg.sa import Engine, create_engine
from mode import Service

from jsearch import settings
from jsearch.common import logs
from jsearch.common import worker
from jsearch.common.last_block import LastBlock
from jsearch.multiprocessing import executor
from jsearch.service_bus import service_bus, ROUTE_HANDLE_REORGANIZATION_EVENTS, ROUTE_HANDLE_LAST_BLOCK
from jsearch.syncer.database_queries.assets_summary import insert_or_update_assets_summary
from jsearch.utils import Singleton
from jsearch.worker.api_service import ApiService
from jsearch.worker.token_balances import get_balance_updates, update_balances

logger = logging.getLogger('worker')


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


@service_bus.listen_stream(ROUTE_HANDLE_REORGANIZATION_EVENTS, service_name='jsearch-worker')
async def handle_block_reorganization(record):
    reinserted = record['reinserted']
    block_hash = record['block_hash']
    block_number = record['block_number']

    logger.info(
        'Handling block reorganization...',
        extra={
            'tag': 'REORG',
            'block_number': block_number,
            'block_hash': block_hash,
            'reinserted': reinserted,
        }
    )

    loop = asyncio.get_event_loop()
    last_block = await LastBlock().get()

    async with service.engine.acquire() as connection:
        updates = await get_balance_updates(connection, block_hash, block_number)

    logger.info(
        'Fetched balance updates',
        extra={
            'tag': 'REORG',
            'block_number': block_number,
            'block_hash': block_hash,
            'reinserted': reinserted,
            'balance_updates_count': len(updates),
        }
    )

    updates = await loop.run_in_executor(executor.get(), update_balances, updates, last_block)

    async with service.engine.acquire() as connection:
        for update in updates:
            asset_update = update.to_asset_update()
            query = insert_or_update_assets_summary(
                address=asset_update['address'],
                asset_address=asset_update['asset_address'],
                value=asset_update['value'],
                decimals=asset_update['decimals']
            )

            await connection.execute(query)


@service_bus.listen_stream(service_name='jsearch-worker', stream_name=ROUTE_HANDLE_LAST_BLOCK)
async def receive_last_block(record: Dict[str, int]):
    number = record.get('number')

    logger.info("Received new last block", extra={'tag': 'LAST BLOCK', 'number': number})

    last_block = LastBlock()
    last_block.update(number=number)


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL)
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def main(log_level: str, no_json_formatter: bool) -> None:
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))
    worker.Worker(
        service,
        ApiService(),
    ).execute_from_commandline()
