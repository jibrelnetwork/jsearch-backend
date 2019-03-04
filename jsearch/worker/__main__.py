"""
Communication scheme for token transfer reorganization.

 --------
| raw_db |
 --------
     |
    \./                   jsearch.reorganization                           jsearch.update_token_holder_balance
 --------  reorg event  -----------------------------   update event      ------------------------------------
| syncer | - - - - - > | handle_block_reorganization | - - - - - - - - > | handle_update_token_holder_balance |
 --------  block hash   -----------------------------  token and account  ------------------------------------
                                 /'\                                                     |
                                  | get token and accounts                              \./ update balances
                            ----------------------------------------------------------------------------
                           |                             main db                                        |
                            ----------------------------------------------------------------------------

"""

import asyncio
import json
import logging
from functools import partial
from typing import Optional, List

import backoff
import click
from aiopg.sa import Engine, create_engine
from mode import Service, Worker
from sqlalchemy.dialects.postgresql import psycopg2

from jsearch import settings
from jsearch.common.logs import configure
from jsearch.common.processing.erc20_balances import fetch_erc20_token_balance
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics
from jsearch.service_bus import service_bus, WORKER_UPDATE_TOKEN_HOLDER_BALANCE, WORKER_HANDLE_REORGANIZATION_EVENT
from jsearch.syncer.database_queries.token_holders import update_token_holder_balance_q
from jsearch.syncer.database_queries.token_transfers import get_token_address_and_accounts_for_block_q
from jsearch.utils import Singleton

logger = logging.getLogger('worker')

metrics = Metrics()


class DatabaseService(Service, Singleton):
    engine: Engine

    def on_init_dependencies(self) -> List[Service]:
        return [service_bus]

    @backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
    async def on_start(self) -> None:
        self.engine = await create_engine(settings.DB_DSN)

    async def on_stop(self) -> None:
        self.engine.close()
        await self.engine.wait_closed()


database_service = DatabaseService()


@service_bus.listen_stream(WORKER_HANDLE_REORGANIZATION_EVENT)
async def handle_block_reorganization(block_hash, block_number, reinserted):
    logging.info("[REORG] Block number %s, hash %s with reinsert status (%s)", block_number, block_hash, reinserted)

    async with database_service.engine.acquire() as connection:
        query = get_token_address_and_accounts_for_block_q(block_hash=block_hash)

        async with connection.execute(query) as cursor:
            for record in cursor.fetch_all():
                token_address = record['token_address']
                account_address = record['address']

                await service_bus.emit_update_token_balance_holder_event(
                    token_address=token_address,
                    account_address=account_address,
                    reason=json.dumps({
                        'type': 'Reorganization',
                        'block_hash': block_hash,
                        'block_number': block_number
                    })
                )


@service_bus.listen_stream(WORKER_UPDATE_TOKEN_HOLDER_BALANCE)
async def handle_update_token_holder_balance(token_address: str, account_address: str, reason: Optional[str] = None):
    """
    get new balance and update value in database
    """
    loop = asyncio.get_event_loop()

    contract = await service_bus.get_contract(address=token_address)

    abi = contract['abi']
    decimals = contract['decimals']

    logger.info("[UPDATE BALANCE] Get balance for token %s holder %", token_address, account_address)

    balance = await loop.run_in_executor(
        executor=executor.get(),
        func=partial(
            fetch_erc20_token_balance,
            contract_abi=abi,
            token_address=token_address,
            account_address=account_address
        )
    )
    if balance is not None:
        logger.info(
            "[UPDATE BALANCE] Update balance for token %s holder % with balance %s, reason is %s",
            token_address, account_address, balance, reason
        )

        async with database_service.engine.acquire() as connection:
            query = update_token_holder_balance_q(
                token_address=token_address,
                account_address=account_address,
                balance=balance,
                decimals=decimals
            )
            await connection.execute(query=query)


@click.command()
@click.option('--log-level', default='INFO')
def main(log_level: str) -> None:
    configure(log_level)
    Worker(database_service, loglevel=log_level).execute_from_commandline()
