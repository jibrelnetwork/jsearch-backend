r"""
Communication scheme for token transfer reorganization.

 --------                                    -------------------
| raw_db |                                  | jsearch_contracts |
 --------                                    -------------------
     |                                            /'\
     |  reorg record                               |
     |                                             | get contracts
    \./                   jsearch.reorganization   |
 --------  reorg event  -----------------------------   balance     -----------
| syncer | - - - - - > | handle_block_reorganization | < - - - ->  | geth node |
 --------  block hash   -----------------------------   request     -----------
                                 /'\   |
                                  |    |
           get token and accounts |   \./ update balances
                            ----------------
                           |     main db    |
                            ----------------

"""

import asyncio
import logging
from collections import defaultdict
from functools import partial
from typing import List

import backoff
import click
import psycopg2
from aiopg.sa import Engine, create_engine
from mode import Service, Worker

from jsearch import settings
from jsearch.common.logs import configure
from jsearch.common.processing.erc20_balances import fetch_erc20_token_balance
from jsearch.multiprocessing import executor
from jsearch.post_processing.metrics import Metrics
from jsearch.service_bus import service_bus, WORKER_HANDLE_REORGANIZATION_EVENT
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


service = DatabaseService()


@service_bus.listen_stream(WORKER_HANDLE_REORGANIZATION_EVENT)
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

        loop = asyncio.get_event_loop()
        for token, accounts in updates.items():
            for account in accounts:
                contract = contracts.get(token)

                abi = contract['abi']
                decimals = contract['token_decimals']

                logger.info("[REORG] Get balance for token %s holder %s", token, account)

                balance = await loop.run_in_executor(
                    executor=executor.get(),
                    func=partial(
                        fetch_erc20_token_balance,
                        contract_abi=abi,
                        token_address=token,
                        account_address=account
                    )
                )
                if balance is None:
                    logger.info("[REORG] Balance can't be fetched for %s holder %s", token, account)
                    balance = 0

                logger.info("[REORG] Update balance for token %s holder %s with balance %s", token, account, balance)

                query = update_token_holder_balance_q(
                    token_address=token,
                    account_address=account,
                    balance=balance,
                    decimals=decimals
                )
                await connection.execute(query=query)


@click.command()
@click.option('--log-level', default='INFO')
def main(log_level: str) -> None:
    configure(log_level)
    Worker(service, loglevel=log_level).execute_from_commandline()
