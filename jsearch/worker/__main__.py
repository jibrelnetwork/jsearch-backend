import asyncio
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
from jsearch.service_bus import service_bus
from jsearch.syncer.database_queries.token_holders import update_token_holder_balance_q
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


@service_bus.listen_stream('update_token_holder_balance')
async def handle_recalculate_balance(token_address: str, account_address: str, reason: Optional[str] = None):
    """
    get new balance and update value in database
    """
    loop = asyncio.get_event_loop()

    contract = await service_bus.get_contract(address=token_address)

    abi = contract['abi']
    decimals = contract['decimals']

    balance = await loop.run_in_executor(
        executor=executor.get(),
        func=partial(
            fetch_erc20_token_balance,
            contract_abi=abi,
            token_address=token_address,
            account_address=account_address
        )
    )

    logger.info(
        "[TASK] Update token %s holder % balance %s, reason is %s",
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
