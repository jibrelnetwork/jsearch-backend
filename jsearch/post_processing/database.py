import backoff
from aiopg.sa import create_engine, Engine
from mode import Service
from sqlalchemy.dialects.postgresql import psycopg2

from jsearch import settings
from jsearch.syncer.database_queries.logs import update_log_query
from jsearch.typing import Logs
from jsearch.utils import Singleton


class DatabaseService(Service, Singleton):
    engine: Engine

    @backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
    async def on_start(self) -> None:
        self.engine = await create_engine(settings.JSEARCH_MAIN_DB)

    async def on_stop(self) -> None:
        self.engine.close()
        await self.engine.wait_closed()

    async def update_logs(self, logs: Logs) -> None:
        for log in logs:
            query = update_log_query(
                tx_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_hash=log['block_hash'],
                values={
                    'is_processed': True,
                    'is_token_transfer': log.get('is_token_transfer', False),
                    'token_transfer_to': log.get('token_transfer_to'),
                    'token_transfer_from': log.get('token_transfer_from'),
                    'token_amount': log.get('token_amount'),
                    'event_type': log.get('event_type'),
                    'event_args': log.get('event_args'),
                }
            )
            async with self.engine.acquire() as connection:
                await connection.execute(query)


db_service = DatabaseService()
