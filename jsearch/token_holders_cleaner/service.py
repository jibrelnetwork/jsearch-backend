from aiopg.sa import Engine
from typing import Optional

import asyncio
import logging

import aiopg
import mode
from psycopg2.extras import DictCursor

from jsearch.common.db import execute, fetch_all

logger = logging.getLogger(__name__)


SLEEP_TIME = 1
BATCH_SIZE = 100


class TokenHoldersCleaner(mode.Service):
    engine: Optional[Engine] = None

    def __init__(self, main_db_dsn: str, **kwargs) -> None:
        self.main_db_dsn = main_db_dsn
        self.total = 0
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.connect()

    async def on_stop(self) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        self.engine = await aiopg.sa.create_engine(self.main_db_dsn, cursor_factory=DictCursor)

    async def disconnect(self) -> None:
        if self.engine is None:
            return

        self.engine.close()

    @mode.Service.task
    async def main_loop(self):
        logger.info('Enter main loop')
        last_scanned = '0'
        while not self.should_stop:
            last_scanned = await self.clean_next_batch(last_scanned)
            if last_scanned is None:
                last_scanned = '0'
                self.total = 0
                logger.info('Starting new iteration')
        logger.info('Leaving main loop')

    async def clean_next_batch(self, last_scanned):
        logger.info('Fetching next batch')
        holders = await self.get_next_batch(last_scanned)
        self.total += len(holders)
        logger.info('%s items to process, total %s', len(holders), self.total)

        for holder in holders:
            await self.clean_holder(holder)
            await asyncio.sleep(SLEEP_TIME)
        if holders:
            last = holders[-1]
            return last

    async def get_next_batch(self, last_scanned):
        q = """
            SELECT DISTINCT token_address
            FROM token_holders
            WHERE token_address > %s
            ORDER BY token_address
            LIMIT %s;
        """

        rows = await fetch_all(self.engine, q, last_scanned, BATCH_SIZE)
        return [r['token_address'] for r in rows]

    async def clean_holder(self, holder):
        q = """
            SELECT clean_holder(%s);
        """
        await execute(self.engine, q, holder)
