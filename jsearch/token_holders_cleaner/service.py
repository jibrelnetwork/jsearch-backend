
import asyncio
import logging

import aiopg
import mode
from psycopg2.extras import DictCursor


logger = logging.getLogger(__name__)


SLEEP_TIME = 1
BATCH_SIZE = 100


class TokenHoldersCleaner(mode.Service):

    def __init__(self, main_db_dsn: str, *args, **kwargs) -> None:
        self.main_db_dsn = main_db_dsn
        self.total = 0
        super().__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.connect()

    async def on_stop(self) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        self.conn = await aiopg.connect(self.main_db_dsn, cursor_factory=DictCursor)

    async def disconnect(self) -> None:
        self.conn.close()

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
        async with self.conn.cursor() as cur:
            await cur.execute(q, [last_scanned, BATCH_SIZE])
            rows = await cur.fetchall()
        return [r['token_address'] for r in rows]

    async def clean_holder(self, holder):
        q = """
            SELECT clean_holder(%s);
        """
        async with self.conn.cursor() as cur:
            logger.debug('Clean for %s', holder)
            await cur.execute(q, [holder])
