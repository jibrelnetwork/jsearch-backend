import functools

from typing import List, Optional

import asyncio
import logging

import mode

from jsearch.common.db import execute, fetch_all
from jsearch.common.services import DatabaseService
from jsearch.common.worker import shutdown_root_worker

logger = logging.getLogger(__name__)


SLEEP_TIME = 1
BATCH_SIZE = 100


class TokenHoldersCleaner(mode.Service):
    def __init__(self, main_db_dsn: str, **kwargs) -> None:
        self.database = DatabaseService(dsn=main_db_dsn)
        self.total = 0

        super().__init__(**kwargs)

    def on_init_dependencies(self) -> List[mode.Service]:
        return [self.database]

    async def on_started(self) -> None:
        fut = asyncio.create_task(self.cleaner())
        fut.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def cleaner(self) -> None:
        logger.info('Enter main loop')
        last_scanned = '0'
        while not self.should_stop:
            last_scanned = await self.clean_next_batch(last_scanned)  # type: ignore
            if last_scanned is None:
                last_scanned = '0'
                self.total = 0
                logger.info('Starting new iteration')
        logger.info('Leaving main loop')

    async def clean_next_batch(self, last_scanned: str) -> Optional[str]:
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

        return None

    async def get_next_batch(self, last_scanned: str) -> List[str]:
        q = """
            SELECT DISTINCT token_address
            FROM token_holders
            WHERE token_address > %s
            ORDER BY token_address
            LIMIT %s;
        """

        rows = await fetch_all(self.database.engine, q, last_scanned, BATCH_SIZE)
        return [r['token_address'] for r in rows]

    async def clean_holder(self, holder: str) -> None:
        q = """
            SELECT clean_holder(%s);
        """
        await execute(self.database.engine, q, holder)
