import asyncio
import functools
import logging
import mode
from typing import List, Optional, Tuple

from jsearch.common.db import execute, fetch_all, fetch_one
from jsearch.common.services import DatabaseService
from jsearch.common.worker import shutdown_root_worker
from jsearch.token_holders_cleaner import settings
from jsearch.token_holders_cleaner.database_queries import (
    get_pairs_batch,
    delete_stale_holders_by_pair,
    get_max_block_number_for_pair,
)
from jsearch.token_holders_cleaner.structs import Pair

logger = logging.getLogger(__name__)


def get_starting_pair() -> Pair:
    return Pair(account_address='0', token_address='0')


class TokenHoldersCleaner(mode.Service):
    def __init__(self, main_db_dsn: str, **kwargs) -> None:
        self.database = DatabaseService(dsn=main_db_dsn)
        super().__init__(**kwargs)

    def on_init_dependencies(self) -> List[mode.Service]:
        return [self.database]

    async def on_started(self) -> None:
        fut = asyncio.create_task(self.cleaner())
        fut.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def cleaner(self) -> None:
        last_processed_pair = get_starting_pair()
        total_processed = 0

        while not self.should_stop:
            last_processed_pair, total_processed = await self.clean_next_batch(  # type: ignore
                last_processed_pair, total_processed
            )

            if last_processed_pair is None:
                logger.info('Starting new cleaning iteration...')
                last_processed_pair = get_starting_pair()
                total_processed = 0

            logger.info('%s total pairs processed', total_processed)

    async def clean_next_batch(
            self,
            last_processed_pair: Pair,
            total_processed: int,
    ) -> Tuple[Optional[Pair], Optional[int]]:
        pairs = await self.get_next_batch(last_processed_pair)

        if not pairs:
            return None, None

        total_processed += len(pairs)
        logger.info('Gotta process %s pairs', len(pairs))

        for pair in pairs:
            await self.clean_pair(pair)
            await asyncio.sleep(settings.SLEEP_TIME)

        return pairs[-1], total_processed

    async def get_next_batch(self, last_scanned_pair: Pair) -> List[Pair]:
        logger.info('Fetching next batch...')

        q = get_pairs_batch(last_scanned_pair, limit=settings.BATCH_SIZE)

        pairs_rows = await fetch_all(self.database.engine, q)
        pairs = [Pair(account_address=row['address'], token_address=row['asset_address']) for row in pairs_rows]

        logger.info('Fetched %s pairs', len(pairs))

        return pairs

    async def clean_pair(self, pair: Pair) -> None:
        block_number_q = get_max_block_number_for_pair(pair)
        block_number_row = await fetch_one(self.database.engine, block_number_q)
        block_number = block_number_row['max_block_number']

        if block_number is None:
            return

        logger.info('Cleaning stale entries for %r until %s block', pair, block_number)

        block_number = int(block_number)
        block_number = block_number - settings.OFFSET

        clean_q = delete_stale_holders_by_pair(pair, block_number)

        await execute(self.database.engine, clean_q)
