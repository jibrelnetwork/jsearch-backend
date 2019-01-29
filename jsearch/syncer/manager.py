import asyncio
import concurrent.futures
import logging
import time

from jsearch import settings
from jsearch.common.database import DatabaseError
from jsearch.syncer.processor import SyncProcessor

logger = logging.getLogger(__name__)

SLEEP_ON_ERROR_DEFAULT = 0.1
SLEEP_ON_DB_ERROR_DEFAULT = 5
SLEEP_ON_NO_BLOCKS_DEFAULT = 1
REORGS_BATCH_SIZE = settings.JSEARCH_SYNC_PARALLEL / 2

loop = asyncio.get_event_loop()


class Manager:
    """
    Sync manager
    """

    def __init__(self, service, main_db, raw_db, sync_range):
        self.service = service
        self.main_db = main_db
        self.raw_db = raw_db
        self.sync_range = sync_range
        self._running = False
        self.chunk_size = settings.JSEARCH_SYNC_PARALLEL
        self.sleep_on_db_error = SLEEP_ON_DB_ERROR_DEFAULT
        self.sleep_on_error = SLEEP_ON_ERROR_DEFAULT
        self.sleep_on_no_blocks = SLEEP_ON_NO_BLOCKS_DEFAULT
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=settings.JSEARCH_SYNC_PARALLEL)

    async def run(self):
        logger.info("Starting Sync Manager, sync range: %s", self.sync_range)
        self._running = True
        asyncio.ensure_future(self.sequence_sync_loop())
        asyncio.ensure_future(self.reorg_loop())

    def stop(self):
        self._running = False

    async def sequence_sync_loop(self):
        logger.info("Entering Sequence Sync Loop")
        while self._running is True:
            try:
                start_time = time.monotonic()
                blocks_to_sync = await self.get_blocks_to_sync()
                if len(blocks_to_sync) == 0:
                    await asyncio.sleep(self.sleep_on_no_blocks)
                    continue
                coros = [loop.run_in_executor(self.executor, sync_block, b[0], b[1]) for b in blocks_to_sync]
                results = await asyncio.gather(*coros)
                synced_blocks_cnt = sum(results)

                sync_time = time.monotonic() - start_time
                avg_time = sync_time / synced_blocks_cnt if synced_blocks_cnt else 0
                logger.info("%s blocks synced on %0.2fs, avg time %0.2fs", synced_blocks_cnt, sync_time, avg_time)
            except DatabaseError:
                logger.exception("Database Error accured:")
                await asyncio.sleep(self.sleep_on_db_error)
            except Exception:
                logger.exception("Error accured:")
                await asyncio.sleep(self.sleep_on_error)
                self.sleep_on_error = self.sleep_on_error * 2
            else:
                self.sleep_on_error = SLEEP_ON_ERROR_DEFAULT

    async def reorg_loop(self):
        logger.info("Entering Reorg Loop")
        while self._running is True:
            try:
                start_time = time.monotonic()
                new_reorgs = await self.get_new_reorgs()
                if len(new_reorgs) == 0:
                    logger.info("No reorgs, sleeping")
                    await asyncio.sleep(self.sleep_on_no_blocks)
                    continue

                processed_reorgs_num = await self.process_reorgs(new_reorgs)

                proc_time = time.monotonic() - start_time
                logger.info("%s reorgs processed on %0.2fs", processed_reorgs_num, proc_time)
                await asyncio.sleep(0.1)
            except DatabaseError:
                logger.exception("Database Error accured:")
                await asyncio.sleep(self.sleep_on_db_error)
            except Exception:
                logger.exception("Error accured:")
                await asyncio.sleep(self.sleep_on_error)
                self.sleep_on_error = self.sleep_on_error * 2
            else:
                self.sleep_on_error = SLEEP_ON_ERROR_DEFAULT

    async def get_blocks_to_sync(self):
        latest_synced_block_num = await self.main_db.get_latest_synced_block_number(blocks_range=self.sync_range)
        latest_available_block_num = await self.raw_db.get_latest_available_block_number()
        if latest_available_block_num - (latest_synced_block_num or 0) < self.chunk_size:
            # syncer is almost reached the head of chain, can fetch missed blocks now
            sync_mode = 'strict'
            blocks = await self.get_blocks_to_sync_strict()
            if len(blocks) < self.chunk_size:
                start_num = latest_synced_block_num + 1
                end_num = start_num + self.chunk_size - len(blocks)
                extra_blocks = await self.raw_db.get_blocks_to_sync(start_num, end_num)
                blocks += extra_blocks
        else:
            # syncer is far from chain head, need more speed, will skip missed blocks
            sync_mode = 'fast'
            blocks = await self.get_blocks_to_sync_fast(latest_synced_block_num, self.chunk_size)

        logger.info("Latest synced block num is %s, %s blocks to sync, sync mode: %s",
                    latest_synced_block_num, len(blocks), sync_mode)
        return blocks

    async def get_blocks_to_sync_fast(self, latest_synced_block_num, chunk_size):
        if latest_synced_block_num is None:
            start_block_num = self.sync_range[0]
        else:
            start_block_num = latest_synced_block_num + 1
        end_block_num = start_block_num + chunk_size - 1
        if self.sync_range[1]:
            end_block_num = min(end_block_num, self.sync_range[1])
        blocks = await self.raw_db.get_blocks_to_sync(start_block_num, end_block_num)
        return blocks

    async def get_blocks_to_sync_strict(self):
        missed_blocks_nums = await self.main_db.get_missed_blocks_numbers(limit=self.chunk_size // 2)
        if not missed_blocks_nums:
            return []
        blocks = await self.raw_db.get_missed_blocks(missed_blocks_nums)
        return blocks

    async def get_new_reorgs(self):
        last_reorg_num = await self.main_db.get_last_reorg()
        new_reorgs = await self.raw_db.get_reorgs_from(last_reorg_num, REORGS_BATCH_SIZE)
        return new_reorgs

    async def process_reorgs(self, reorgs):
        c = 0
        for reorg in reorgs:
            success = await self.main_db.apply_reorg(reorg)
            if success is not True:
                # reorg block is not synced, try to sync it now
                logger.debug('Reorg: try to sync missed block %s %s', reorg['block_hash'], reorg['block_number'])
                await loop.run_in_executor(self.executor, sync_block, reorg['block_hash'], reorg['block_number'])
                return c
            c += 1
        return c


def sync_block(block_hash, block_number=None, main_db_dsn=None, raw_db_dsn=None):
    processor = SyncProcessor(main_db_dsn=main_db_dsn, raw_db_dsn=raw_db_dsn)
    return processor.sync_block(block_hash, block_number)
