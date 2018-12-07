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
        # TODO: de we really need sync by events?
        # asyncio.ensure_future(self.listen_new_blocks(self.raw_db.conn))
        # asyncio.ensure_future(self.listen_reinsert(self.raw_db.conn))
        # asyncio.ensure_future(self.listen_reorg(self.raw_db.conn))

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
                    await asyncio.sleep(self.sleep_on_no_blocks)
                    continue

                await self.process_reorgs(new_reorgs)

                proc_time = time.monotonic() - start_time
                logger.info("%s reorgs processed on %0.2fs", len(new_reorgs), proc_time)
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
        latest_block_num = await self.main_db.get_latest_sequence_synced_block_number(blocks_range=self.sync_range)
        if latest_block_num is None:
            start_block_num = self.sync_range[0]
        else:
            start_block_num = latest_block_num + 1
        end_block_num = start_block_num + self.chunk_size - 1
        if self.sync_range[1]:
            end_block_num = min(end_block_num, self.sync_range[1])
        blocks = await self.raw_db.get_blocks_to_sync(start_block_num, end_block_num)
        logger.info("Latest synced block num is %s, %s blocks to sync", latest_block_num, len(blocks))
        return blocks

    async def get_blocks_to_sync(self):
        latest_block_num = await self.main_db.get_latest_sequence_synced_block_number(blocks_range=self.sync_range)
        if latest_block_num is None:
            start_block_num = self.sync_range[0]
        else:
            start_block_num = latest_block_num + 1
        end_block_num = start_block_num + self.chunk_size - 1
        if self.sync_range[1]:
            end_block_num = min(end_block_num, self.sync_range[1])
        blocks = await self.raw_db.get_blocks_to_sync(start_block_num, end_block_num)
        logger.info("Latest synced block num is %s, %s blocks to sync", latest_block_num, len(blocks))
        return blocks

    async def get_new_reorgs(self):
        last_reorg_num = await self.main_db.get_last_reorg()
        new_reorgs = await self.raw_db.get_reorgs_from(last_reorg_num, REORGS_BATCH_SIZE)
        return new_reorgs

    async def process_reorgs(self, reorgs):
        for reorg in reorgs:
            success = await self.main_db.apply_reorg(reorg)
            if success is not True:
                return

    async def listen_new_blocks(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("LISTEN newblock")
            logger.info("Starting Listen newblock channel")
            while self._running is True:
                msg = await conn.notifies.get()
                logger.info('Newblock notification received: %s', msg.payload)
                try:
                    block_hash = msg.payload
                    await loop.run_in_executor(self.executor, sync_block, block_hash)
                except Exception:
                    logger.exception('Error on newblock listener')

    async def listen_reorg(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("LISTEN newreorg")
            logger.info("Starting Listen newreorg channel")
            while self._running is True:
                msg = await conn.notifies.get()
                logger.info('New Reorg notification received: %s', msg.payload)
                try:
                    block_hash = msg.payload
                    await loop.run_in_executor(self.executor, reorg_block, block_hash)
                except Exception:
                    logger.exception('Error on newreorg listener')

    async def listen_reinsert(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("LISTEN newreinsert")
            logger.info("Starting Listen newreinsert channel")
            while self._running is True:
                msg = await conn.notifies.get()
                logger.info('New Reinsert notification received: %s', msg.payload)
                try:
                    block_hash = msg.payload
                    await loop.run_in_executor(self.executor, reinsert_block, block_hash)
                except Exception:
                    logger.exception('Error on newreinsert listener')


def sync_block(block_hash, block_number=None, main_db_dsn=None, raw_db_dsn=None):
    processor = SyncProcessor(main_db_dsn=main_db_dsn, raw_db_dsn=raw_db_dsn)
    return processor.sync_block(block_hash, block_number)


def reorg_block(block_hash):
    logger.info('Reorg block %s', block_hash)


def reinsert_block(block_hash):
    logger.info('ReInsert block %s', block_hash)