import asyncio
import concurrent.futures
import logging

import backoff
import time

from jsearch import settings
from jsearch.service_bus import service_bus
from jsearch.syncer.database_queries.pending_transactions import prepare_pending_tx
from jsearch.syncer.processor import SyncProcessor

logger = logging.getLogger(__name__)

SLEEP_ON_NO_BLOCKS_DEFAULT = 1
REORGS_BATCH_SIZE = settings.JSEARCH_SYNC_PARALLEL / 2
PENDING_TX_BATCH_SIZE = settings.JSEARCH_SYNC_PARALLEL * 2

loop = asyncio.get_event_loop()

SYNC_MODE_FAST = 'fast'
SYNC_MODE_STRICT = 'strict'


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
        self.sleep_on_no_blocks = SLEEP_ON_NO_BLOCKS_DEFAULT

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=settings.JSEARCH_SYNC_PARALLEL)

        self.latest_available_block_num = None
        self.tasks = []

    async def start(self):
        logger.info("Starting Sync Manager", extra={'sync range': self.sync_range})
        self._running = True

        service_loops = [
            self.sequence_sync_loop(),
            self.reorg_loop(),
            self.pending_tx_loop(),
        ]

        for coro in service_loops:
            coro = asyncio.shield(coro)

            task = asyncio.ensure_future(coro)
            task.add_done_callback(self.tasks.remove)

            self.tasks.append(task)

    async def wait(self):
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            try:
                task.result()
            except Exception as e:
                logger.exception(e)
            self.tasks.remove(task)

        await self.stop()

    async def stop(self, timeout=60):
        self._running = False

        if not self.tasks:
            # All tasks have been completed already and removed from the list.
            return

        done, pending = await asyncio.wait(self.tasks, timeout=timeout)

        for future in done:
            future.result()

        if pending:
            logger.warning(
                'There are pending futures, that will be canceled',
                extra={
                    'tag': 'SYNCER',
                    'count': len(pending)
                }
            )

        for future in pending:
            future.cancel()

    async def sequence_sync_loop(self):
        logger.info("Entering Sequence Sync Loop")
        while self._running is True:
            await self.get_and_process_blocks()

    async def reorg_loop(self):
        logger.info("Entering Reorg Loop")
        while self._running is True:
            await self.get_and_process_chain_splits()

    async def pending_tx_loop(self):
        logger.info("Entering Pending Tx Loop")

        while self._running is True:
            await self.get_and_process_pending_txs()

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_blocks(self):
        start_time = time.monotonic()
        blocks_to_sync = await self.get_blocks_to_sync()
        if len(blocks_to_sync) == 0:
            await asyncio.sleep(self.sleep_on_no_blocks)
            return
        coros = [loop.run_in_executor(self.executor, sync_block, b[0], b[1]) for b in blocks_to_sync]
        results = await asyncio.gather(*coros)
        synced_blocks_cnt = sum(results)

        sync_time = time.monotonic() - start_time
        avg_time = sync_time / synced_blocks_cnt if synced_blocks_cnt else 0
        logger.info(
            "Synced batch of blocks",
            extra={
                'amount': synced_blocks_cnt,
                'total_time': sync_time,
                'average_time': avg_time,
            }
        )

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_chain_splits(self):
        start_time = time.monotonic()
        new_splits = await self.get_new_chain_splits()
        if len(new_splits) == 0:
            logger.info("No chain splits(reorgs), sleeping")
            await asyncio.sleep(self.sleep_on_no_blocks)
            return
        for split in new_splits:
            await self.process_chain_split(split)

        proc_time = time.monotonic() - start_time
        logger.info(
            "Processed batch of chain splits",
            extra={
                'amount': len(new_splits),
                'total_time': proc_time,
            }
        )

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_pending_txs(self):
        start_time = time.monotonic()
        new_pending_txs = await self.get_new_pending_txs()

        if len(new_pending_txs) == 0:
            logger.info("No pending txs, sleeping")
            await asyncio.sleep(self.sleep_on_no_blocks)
            return

        for pending_tx in new_pending_txs:
            data = prepare_pending_tx(pending_tx)
            await self.main_db.insert_or_update_pending_tx(data)

        proc_time = time.monotonic() - start_time
        logger.info(
            "Processed batch of pending txs",
            extra={
                'amount': len(new_pending_txs),
                'total_time': proc_time,
            }
        )

    async def process_chain_split(self, split):
        new_reorgs = await self.get_reorgs(split['id'])
        await self.process_reorgs(new_reorgs)
        await self.main_db.insert_chain_split(split)
        await asyncio.sleep(0.1)

    async def get_blocks_to_sync(self):
        latest_synced_block_num = await self.main_db.get_latest_synced_block_number(blocks_range=self.sync_range)
        latest_available_block_num = await self.raw_db.get_latest_available_block_number()

        if (latest_synced_block_num is not None and
                (latest_available_block_num - latest_synced_block_num) < self.chunk_size):

            # syncer is almost reached the head of chain, can fetch missed blocks now

            sync_mode = SYNC_MODE_STRICT
            blocks = await self.get_blocks_to_sync_strict()
            if len(blocks) < self.chunk_size:
                start_num = latest_synced_block_num + 1
                end_num = start_num + self.chunk_size - len(blocks)

                extra_blocks = await self.raw_db.get_blocks_to_sync(start_num, end_num)
                blocks += extra_blocks
        else:
            # syncer is far from chain head, need more speed, will skip missed blocks
            sync_mode = SYNC_MODE_FAST
            blocks = await self.get_blocks_to_sync_fast(latest_synced_block_num, self.chunk_size)

        if self.latest_available_block_num != latest_available_block_num:
            self.latest_available_block_num = latest_available_block_num
            await service_bus.emit_last_block_event(number=self.latest_available_block_num)

        logger.info(
            "Fetched new blocks to sync",
            extra={
                'latest_synced_block_number': latest_synced_block_num,
                'blocks_to_sync': len(blocks),
                'sync_mode': sync_mode,
            }
        )
        return blocks

    async def get_blocks_to_sync_fast(self, latest_synced_block_num, chunk_size):
        if latest_synced_block_num is None:
            start_block_num = self.sync_range[0]
        else:
            start_block_num = latest_synced_block_num + 1

        end_block_num = start_block_num + chunk_size - 1
        if self.sync_range[1]:
            end_block_num = min(end_block_num, self.sync_range[1])

        return await self.raw_db.get_blocks_to_sync(start_block_num, end_block_num)

    async def get_blocks_to_sync_strict(self):
        missed_blocks_nums = await self.main_db.get_missed_blocks_numbers(limit=self.chunk_size // 2)
        if not missed_blocks_nums:
            return []
        blocks = await self.raw_db.get_missed_blocks(missed_blocks_nums)
        return blocks

    async def get_reorgs(self, chain_split_id):
        new_reorgs = await self.raw_db.get_reorgs_by_chain_split_id(chain_split_id)
        return new_reorgs

    async def get_new_chain_splits(self):
        last_chain_split_num = await self.main_db.get_last_chain_split()

        logger.info("Fetched last chain split", extra={'number': last_chain_split_num})
        new_chain_splits = await self.raw_db.get_chain_splits_from(last_chain_split_num, REORGS_BATCH_SIZE)
        return new_chain_splits

    async def get_new_pending_txs(self):
        last_synced_id = await self.main_db.get_pending_tx_last_synced_id()
        logger.info("Fetched last pending tx synced ID", extra={'number': last_synced_id})

        return await self.raw_db.get_pending_txs_from(last_synced_id, PENDING_TX_BATCH_SIZE)

    async def process_reorgs(self, reorgs):
        c = 0
        for reorg in reorgs:
            await self.main_db.apply_reorg(reorg)
            await service_bus.emit_reorganization_event(
                block_hash=reorg['block_hash'],
                block_number=reorg['block_number'],
                reinserted=reorg['reinserted']
            )
            c += 1
        return c


def sync_block(block_hash, block_number=None, main_db_dsn=None, raw_db_dsn=None):
    processor = SyncProcessor(main_db_dsn=main_db_dsn, raw_db_dsn=raw_db_dsn)
    return processor.sync_block(block_hash, block_number)
