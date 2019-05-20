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


"""
Dev Note - RawDB filling order:

internal_transactions
rewards
bodies
headers
accounts
receipts
reorgs
chain_splits

"""

class ChainEvent:
    INSERT = 'created'
    REINSERT = 'reinserted'
    SPLIT = 'split'


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
        self.latest_synced_block_num = None
        self.blockchain_tip = None
        self.sync_mode = SYNC_MODE_STRICT
        self.tasks = []
        self.tip = None

        self.processor = SyncProcessor()

    async def start(self):
        logger.info("Starting Sync Manager", extra={'sync range': self.sync_range})
        self._running = True

        service_loops = [
            # self.sequence_sync_loop(),
            self.chain_events_process_loop(),
            # self.reorg_loop(),
            self.pending_tx_loop(),
        ]

        for coro in service_loops:
            coro = asyncio.shield(coro)

            task = asyncio.ensure_future(coro)
            task.add_done_callback(self.tasks.remove)

            self.tasks.append(task)

    async def wait(self):
        await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
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

    async def chain_events_process_loop(self):
        logger.info("Entering Chain Events Process Loop")
        while self._running is True:
            await self.get_and_process_chain_event()

    async def process_chain_event(self, event):
        start_time = time.monotonic()
        logger.info("Start Processing Chain Event", extra={
            'event_id': event['id'],
            'event_type': event['event_type'],
        })
        if event['event_type'] == ChainEvent.INSERT:
            await self.process_insert_block(event['block_hash'], event['block_number'])
        elif event['event_type'] == ChainEvent.REINSERT:
            # await self.process_reinsert_block(event['block_hash'], event['block_num'])
            pass
        elif event['event_type'] == ChainEvent.SPLIT:
            await self.process_chain_split(event['split_id'])
        else:
            logger.error('Invalid chain event', extra={
                'event_id': event['id'],
                'event_type': event['event_type'],
            })
        logger.info("Finish Processing Chain Event", extra={
            'event_id': event['id'],
            'event_type': event['event_type'],
            'time': '{:0.2f}s'.format(time.monotonic() - start_time)
        })
        await self.main_db.insert_chain_event(event)

    async def process_insert_block(self, block_hash, block_num):
        is_block_number_exists = await self.main_db.is_block_number_exists(block_hash, block_num)
        parent_hash = await self.raw_db.get_parent_hash(block_hash)
        is_canonical_parent = await self.raw_db.is_canonical_block(parent_hash)
        is_forked = is_block_number_exists or (not is_canonical_parent)
        await self.processor.sync_block(block_hash, block_num, is_forked)

    async def process_chain_split(self, split_id):
        split_data = await self.raw_db.get_chain_split(split_id)
        await self.main_db.apply_chain_split(split_data)

    async def pending_tx_loop(self):
        logger.info("Entering Pending Tx Loop")

        while self._running is True:
            await self.get_and_process_pending_txs()

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_chain_event(self):
        last_event = await self.main_db.get_last_chain_event()
        if last_event is None:
            next_event_id = 1
        else:
            next_event_id = last_event['id'] + 1
        next_event = await self.raw_db.get_chain_event(next_event_id)
        if next_event is None:
            await asyncio.sleep(self.sleep_on_no_blocks)
            return
        await self.process_chain_event(next_event)

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


def sync_block(block_hash, block_number=None, is_forked=False, main_db_dsn=None, raw_db_dsn=None):
    processor = SyncProcessor(main_db_dsn=main_db_dsn, raw_db_dsn=raw_db_dsn)
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(processor.sync_block(block_hash, block_number))
