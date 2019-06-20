import asyncio
import concurrent.futures
import logging

import backoff
import time

from jsearch import settings
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


    TODO: move common async daemon logic (start, stop, wait and etc. ) to generic class
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
        self.node_id = settings.ETH_NODE_ID

        self.processor = SyncProcessor()

    async def start(self):
        logger.info("Starting Sync Manager", extra={'sync range': self.sync_range})
        self._running = True

        service_loops = [
            self.chain_events_process_loop(),
        ]

        for coro in service_loops:
            coro = asyncio.shield(coro)

            task = asyncio.ensure_future(coro)
            task.add_done_callback(self.tasks.remove)

            self.tasks.append(task)

    async def wait(self):
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)

        exceptions = []
        for task in done:
            try:
                task.result()
            except Exception as e:
                exceptions.append(e)
                logger.exception(e)

        await self.stop()

        if exceptions:
            return exceptions[0]

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
            'event_type': event['type'],
            'block_number': event['block_number'],
            'block_hash': event['block_hash'],
        })
        if event['type'] == ChainEvent.INSERT:
            await self.process_insert_block(event['block_hash'], event['block_number'])
        elif event['type'] == ChainEvent.REINSERT:
            pass
        elif event['type'] == ChainEvent.SPLIT:
            await self.main_db.apply_chain_split(split_data=event)
        else:
            logger.error('Invalid chain event', extra={
                'event_id': event['id'],
                'event_type': event['type'],
            })
        logger.info("Finish Processing Chain Event", extra={
            'event_id': event['id'],
            'event_type': event['type'],
            'block_number': event['block_number'],
            'block_hash': event['block_hash'],
            'time': '{:0.2f}s'.format(time.monotonic() - start_time),
        })
        await self.main_db.insert_chain_event(event)

    async def process_insert_block(self, block_hash, block_num):
        is_block_number_exists = await self.main_db.is_block_number_exists(block_hash, block_num)
        parent_hash = await self.raw_db.get_parent_hash(block_hash)
        is_canonical_parent = await self.raw_db.is_canonical_block(parent_hash)
        is_forked = is_block_number_exists or (not is_canonical_parent)
        await self.processor.sync_block(block_hash, block_num, is_forked)

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_chain_event(self):
        last_event = await self.main_db.get_last_chain_event(self.sync_range, self.node_id)
        if last_event is None:
            next_event = await self.raw_db.get_first_chain_event_for_block_range(self.sync_range, self.node_id)
        else:
            next_event = await self.raw_db.get_next_chain_event(last_event['id'], self.node_id)

        if next_event is None:
            await asyncio.sleep(self.sleep_on_no_blocks)
            return

        if self.sync_range[1] and next_event['block_number'] > self.sync_range[1]:
            logger.info(
                'Sync range complete',
                extra={
                    'from': self.sync_range[0],
                    'to': self.sync_range[1]
                }
            )
            await asyncio.sleep(10)
            return
        await self.process_chain_event(next_event)
