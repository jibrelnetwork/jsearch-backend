import asyncio
import concurrent.futures
import logging

import backoff
import time
from typing import Dict, Any

from jsearch import settings
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.processor import SyncProcessor

logger = logging.getLogger(__name__)

SLEEP_ON_NO_BLOCKS_DEFAULT = 1
REORGS_BATCH_SIZE = settings.JSEARCH_SYNC_PARALLEL / 2
PENDING_TX_BATCH_SIZE = settings.JSEARCH_SYNC_PARALLEL * 2

SYNCER_BALANCE_MODE_LATEST = 'latest'
SYNCER_BALANCE_MODE_OFFSET = 'offset'


class ChainEvent:
    INSERT = 'created'
    REINSERT = 'reinserted'
    SPLIT = 'split'


async def process_insert_block(raw_db: RawDB,
                               main_db: MainDB,
                               block_hash: str,
                               block_num: int,
                               chain_event: Dict[str, Any],
                               last_block: int,
                               use_offset: bool = False) -> None:
    parent_hash = await raw_db.get_parent_hash(block_hash)
    is_block_number_exists = await main_db.is_block_number_exists(block_num)

    is_canonical_parent = await raw_db.is_canonical_block(parent_hash)
    is_forked = is_block_number_exists or (not is_canonical_parent)

    await SyncProcessor(raw_db=raw_db, main_db=main_db).sync_block(
        block_hash=block_hash,
        block_number=block_num,
        is_forked=is_forked,
        chain_event=chain_event,
        last_block=last_block,
        use_offset=use_offset
    )


async def process_chain_split(main_db: MainDB,
                              split_data: Dict[str, Any],
                              last_block: int,
                              use_offset: bool = False) -> None:
    from_block = split_data['block_number']
    to_block = split_data['block_number'] + split_data['add_length']

    hash_map = await main_db.get_hash_map_from_block_range(from_block, to_block)

    # get chains
    new_head = hash_map[split_data['add_block_hash']]
    new_chain_fragment = [new_head]

    while len(new_chain_fragment) < split_data['add_length']:
        next_block = hash_map[new_chain_fragment[-1]['parent_hash']]
        new_chain_fragment.append(next_block)

    old_head = hash_map[split_data['drop_block_hash']]
    old_chain_fragment = [old_head]
    while len(old_chain_fragment) < split_data['drop_length']:
        next_block = hash_map[old_chain_fragment[-1]['parent_hash']]
        old_chain_fragment.append(next_block)

    await main_db.apply_chain_split(
        new_chain_fragment=new_chain_fragment,
        old_chain_fragment=old_chain_fragment,
        last_block=last_block,
        chain_event=split_data
    )


class Manager:
    """
    Sync manager

    TODO: move common async daemon logic (start, stop, wait and etc. ) to generic class
    Notes:
        RawDB filling order:
            - internal_transactions
            - rewards
            - bodies
            - headers
            - accounts
            - receipts
            - reorgs
            - chain_splits
    """

    def __init__(self, service, main_db, raw_db, sync_range, balance_mode: str = SYNCER_BALANCE_MODE_LATEST):
        self.service = service
        self.main_db = main_db
        self.raw_db = raw_db
        self.sync_range = sync_range
        self._running = False
        self.chunk_size = settings.JSEARCH_SYNC_PARALLEL
        self.sleep_on_no_blocks = SLEEP_ON_NO_BLOCKS_DEFAULT
        self.balance_mode = balance_mode

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=settings.JSEARCH_SYNC_PARALLEL)

        self.latest_available_block_num = None
        self.latest_synced_block_num = None
        self.blockchain_tip = None
        self.tasks = []
        self.tip = None
        self.node_id = settings.ETH_NODE_ID

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

        last_block = await self.raw_db.get_latest_available_block_number()
        use_offset = self.balance_mode == SYNCER_BALANCE_MODE_OFFSET

        if event['type'] == ChainEvent.INSERT:
            block_hash = event['block_hash']
            block_number = event['block_number']
            await process_insert_block(
                raw_db=self.raw_db,
                main_db=self.main_db,
                block_hash=block_hash,
                block_num=block_number,
                chain_event=event,
                last_block=last_block,
                use_offset=use_offset
            )
        elif event['type'] == ChainEvent.REINSERT:
            await self.main_db.insert_chain_event(event)
        elif event['type'] == ChainEvent.SPLIT:
            await process_chain_split(self.main_db, split_data=event, last_block=last_block, use_offset=use_offset)
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
            'last_block': last_block,
            'offset': use_offset and settings.ETH_BALANCE_BLOCK_OFFSET,
            'time': '{:0.2f}s'.format(time.monotonic() - start_time),
        })

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_and_process_chain_event(self):
        last_event = await self.main_db.get_last_chain_event(self.sync_range, self.node_id)
        if last_event is None:
            next_event = await self.raw_db.get_first_chain_event_for_block_range(self.sync_range, self.node_id)
        else:
            next_event = await self.raw_db.get_next_chain_event(self.sync_range, last_event['id'], self.node_id)

        if self.sync_range[1] and next_event is None:
            logger.info(
                'Sync range complete',
                extra={
                    'from': self.sync_range[0],
                    'to': self.sync_range[1]
                }
            )
            await asyncio.sleep(10)
            self._running = False
            return

        if next_event is None:
            await asyncio.sleep(self.sleep_on_no_blocks)
            return

        await self.process_chain_event(next_event)
