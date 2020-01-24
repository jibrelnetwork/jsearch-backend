import asyncio
import logging
import time
from asyncio import Future
from functools import partial
from typing import Dict, Any, Optional, List

import aiopg
import backoff
import psycopg2

from jsearch import settings
from jsearch.api.helpers import ChainEvent
from jsearch.common.prom_metrics import METRIC_SYNCER_EVENT_SYNC_DURATION
from jsearch.common.structs import BlockRange
from jsearch.common.utils import timeit, safe_get
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.state import SyncerState
from jsearch.syncer.structs import SwitchEvent, NodeState
from jsearch.syncer.syncer import apply_create_event, apply_split_event, sync_block

logger = logging.getLogger(__name__)

SLEEP_ON_NO_BLOCKS_DEFAULT = 1


async def reconnect(details: Dict[str, Any]) -> None:
    manager: Manager = details['args'][0]

    try:
        await manager.raw_db.disconnect()
    finally:
        await manager.raw_db.connect()

    try:
        await manager.main_db.disconnect()
    finally:
        await manager.main_db.connect()


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

    # FIXME (nickgashkov): `state` should be `None` by default.
    def __init__(  # type: ignore
            self,
            service,
            main_db,
            raw_db,
            sync_range: BlockRange,
            state: Optional[SyncerState] = False,
            resync: bool = False,
            resync_chain_splits: bool = False
    ):
        self.service = service
        self.main_db = main_db
        self.raw_db = raw_db
        self.sync_range = sync_range
        self._running = False
        self.sleep_on_no_blocks = SLEEP_ON_NO_BLOCKS_DEFAULT
        self.resync = resync
        self.resync_chain_splits = resync_chain_splits
        self.state = state or SyncerState(started_at=int(time.time()))

        self.latest_available_block_num = None
        self.latest_synced_block_num = None
        self.blockchain_tip = None
        self.tasks: List[Future] = []
        self.tip = None
        self.node_id = settings.ETH_NODE_ID

    async def start(self):
        logger.info("Starting Sync Manager", extra={'sync range': self.sync_range})
        self._running = True

        service_loops = []
        if self.resync is True:
            service_loops.append(self.resync_loop())
        else:
            service_loops.append(self.chain_events_process_loop())

        for coro in service_loops:
            coro = asyncio.shield(coro)

            task = asyncio.ensure_future(coro)
            task.add_done_callback(self.tasks.remove)

            self.tasks.append(task)

    async def wait(self):
        if not self.tasks:
            return
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

    async def stop(self, timeout=5):
        self._running = False

        if not self.tasks:
            # All tasks have been completed already and removed from the list.
            return

        done, pending = await asyncio.wait(self.tasks, timeout=timeout)

        for future in done:
            try:
                future.result()
            except asyncio.CancelledError:
                pass

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
            started_at = time.perf_counter()
            event = await self.get_and_process_chain_event()
            event_type = event and safe_get(event, 'type')
            if event_type is not None:
                ended_at = time.perf_counter()
                METRIC_SYNCER_EVENT_SYNC_DURATION.labels(event_type).observe(ended_at - started_at)

    async def resync_loop(self):
        logger.info("Entering ReSync Loop")
        if self.resync_chain_splits:
            await self.reapply_splits(self.sync_range)

        last_block = self.sync_range.end or await self.main_db.get_last_block_number(self.sync_range)
        for block_number in range(last_block, self.sync_range.start, -1):
            if not self._running:
                logger.info("Leave ReSync Loop")
                break
            await self.rewrite_block(block_number)

    @timeit('[SYNCER] Rewrite block')
    async def rewrite_block(self, block_number):
        logger.info("Rewrite block", extra={'block_number': block_number})
        block_hash = await self.main_db.get_block_hash_by_number(block_number)
        if block_hash is None:
            logger.info('Block is missed, use the syncer to sync it', extra={'number': block_number})
        else:
            await sync_block(
                raw_db=self.raw_db,
                main_db=self.main_db,
                block_hash=block_hash,
                block_number=block_number,
                is_forked=False,
                chain_event=None,
                rewrite=True
            )

    @timeit('[SYNCER] Reapply splits')
    async def reapply_splits(self, block_range: BlockRange) -> None:
        logger.info("Reapply chain splits on", extra={'block_range': block_range})
        chain_splits = await self.raw_db.get_chain_splits_for_range(block_range, self.node_id)
        async for chain_split in chain_splits:
            logger.info("Reapply chain split", extra={
                'block_number': chain_split['block_number'],
                'block_hash': chain_split['block_hash']
            })

            await apply_split_event(
                main_db=self.main_db,
                split_data=dict(chain_split),
                load_missed=partial(sync_block, self.raw_db, self.main_db)
            )

            if not self._running:
                logger.info("Stop reapplying chain splits")
                break

    async def process_chain_event(self, event):
        start_time = time.monotonic()
        logger.info("Start Processing Chain Event", extra={
            'event_id': event['id'],
            'event_type': event['type'],
            'block_number': event['block_number'],
            'block_hash': event['block_hash'],
        })

        if event['type'] == ChainEvent.INSERT:
            block_hash = event['block_hash']
            block_number = event['block_number']
            await apply_create_event(
                raw_db=self.raw_db,
                main_db=self.main_db,
                block_hash=block_hash,
                block_num=block_number,
                chain_event=event,
            )
            self.state.update(block_number)
        elif event['type'] == ChainEvent.REINSERT:
            await self.main_db.insert_chain_event(event)
        elif event['type'] == ChainEvent.SPLIT:
            await apply_split_event(
                self.main_db,
                split_data=event,
                load_missed=partial(sync_block, self.raw_db, self.main_db)
            )
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

    @backoff.on_exception(
        backoff.expo,
        jitter=None,
        max_tries=settings.SYNCER_BACKOFF_MAX_TRIES,
        exception=(psycopg2.OperationalError, psycopg2.InterfaceError, aiopg.sa.exc.InvalidRequestError),
        on_backoff=reconnect
    )
    @timeit('[SYNCER] Get and process chain event')
    async def get_and_process_chain_event(self):
        if self.state.already_processed is None:
            self.state.already_processed = self.sync_range.start

        block_range = await get_next_range(self.main_db, self.sync_range, self.state)
        last_event = await self.main_db.get_last_chain_event(block_range, self.node_id)
        next_event = await get_next_event(last_event, self.raw_db, self.node_id, block_range)

        logger.info("Event range", extra={"range": block_range})

        if next_event:
            self.state.already_processed = max(self.state.already_processed, block_range.start)
            self.state.last_synced_at = time.monotonic()
            await self.process_chain_event(next_event)
            return next_event

        if self.state.last_synced_at is None:
            self.state.last_synced_at = time.monotonic()

        next_block = await self.main_db.get_last_block_number(block_range) + 1
        logger.info('No blocks, handle gaps', extra={'next_block': next_block})

        if self.sync_range.end != block_range.end:
            logger.info("No more events in the range", extra={
                "range": block_range,
                'last': last_event and last_event['id']
            })
            self.state.hole = None
            self.state.already_processed = block_range.end + 1
            self.state.checked_on_holes = block_range
        else:
            is_need_to_wait = await self._try_to_switch(block_range, next_block)

            if not is_need_to_wait and self.sync_range.end and self.sync_range.end == block_range.end:
                logger.info('Sync range complete', extra={'range': self.sync_range})
                self._running = False
            else:
                logger.info('No blocks, sleeping')
                await asyncio.sleep(self.sleep_on_no_blocks)

    async def _try_to_switch(self, block_range: BlockRange, start_from: int) -> bool:
        before_switch = 0.0
        if self.state.last_synced_at:
            before_switch = self.state.last_synced_at + settings.ETH_NODE_SWITCH_TIMEOUT - time.monotonic()

        available_nodes = await self.raw_db.get_nodes_for_block(start_from, exclude_node=self.node_id)
        logger.info(
            'No blocks, try to find alternative',
            extra={
                'next_block': start_from,
                'available nodes': available_nodes,
                'before_switch': round(before_switch, 2)
            }
        )
        is_it_time_to_switch_node = block_range.end is not None or before_switch < 0

        if not available_nodes:
            return False

        if is_it_time_to_switch_node:
            logger.info('Search candidate to switch data source')
            switch = await search_candidate_to_switch_data_source(start_from, self.node_id, self.raw_db)
            if switch:
                logger.info(
                    'No blocks, switch node_id',
                    extra={
                        'old': self.node_id,
                        'new': switch.id,
                        'common_block': switch.block_number
                    }
                )
                self.node_id = switch.id
                self.state.already_processed = switch.block_number

                return True
            else:
                raise ValueError('No common block to switch')

        logger.info(
            'There are no blocks from current node, but there are from others',
            extra={
                'range': self.sync_range,
                'node_id': self.node_id,
                'available_nodes': available_nodes
            }
        )
        return True


async def search_candidate_to_switch_data_source(
        next_block: int,
        node_id: str,
        raw_db: RawDB,
        offset: int = 1024,
) -> Optional[SwitchEvent]:
    node_states: List[NodeState] = await raw_db.get_nodes(BlockRange(next_block, next_block + offset))
    nodes_map = {node.id: node for node in node_states}

    switch_candidate: Optional[NodeState]
    try:
        switch_candidate = sorted(node_states, key=lambda x: x.events)[-1]
    except IndexError:
        switch_candidate = None

    current_node = nodes_map.get(node_id)
    current_node_events = current_node and current_node.events or 0
    if switch_candidate and switch_candidate.events > current_node_events:
        logging.info(
            "Difference between nodes",
            extra={
                "candidate": switch_candidate.id,
                "current": node_id,
                "difference": switch_candidate.events - current_node_events
            }
        )
        common_block = await raw_db.get_common_block_number(
            node_left=node_id,
            node_right=switch_candidate.id,
            block_range=BlockRange(next_block - offset, next_block)
        )
        return SwitchEvent(switch_candidate.id, common_block)
    return None


async def get_next_range(
        main_db: MainDB,
        sync_range: BlockRange,
        state: SyncerState,
) -> BlockRange:
    """
    Get range until
    a                     b
    |                     |
    "----    -----        "
         |  |     |
         h1 h2    c

    Where:
     "-" - already synced blocks
     " " - empty block records

    This function returns the range nearliest to first hole on sync range.

    For example:
        - h1
        - ... until h2
        - c
        - ... until b

    Early we get latest block on this range we can sync only from c to b.
    In such case we will have the hole from h1 to h2.

    For example:
        - c
        - ... until b

    Such approach allows us to avoid holes appearing.

    Output data:
      - range
      - state:
          - hole
          - checked_on_holes
    """
    sync_range = BlockRange(max(state.already_processed or 0, sync_range.start), sync_range.end)
    if sync_range.end is None:
        return sync_range

    if state.hole and sync_range.start in state.hole:
        sync_range = state.hole

    elif state.checked_on_holes and sync_range.start in state.checked_on_holes:
        sync_range = state.checked_on_holes

    else:
        left, right = sync_range

        gap = await main_db.check_on_holes(left, right)
        if gap:
            state.hole = gap
            sync_range = gap
        else:
            state.hole = None
            state.checked_on_holes = BlockRange(left, right)
            sync_range = state.checked_on_holes

    return sync_range


async def get_next_event(
        last_event: Optional[Dict[str, Any]],
        raw_db: RawDB,
        node_id: str,
        event_range: BlockRange,
) -> Optional[Dict[str, Any]]:
    last_event_id = last_event and last_event['id']
    if last_event_id is None:
        next_event = await raw_db.get_first_chain_event_for_block_range(event_range, node_id)
    else:
        next_event = await raw_db.get_next_chain_event(event_range, last_event_id, node_id)

    return next_event
