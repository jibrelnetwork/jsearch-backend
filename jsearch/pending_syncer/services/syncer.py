import functools

import asyncio
import logging

import backoff
import mode
import time
import psycopg2
from typing import Any, Dict, List, Optional, Tuple

from jsearch import settings
from jsearch.common.prom_metrics import METRIC_SYNCER_PENDING_TXS_BATCH_SYNC_SPEED, METRIC_SYNCER_PENDING_LAG_RAW_DB
from jsearch.common.structs import BlockRange
from jsearch.common.utils import timeit
from jsearch.common.worker import shutdown_root_worker
from jsearch.pending_syncer.services import ApiService
from jsearch.pending_syncer.utils.processing import prepare_pending_txs
from jsearch.syncer.database import MainDB, RawDB

logger = logging.getLogger(__name__)


class PendingSyncerService(mode.Service):
    def __init__(self, raw_db_dsn: str, main_db_dsn: str, sync_range: BlockRange, **kwargs: Any) -> None:
        self.raw_db = RawDB(raw_db_dsn)
        self.main_db = MainDB(main_db_dsn)
        self.sync_range = sync_range

        self.api = ApiService()

        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()

    def on_init_dependencies(self) -> List[mode.Service]:
        return [self.api]

    async def on_stop(self) -> None:
        await self.raw_db.disconnect()
        await self.main_db.disconnect()

    async def on_started(self) -> None:
        fut = asyncio.create_task(self.syncer())
        fut.add_done_callback(functools.partial(shutdown_root_worker, service=self))

    async def syncer(self) -> None:
        pending_txs: List[Dict[str, Any]] = []
        last_sync_id = None

        while not self.should_stop:
            last_sync_id, pending_txs = await self.get_and_process_pending_txs(last_sync_id, pending_txs)

            if not pending_txs:
                logger.info("No pending txs, sleeping")
                await asyncio.sleep(settings.PENDING_TX_SLEEP_ON_NO_TXS)

    @timeit('[SYNCER] Get and process pending TXs')
    async def get_and_process_pending_txs(
            self, last_sync_id: int,
            pending_txs: List[Dict[str, Any]],
    ) -> Tuple[int, List[Dict[str, Any]]]:
        started_at = time.perf_counter()

        load_task = self.get_pending_txs_to_sync(last_sync_id)
        sync_task = self.sync_pending_txs(pending_txs)

        pending_txs, _ = await asyncio.gather(load_task, sync_task)

        if pending_txs:
            last_sync_id = max((tx.get('id') for tx in pending_txs))

            ended_at = time.perf_counter()
            speed = len(pending_txs) / (ended_at - started_at)

            METRIC_SYNCER_PENDING_TXS_BATCH_SYNC_SPEED.observe(speed)

        return last_sync_id, pending_txs

    @backoff.on_exception(
        backoff.expo,
        max_tries=settings.PENDING_SYNCER_BACKOFF_MAX_TRIES,
        exception=psycopg2.OperationalError
    )
    @timeit('[CPU/MAIN DB] Sync pending TXs')
    async def sync_pending_txs(self, pending_txs: List[Dict[str, Any]]) -> None:
        """
        We load history of pending transactions
        and want to save it in another storage.
        Each transaction can appears several times in
        loaded chunk of history.

        We must strictly follow for history order.
        """
        if not pending_txs:
            return

        prepared_txs = prepare_pending_txs(pending_txs)
        prepared_txs_as_dicts = [tx.to_dict() for tx in prepared_txs]

        await self.main_db.insert_or_update_pending_txs(prepared_txs_as_dicts)

    @backoff.on_exception(
        backoff.expo,
        max_tries=settings.PENDING_SYNCER_BACKOFF_MAX_TRIES,
        exception=psycopg2.OperationalError
    )
    @timeit('[RAW DB] Get pending TXs to sync')
    async def get_pending_txs_to_sync(self, last_synced_id: Optional[int]) -> List[Dict[str, Any]]:
        last_synced_id = last_synced_id or await self.main_db.get_pending_tx_last_synced_id()
        logger.info("Fetched last pending tx synced ID", extra={'number': last_synced_id})

        # TODO (Nick Gashkov): Move this to a generic function and reuse in
        # `jsearch.syncer.manager.Manager`. I didn't do it right now because
        # this causes slight refactoring of the `jsearch.syncer.manager.Manager`
        # which causes merge conflicts.
        if last_synced_id is None:
            # WTF: There're cases, when no TXs has been synced yet, and first
            # TX's number is way ahead (e.g. table has been truncated, but
            # 'id_seq' have't changed).
            raw_db_start_id = await self.raw_db.get_first_pending_tx_id()
            start_id = max(self.sync_range.start, raw_db_start_id)
        else:
            start_id = max(self.sync_range.start, last_synced_id + 1)

        end_id = start_id + settings.PENDING_TX_BATCH_SIZE - 1
        if self.sync_range.end:
            end_id = min(end_id, self.sync_range.end)

        pending_txs = await self.raw_db.get_pending_txs(start_id, end_id)
        last_pending_id = await self.raw_db.get_last_pending_tx_id()

        need_to_sync = last_pending_id - end_id
        if need_to_sync < 0:
            need_to_sync = 0

        METRIC_SYNCER_PENDING_LAG_RAW_DB.set(need_to_sync)
        logger.info(
            "Fetched pending txs",
            extra={
                'start_id': start_id,
                'end_id': end_id,
                'pending_txs': need_to_sync,
            }
        )

        return pending_txs
