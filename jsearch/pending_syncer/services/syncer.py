import asyncio
import logging

import backoff
import mode
from typing import Any, Dict, List, Optional

from jsearch.syncer.database_queries.pending_transactions import prepare_pending_tx

from jsearch import settings
from jsearch.common import metrics, async_utils
from jsearch.common.structs import SyncRange
from jsearch.syncer.database import MainDB, RawDB

logger = logging.getLogger(__name__)


class PendingSyncerService(mode.Service):
    def __init__(self, raw_db_dsn: str, main_db_dsn: str, sync_range: SyncRange, *args: Any, **kwargs: Any) -> None:
        self.raw_db = RawDB(raw_db_dsn)
        self.main_db = MainDB(main_db_dsn)
        self.sync_range = sync_range

        super().__init__(*args, **kwargs)

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()

    async def on_stop(self) -> None:
        self.raw_db.disconnect()
        await self.main_db.disconnect()

    @mode.Service.task
    async def syncer(self) -> None:
        pending_txs = []
        last_sync_id = None
        while not self.should_stop:
            load_task = self.get_pending_txs_to_sync(last_sync_id)
            sync_task = self.sync_pending_txs(pending_txs)

            # pass
            pending_txs, _ = await asyncio.gather(load_task, sync_task)

            if pending_txs:
                last_sync_id = max((tx.get('id') for tx in pending_txs))
            else:
                logger.info("No pending txs, sleeping")
                await asyncio.sleep(settings.PENDING_TX_SLEEP_ON_NO_TXS)

    @metrics.with_metrics('pending_transactions')
    @backoff.on_exception(backoff.expo, max_tries=5, exception=Exception)
    async def sync_pending_txs(self, pending_txs) -> int:
        """
        We load history of pending transactions
        and want to save it in another storage.
        Each transaction can appears several times in
        loaded chunk of history.

        We must strictly follow for history order.
        """

        tasks = async_utils.chain_dependent_coros(
            items=[prepare_pending_tx(tx) for tx in pending_txs],
            item_id_key='hash',
            create_task=self.main_db.insert_or_update_pending_tx,
        )

        await asyncio.gather(*tasks)

        return len(pending_txs)

    @backoff.on_exception(backoff.expo, max_tries=5, exception=Exception)
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
            start_id = last_synced_id + 1

        end_id = start_id + settings.PENDING_TX_BATCH_SIZE - 1
        if self.sync_range.end:
            end_id = min(end_id, self.sync_range.end)

        pending_txs = await self.raw_db.get_pending_txs(start_id, end_id)
        last_pending_id = await self.raw_db.get_last_pending_tx_id()

        need_to_sync = last_pending_id - end_id
        if need_to_sync < 0:
            need_to_sync = 0

        logger.info(
            "Fetched pending txs",
            extra={
                'start_id': start_id,
                'end_id': end_id,
                'pending_txs': need_to_sync,
            }
        )

        return pending_txs
