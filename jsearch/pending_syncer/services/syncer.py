import asyncio
import logging
from typing import Any, Dict, List

import backoff
import mode

from jsearch import settings
from jsearch.common import metrics
from jsearch.common.structs import SyncRange
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.database_queries.pending_transactions import prepare_pending_tx

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
        while not self.should_stop:
            await self.sync_pending_txs()

    @metrics.with_metrics('pending_transactions')
    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def sync_pending_txs(self) -> int:
        pending_txs = await self.get_pending_txs_to_sync()

        if len(pending_txs) == 0:
            logger.info("No pending txs, sleeping")
            await asyncio.sleep(settings.PENDING_TX_SLEEP_ON_NO_TXS)
            return 0

        for pending_tx in pending_txs:
            data = prepare_pending_tx(pending_tx)
            await self.main_db.insert_or_update_pending_tx(data)

        return len(pending_txs)

    async def get_pending_txs_to_sync(self) -> List[Dict[str, Any]]:
        last_synced_id = await self.main_db.get_pending_tx_last_synced_id()
        logger.info("Fetched last pending tx synced ID", extra={'number': last_synced_id})

        # TODO (Nick Gashkov): Move this to a generic function and reuse in
        # `jsearch.syncer.manager.Manager`. I didn't do it right now because
        # this causes slight refactoring of the `jsearch.syncer.manager.Manager`
        # which causes merge conflicts.
        if last_synced_id is None:
            start_id = self.sync_range.start
        else:
            start_id = last_synced_id + 1

        end_id = start_id + settings.PENDING_TX_BATCH_SIZE - 1
        if self.sync_range.end:
            end_id = min(end_id, self.sync_range.end)

        pending_txs = await self.raw_db.get_pending_txs(start_id, end_id)
        logger.info("Fetched pending txs", extra={'start_id': start_id, 'end_id': end_id})

        return pending_txs
