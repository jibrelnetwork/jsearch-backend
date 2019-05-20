import asyncio
import logging

import mode

from jsearch.common import metrics
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.database_queries.pending_transactions import prepare_pending_tx

logger = logging.getLogger(__name__)

PENDING_TX_BATCH_SIZE = 20
PENDING_TX_SLEEP_ON_NO_TXS = 1


class PendingSyncerService(mode.Service):
    def __init__(self, raw_db_dsn, main_db_dsn, *args, **kwargs):
        self.raw_db = RawDB(raw_db_dsn)
        self.main_db = MainDB(main_db_dsn)

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
    async def sync_pending_txs(self) -> int:
        pending_txs = await self.get_pending_txs_to_sync()

        if len(pending_txs) == 0:
            logger.info("No pending txs, sleeping")
            await asyncio.sleep(PENDING_TX_SLEEP_ON_NO_TXS)
            return 0

        for pending_tx in pending_txs:
            data = prepare_pending_tx(pending_tx)
            await self.main_db.insert_or_update_pending_tx(data)

        return len(pending_txs)

    async def get_pending_txs_to_sync(self):
        last_synced_id = await self.main_db.get_pending_tx_last_synced_id()
        logger.info("Fetched last pending tx synced ID", extra={'number': last_synced_id})

        return await self.raw_db.get_pending_txs_from(last_synced_id, PENDING_TX_BATCH_SIZE)
