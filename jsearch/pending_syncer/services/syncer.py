import asyncio
import logging

import backoff
import mode
from typing import Any, Dict, List, Generator, Callable, Optional

from jsearch import settings
from jsearch.common import metrics
from jsearch.common.structs import SyncRange
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.database_queries.pending_transactions import prepare_pending_tx
from jsearch.typing import PendingTransactions, AnyCoroutine

logger = logging.getLogger(__name__)


async def make_chain(task_before: AnyCoroutine, task: AnyCoroutine) -> AnyCoroutine:
    await task_before
    return await task


def get_task_generator_from_txs(
        txs: PendingTransactions,
        create_task: Callable[[Dict[str, Any], ], AnyCoroutine]
) -> Generator[AnyCoroutine, None, None]:
    """
    We load history of pending transactions
    and want to save it in another storage.
    Each transaction can appears several times in
    loaded chunk of history.

    We must strictly follow for history order.

    Args:
        txs: list of pending transactions
        create_task: create coroutine per transaction
    """
    last_tasks = {}

    for tx in txs:
        tx_hash = tx.get('hash')
        tx_data = prepare_pending_tx(tx)
        task = create_task(tx_data)

        task_before = last_tasks.get(tx_hash)
        if task_before:
            task = make_chain(task_before, task)
            last_tasks[tx_hash] = task

        yield task


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
    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def sync_pending_txs(self, pending_txs) -> int:
        tasks = get_task_generator_from_txs(
            txs=pending_txs,
            create_task=self.main_db.insert_or_update_pending_tx
        )
        await asyncio.gather(*tasks)
        return len(pending_txs)

    @backoff.on_exception(backoff.fibo, max_tries=5, exception=Exception)
    async def get_pending_txs_to_sync(self, last_synced_id: Optional[int]) -> List[Dict[str, Any]]:
        last_synced_id = last_synced_id or await self.main_db.get_pending_tx_last_synced_id()
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
