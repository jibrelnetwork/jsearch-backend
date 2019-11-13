import asyncio
import logging

import mode
from typing import Any

from jsearch import settings
from jsearch.common.async_utils import aiosuppress
from jsearch.common.reference_data import set_lag_statistics
from jsearch.common.structs import BlockRange
from jsearch.syncer.database import MainDB, RawDB
from jsearch.syncer.manager import Manager
from jsearch.syncer.state import SyncerState

logger = logging.getLogger(__name__)


class SyncerService(mode.Service):
    def __init__(self,
                 state: SyncerState,
                 sync_range: BlockRange,
                 resync: bool = False,
                 resync_chain_splits: bool = False,
                 check_lag: bool = False,
                 *args: Any,
                 **kwargs: Any) -> None:
        self.raw_db = RawDB(settings.JSEARCH_RAW_DB)
        self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.manager = Manager(
            service=self,
            main_db=self.main_db,
            raw_db=self.raw_db,
            sync_range=sync_range,
            resync_chain_splits=resync_chain_splits,
            resync=resync,
            state=state
        )
        self.check_lag = check_lag

        # FIXME (nickgashkov): `mode.Service` does not support `*args`
        super(SyncerService, self).__init__(*args, **kwargs)  # type: ignore

    async def on_start(self) -> None:
        await self.raw_db.connect()
        await self.main_db.connect()

    async def on_stop(self) -> None:
        await self.manager.stop()

        await self.main_db.disconnect()
        await self.raw_db.disconnect()

    async def stop(self):
        await self.on_stop()

    @mode.Service.task
    async def syncer(self):
        await self.manager.start()
        exception = await self.manager.wait()
        if exception:
            await self.crash(exception)

        # we schedule shutdown on root Worker
        self.beacon.root.data.schedule_shutdown()

    @mode.Service.task
    async def update_lag_statistics(self):
        if not self.check_lag:
            return

        while not self.should_stop:
            await self.update_lag_statistics_once()
            await asyncio.sleep(settings.UPDATE_LAG_STATISTICS_DELAY_SECONDS)

    async def update_lag_statistics_once(self):
        latest_synced_block_number = await self.main_db.get_latest_synced_block_number()

        with aiosuppress(Exception):
            await set_lag_statistics(latest_synced_block_number)
